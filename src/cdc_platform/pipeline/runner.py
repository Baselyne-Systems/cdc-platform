"""Pipeline orchestrator — source → consumer → sink(s) lifecycle."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any

import structlog

from cdc_platform.config.models import PipelineConfig, PlatformConfig
from cdc_platform.observability.http_health import HealthServer
from cdc_platform.sinks.base import SinkConnector
from cdc_platform.sinks.factory import create_sink
from cdc_platform.sources.base import EventSource, SourceEvent
from cdc_platform.sources.error_router import ErrorRouter
from cdc_platform.sources.factory import (
    create_error_router,
    create_event_source,
    create_provisioner,
    create_source_monitor,
)
from cdc_platform.sources.monitor import SourceMonitor

logger = structlog.get_logger()


class Pipeline:
    """Orchestrates the full CDC pipeline: source → transport → sink(s).

    Fan-out: each event is dispatched to all enabled sinks concurrently.
    Per-sink failures are routed to the error router independently.
    Per-partition bounded queues provide backpressure and parallel consumption.
    """

    def __init__(self, pipeline: PipelineConfig, platform: PlatformConfig) -> None:
        self._pipeline = pipeline
        self._platform = platform
        self._sinks: list[SinkConnector] = []
        self._source: EventSource | None = None
        self._error_router: ErrorRouter | None = None
        self._last_committed: dict[tuple[str, int], int] = {}
        self._partition_queues: dict[tuple[str, int], asyncio.Queue[SourceEvent]] = {}
        self._partition_workers: dict[tuple[str, int], asyncio.Task[None]] = {}
        self._monitor: SourceMonitor | None = None
        self._health_server: HealthServer | None = None

    def start(self) -> None:
        """Start the full pipeline (blocking)."""
        asyncio.run(self._start_async())

    async def _start_async(self) -> None:
        # 1. Provision transport resources (topics + connector)
        provisioner = create_provisioner(self._platform)
        await provisioner.provision(self._pipeline)

        # 2. Init error router (DLQ)
        self._error_router = create_error_router(self._platform)

        # 3. Start enabled sinks
        for sink_cfg in self._pipeline.sinks:
            if not sink_cfg.enabled:
                logger.info("pipeline.sink_disabled", sink_id=sink_cfg.sink_id)
                continue
            sink = create_sink(sink_cfg)
            await sink.start()
            self._sinks.append(sink)
            logger.info("pipeline.sink_started", sink_id=sink.sink_id)

        # 4. Build event source
        self._source = create_event_source(self._pipeline, self._platform)

        # 5. Start source monitor (schema + lag)
        self._monitor = create_source_monitor(
            self._pipeline, self._platform, on_incompatible=self.stop
        )
        if self._monitor is not None:
            await self._monitor.start()

        # 6. Start health server
        if self._platform.health_enabled:
            self._health_server = HealthServer(
                port=self._platform.health_port,
                readiness_check=self.health,
            )
            await self._health_server.start()

        logger.info(
            "pipeline.started",
            pipeline_id=self._pipeline.pipeline_id,
            sinks=[s.sink_id for s in self._sinks],
        )

        # 7. Consume — polls transport, enqueues to partition queues
        try:
            await self._source.start(
                handler=self._enqueue,
                on_assign=self._on_partitions_assigned,
                on_revoke=self._on_partitions_revoked,
            )
        finally:
            await self._shutdown()

    async def _enqueue(self, event: SourceEvent) -> None:
        """Enqueue event to the partition's bounded queue (backpressure)."""
        tp = (event.topic, event.partition)
        queue = self._partition_queues.get(tp)
        if queue is None:
            queue = asyncio.Queue(maxsize=self._platform.max_buffered_messages)
            self._partition_queues[tp] = queue
            self._partition_workers[tp] = asyncio.create_task(
                self._partition_loop(tp, queue)
            )
        await queue.put(event)

    async def _partition_loop(
        self, tp: tuple[str, int], queue: asyncio.Queue[SourceEvent]
    ) -> None:
        """Per-partition worker — drains queue and dispatches to sinks.

        Catches all exceptions to prevent silent worker death.  A single
        dispatch failure is logged and the worker continues with the next
        event.  The error router (DLQ) handles per-sink routing inside
        ``_dispatch_to_sinks``, so swallowing here is safe.
        """
        while True:
            event = await queue.get()
            try:
                await self._dispatch_to_sinks(event)
            except Exception:
                logger.exception(
                    "pipeline.partition_worker_error",
                    topic=event.topic,
                    partition=event.partition,
                    offset=event.offset,
                )
            finally:
                queue.task_done()

    def _on_partitions_assigned(self, partitions: list[tuple[str, int]]) -> None:
        for tp in partitions:
            if tp not in self._partition_queues:
                queue: asyncio.Queue[SourceEvent] = asyncio.Queue(
                    maxsize=self._platform.max_buffered_messages
                )
                self._partition_queues[tp] = queue
                self._partition_workers[tp] = asyncio.create_task(
                    self._partition_loop(tp, queue)
                )
        logger.info("pipeline.partitions_assigned", partitions=partitions)

    def _on_partitions_revoked(self, partitions: list[tuple[str, int]]) -> None:
        for tp in partitions:
            worker = self._partition_workers.pop(tp, None)
            if worker:
                worker.cancel()
            self._partition_queues.pop(tp, None)
        logger.info("pipeline.partitions_revoked", partitions=partitions)

    async def _dispatch_to_sinks(self, event: SourceEvent) -> None:
        """Fan-out to all sinks concurrently; per-sink failures → error router."""

        async def _write_to_sink(sink: SinkConnector) -> None:
            try:
                await sink.write(
                    event.key,
                    event.value,
                    event.topic,
                    event.partition,
                    event.offset,
                )
            except Exception as exc:
                logger.error(
                    "pipeline.sink_write_error",
                    sink_id=sink.sink_id,
                    topic=event.topic,
                    partition=event.partition,
                    offset=event.offset,
                    error=str(exc),
                )
                if self._error_router and event.raw is not None:
                    self._error_router.send(
                        source_topic=event.topic,
                        partition=event.partition,
                        offset=event.offset,
                        key=event.raw.key(),
                        value=event.raw.value(),
                        error=exc,
                        extra_headers={"dlq.sink_id": sink.sink_id},
                    )

        await asyncio.gather(*[_write_to_sink(s) for s in self._sinks])
        self._maybe_commit_watermark()

    async def _shutdown(self) -> None:
        """Stop health server, monitors, drain workers, flush sinks."""
        if self._health_server:
            await self._health_server.stop()
        if self._monitor:
            await self._monitor.stop()

        # Cancel all partition workers
        for worker in self._partition_workers.values():
            worker.cancel()
        for worker in self._partition_workers.values():
            with suppress(asyncio.CancelledError):
                await worker
        self._partition_workers.clear()
        self._partition_queues.clear()

        await self._stop_sinks()

    async def _stop_sinks(self) -> None:
        """Flush and stop all sinks."""
        for sink in self._sinks:
            try:
                await sink.flush()
                await sink.stop()
            except Exception as exc:
                logger.error(
                    "pipeline.sink_stop_error",
                    sink_id=sink.sink_id,
                    error=str(exc),
                )
        self._maybe_commit_watermark()

    def _maybe_commit_watermark(self) -> None:
        """Commit the min-watermark offset across all sinks."""
        if not self._sinks or self._source is None:
            return

        all_partitions: set[tuple[str, int]] = set()
        for sink in self._sinks:
            all_partitions.update(sink.flushed_offsets.keys())
        if not all_partitions:
            return

        committable: dict[tuple[str, int], int] = {}
        for tp in all_partitions:
            min_offset = min(sink.flushed_offsets.get(tp, -1) for sink in self._sinks)
            if min_offset >= 0:
                committable[tp] = min_offset
        if not committable:
            return

        to_commit = {
            tp: offset
            for tp, offset in committable.items()
            if offset > self._last_committed.get(tp, -1)
        }
        if not to_commit:
            return

        self._source.commit_offsets(to_commit)
        self._last_committed.update(to_commit)

    def stop(self) -> None:
        """Signal the pipeline to stop."""
        if self._source is not None:
            self._source.stop()

    async def health(self) -> dict[str, Any]:
        """Aggregate health from source + all sinks."""
        sink_health = []
        for sink in self._sinks:
            try:
                sink_health.append(await sink.health())
            except Exception as exc:
                sink_health.append(
                    {"sink_id": sink.sink_id, "status": "error", "error": str(exc)}
                )

        # Check source health
        source_status: dict[str, Any] = {}
        if self._source is not None:
            try:
                source_status = await self._source.health()
            except Exception as exc:
                source_status = {"status": "error", "error": str(exc)}

        lag_data = await self._monitor.get_lag() if self._monitor else []

        return {
            "pipeline_id": self._pipeline.pipeline_id,
            "source": source_status,
            "sinks": sink_health,
            "consumer_lag": lag_data,
        }
