"""Pipeline orchestrator — source → consumer → sink(s) lifecycle."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any

import structlog
from confluent_kafka import Message

from cdc_platform.config.models import PipelineConfig, PlatformConfig
from cdc_platform.observability.http_health import HealthServer
from cdc_platform.observability.metrics import LagMonitor
from cdc_platform.sinks.base import SinkConnector
from cdc_platform.sinks.factory import create_sink
from cdc_platform.sources.debezium.client import DebeziumClient
from cdc_platform.streaming.consumer import CDCConsumer
from cdc_platform.streaming.dlq import DLQHandler
from cdc_platform.streaming.producer import create_producer
from cdc_platform.streaming.schema_monitor import SchemaMonitor
from cdc_platform.streaming.topics import ensure_topics, topics_for_pipeline

logger = structlog.get_logger()


class Pipeline:
    """Orchestrates the full CDC pipeline: source → Kafka → sink(s).

    Fan-out: each event is dispatched to all enabled sinks concurrently.
    Per-sink failures are routed to the DLQ independently.
    Per-partition bounded queues provide backpressure and parallel consumption.
    """

    def __init__(self, pipeline: PipelineConfig, platform: PlatformConfig) -> None:
        self._pipeline = pipeline
        self._platform = platform
        self._sinks: list[SinkConnector] = []
        self._consumer: CDCConsumer | None = None
        self._dlq: DLQHandler | None = None
        self._last_committed: dict[tuple[str, int], int] = {}
        self._partition_queues: dict[
            tuple[str, int], asyncio.Queue[tuple[Any, Any, Message]]
        ] = {}
        self._partition_workers: dict[tuple[str, int], asyncio.Task[None]] = {}
        self._schema_monitor: SchemaMonitor | None = None
        self._lag_monitor: LagMonitor | None = None
        self._health_server: HealthServer | None = None

    def start(self) -> None:
        """Start the full pipeline (blocking)."""
        asyncio.run(self._start_async())

    async def _start_async(self) -> None:
        # 1. Ensure topics exist
        all_topics = topics_for_pipeline(self._pipeline, self._platform)
        ensure_topics(self._platform.kafka.bootstrap_servers, all_topics)

        # 2. Deploy Debezium connector
        async with DebeziumClient(self._platform.connector) as client:
            await client.wait_until_ready()
            await client.register_connector(self._pipeline, self._platform)
            logger.info(
                "pipeline.connector_deployed", pipeline_id=self._pipeline.pipeline_id
            )

        # 3. Init DLQ handler
        if self._platform.dlq.enabled:
            producer = create_producer(self._platform.kafka)
            self._dlq = DLQHandler(producer, self._platform.dlq)

        # 4. Start enabled sinks
        for sink_cfg in self._pipeline.sinks:
            if not sink_cfg.enabled:
                logger.info("pipeline.sink_disabled", sink_id=sink_cfg.sink_id)
                continue
            sink = create_sink(sink_cfg)
            await sink.start()
            self._sinks.append(sink)
            logger.info("pipeline.sink_started", sink_id=sink.sink_id)

        # 5. Build consumer with async enqueue handler + rebalance callbacks
        cdc_topics = [t for t in all_topics if not t.endswith(".dlq")]
        self._consumer = CDCConsumer(
            topics=cdc_topics,
            kafka_config=self._platform.kafka,
            handler=self._enqueue,
            dlq_config=self._platform.dlq,
            on_assign=self._on_partitions_assigned,
            on_revoke=self._on_partitions_revoked,
        )

        # 6. Start schema monitor
        self._schema_monitor = SchemaMonitor(
            registry_url=self._platform.kafka.schema_registry_url,
            topics=cdc_topics,
            interval=self._platform.schema_monitor_interval_seconds,
            stop_on_incompatible=self._platform.stop_on_incompatible_schema,
            on_incompatible=self.stop,
        )
        await self._schema_monitor.start()

        # 7. Start lag monitor
        self._lag_monitor = LagMonitor(
            bootstrap_servers=self._platform.kafka.bootstrap_servers,
            group_id=self._platform.kafka.group_id,
            topics=cdc_topics,
            interval=self._platform.lag_monitor_interval_seconds,
        )
        await self._lag_monitor.start()

        # 8. Start health server
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
            topics=cdc_topics,
        )

        # 9. Consume (async) — polls in a thread, enqueues to partition queues
        try:
            await self._consumer.consume()
        finally:
            await self._shutdown()

    async def _enqueue(
        self,
        key: dict[str, Any] | None,
        value: dict[str, Any] | None,
        msg: Message,
    ) -> None:
        """Enqueue message to the partition's bounded queue (backpressure)."""
        topic = msg.topic()
        partition = msg.partition()
        assert topic is not None
        assert partition is not None
        tp = (topic, partition)
        queue = self._partition_queues.get(tp)
        if queue is None:
            queue = asyncio.Queue(maxsize=self._platform.max_buffered_messages)
            self._partition_queues[tp] = queue
            self._partition_workers[tp] = asyncio.create_task(
                self._partition_loop(tp, queue)
            )
        await queue.put((key, value, msg))

    async def _partition_loop(
        self, tp: tuple[str, int], queue: asyncio.Queue[tuple[Any, Any, Message]]
    ) -> None:
        """Per-partition worker — drains queue and dispatches to sinks."""
        while True:
            key, value, msg = await queue.get()
            try:
                await self._dispatch_to_sinks(key, value, msg)
            finally:
                queue.task_done()

    def _on_partitions_assigned(self, partitions: list[tuple[str, int]]) -> None:
        for tp in partitions:
            if tp not in self._partition_queues:
                queue: asyncio.Queue[tuple[Any, Any, Message]] = asyncio.Queue(
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

    async def _dispatch_to_sinks(
        self,
        key: dict[str, Any] | None,
        value: dict[str, Any] | None,
        msg: Message,
    ) -> None:
        """Fan-out to all sinks concurrently; per-sink failures → DLQ."""
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        assert topic is not None
        assert partition is not None
        assert offset is not None

        async def _write_to_sink(sink: SinkConnector) -> None:
            try:
                await sink.write(key, value, topic, partition, offset)
            except Exception as exc:
                logger.error(
                    "pipeline.sink_write_error",
                    sink_id=sink.sink_id,
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    error=str(exc),
                )
                if self._dlq:
                    self._dlq.send(
                        source_topic=topic,
                        partition=partition,
                        offset=offset,
                        key=msg.key(),
                        value=msg.value(),
                        error=exc,
                        extra_headers={"dlq.sink_id": sink.sink_id},
                    )

        await asyncio.gather(*[_write_to_sink(s) for s in self._sinks])
        self._maybe_commit_watermark()

    async def _shutdown(self) -> None:
        """Stop health server, monitors, drain workers, flush sinks."""
        if self._health_server:
            await self._health_server.stop()
        if self._schema_monitor:
            await self._schema_monitor.stop()
        if self._lag_monitor:
            await self._lag_monitor.stop()

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
        if not self._sinks or self._consumer is None:
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

        self._consumer.commit_offsets(to_commit)
        self._last_committed.update(to_commit)

    def stop(self) -> None:
        """Signal the pipeline to stop."""
        if self._consumer is not None:
            self._consumer.stop()

    async def health(self) -> dict[str, Any]:
        """Aggregate health from source connector + all sinks."""
        sink_health = []
        for sink in self._sinks:
            try:
                sink_health.append(await sink.health())
            except Exception as exc:
                sink_health.append(
                    {"sink_id": sink.sink_id, "status": "error", "error": str(exc)}
                )

        # Check source connector status
        source_status: dict[str, Any] = {}
        try:
            from cdc_platform.sources.debezium.config import connector_name

            name = connector_name(self._pipeline)
            async with DebeziumClient(self._platform.connector) as client:
                source_status = await client.get_connector_status(name)
        except Exception as exc:
            source_status = {"status": "error", "error": str(exc)}

        lag_data = self._lag_monitor.latest_lag if self._lag_monitor else []

        return {
            "pipeline_id": self._pipeline.pipeline_id,
            "source": source_status,
            "sinks": sink_health,
            "consumer_lag": [
                {
                    "topic": p.topic,
                    "partition": p.partition,
                    "lag": p.lag,
                    "offset": p.current_offset,
                }
                for p in lag_data
            ],
        }
