"""Unit tests for per-partition workers and rebalance handling."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest

from cdc_platform.config.models import (
    PipelineConfig,
    PlatformConfig,
    SinkConfig,
    SinkType,
    SourceConfig,
    WebhookSinkConfig,
)
from cdc_platform.pipeline.runner import Pipeline
from cdc_platform.sources.base import SourceEvent


def _make_pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="test",
        source=SourceConfig(database="testdb", tables=["public.t"]),
        sinks=[
            SinkConfig(
                sink_id="wh1",
                sink_type=SinkType.WEBHOOK,
                webhook=WebhookSinkConfig(url="http://example.com"),
            )
        ],
    )


def _make_platform() -> PlatformConfig:
    return PlatformConfig(max_buffered_messages=100)


def _make_event(
    topic: str = "cdc.public.t", partition: int = 0, offset: int = 0
) -> SourceEvent:
    return SourceEvent(
        key=None,
        value=None,
        topic=topic,
        partition=partition,
        offset=offset,
    )


def _tracking_sink() -> tuple[AsyncMock, list[tuple[int, int]]]:
    """Returns a mock sink and a list that records (partition, offset) writes."""
    records: list[tuple[int, int]] = []

    async def track_write(_k, _v, _topic, partition, offset):
        records.append((partition, offset))

    sink = AsyncMock()
    sink.sink_id = "wh1"
    type(sink).flushed_offsets = PropertyMock(return_value={})
    sink.write.side_effect = track_write
    return sink, records


@pytest.mark.asyncio
class TestPartitionWorker:
    async def test_each_partition_gets_own_worker(self):
        """Enqueueing messages from different partitions creates separate workers."""
        pipeline = Pipeline(_make_pipeline(), _make_platform())
        sink, _ = _tracking_sink()
        pipeline._sinks = [sink]

        await pipeline._enqueue(_make_event(partition=0))
        await pipeline._enqueue(_make_event(partition=1))
        await pipeline._enqueue(_make_event(partition=2))

        assert len(pipeline._partition_workers) == 3
        assert ("cdc.public.t", 0) in pipeline._partition_workers
        assert ("cdc.public.t", 1) in pipeline._partition_workers
        assert ("cdc.public.t", 2) in pipeline._partition_workers

        for worker in pipeline._partition_workers.values():
            worker.cancel()

    async def test_partition_assignment_creates_queue_and_worker(self):
        """on_assign callback creates queues and workers for new partitions."""
        pipeline = Pipeline(_make_pipeline(), _make_platform())
        pipeline._sinks = [AsyncMock(sink_id="wh1")]

        pipeline._on_partitions_assigned([("t", 0), ("t", 1)])

        assert ("t", 0) in pipeline._partition_queues
        assert ("t", 1) in pipeline._partition_queues
        assert ("t", 0) in pipeline._partition_workers
        assert ("t", 1) in pipeline._partition_workers

        for worker in pipeline._partition_workers.values():
            worker.cancel()

    async def test_partition_revocation_cancels_worker(self):
        """on_revoke cancels workers and removes queues."""
        pipeline = Pipeline(_make_pipeline(), _make_platform())
        pipeline._sinks = [AsyncMock(sink_id="wh1")]

        pipeline._on_partitions_assigned([("t", 0), ("t", 1)])
        worker_0 = pipeline._partition_workers[("t", 0)]

        pipeline._on_partitions_revoked([("t", 0)])

        assert ("t", 0) not in pipeline._partition_queues
        assert ("t", 0) not in pipeline._partition_workers
        # Let the event loop process the cancellation
        await asyncio.sleep(0)
        assert worker_0.cancelled()
        # Partition 1 is still alive
        assert ("t", 1) in pipeline._partition_workers

        for worker in pipeline._partition_workers.values():
            worker.cancel()

    async def test_slow_partition_does_not_block_other(self):
        """Slow processing on partition 0 doesn't delay partition 1."""
        pipeline = Pipeline(_make_pipeline(), _make_platform())

        p0_events: list[int] = []
        p1_events: list[int] = []

        async def track_write(_k, _v, _topic, partition, offset):
            if partition == 0:
                await asyncio.sleep(0.1)  # slow
                p0_events.append(offset)
            else:
                p1_events.append(offset)

        sink = AsyncMock()
        sink.sink_id = "wh1"
        type(sink).flushed_offsets = PropertyMock(return_value={})
        sink.write.side_effect = track_write
        pipeline._sinks = [sink]

        # Enqueue: p0 gets 3 slow messages, p1 gets 3 fast messages
        for i in range(3):
            await pipeline._enqueue(_make_event(partition=0, offset=i))
            await pipeline._enqueue(_make_event(partition=1, offset=i))

        # Wait for p1 to finish (should be fast)
        await asyncio.sleep(0.05)
        # p1 should be done, p0 still processing
        assert len(p1_events) == 3
        assert len(p0_events) < 3  # p0 is still slow

        # Wait for everything to finish
        for q in pipeline._partition_queues.values():
            await asyncio.wait_for(q.join(), timeout=5.0)

        assert len(p0_events) == 3

        for worker in pipeline._partition_workers.values():
            worker.cancel()

    async def test_watermark_commit_still_partition_aware(self):
        """Watermark commits use per-partition min across sinks (unchanged behavior)."""
        pipeline = Pipeline(_make_pipeline(), _make_platform())

        mock_source = MagicMock()
        pipeline._source = mock_source

        sink1 = AsyncMock()
        sink1.sink_id = "s1"
        type(sink1).flushed_offsets = PropertyMock(
            return_value={("t", 0): 10, ("t", 1): 20}
        )
        sink2 = AsyncMock()
        sink2.sink_id = "s2"
        type(sink2).flushed_offsets = PropertyMock(
            return_value={("t", 0): 8, ("t", 1): 25}
        )
        pipeline._sinks = [sink1, sink2]

        pipeline._maybe_commit_watermark()

        mock_source.commit_offsets.assert_called_once_with({("t", 0): 8, ("t", 1): 20})
