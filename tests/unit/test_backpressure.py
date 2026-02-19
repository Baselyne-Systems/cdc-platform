"""Unit tests for bounded queue backpressure in the pipeline runner."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, PropertyMock

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


def _make_platform(max_buffered: int = 5) -> PlatformConfig:
    return PlatformConfig(max_buffered_messages=max_buffered)


def _make_event(
    topic: str = "cdc.public.t", partition: int = 0, offset: int = 0
) -> SourceEvent:
    return SourceEvent(
        key={"k": offset},
        value={"v": offset},
        topic=topic,
        partition=partition,
        offset=offset,
    )


def _slow_sink(sink_id: str = "wh1", delay: float = 0.1) -> AsyncMock:
    sink = AsyncMock()
    sink.sink_id = sink_id
    type(sink).flushed_offsets = PropertyMock(return_value={})

    async def slow_write(*args, **kwargs):
        await asyncio.sleep(delay)

    sink.write.side_effect = slow_write
    return sink


@pytest.mark.asyncio
class TestBackpressure:
    async def test_queue_blocks_when_full(self):
        """When sink is slow, enqueue blocks once queue is full (backpressure).

        With maxsize=1, the worker pulls 1 item (blocked in slow write),
        so the queue can accept 1 more. The 3rd enqueue should block.
        """
        pipeline = Pipeline(_make_pipeline(), _make_platform(max_buffered=1))
        pipeline._sinks = [_slow_sink(delay=10)]  # very slow sink

        # First enqueue: worker pulls it immediately into slow write
        await pipeline._enqueue(_make_event(offset=0))
        await asyncio.sleep(0.01)  # let worker pick it up

        # Second enqueue: fills the queue (maxsize=1)
        await pipeline._enqueue(_make_event(offset=1))

        # Third enqueue should block (queue full, worker busy with sink)
        enqueue_task = asyncio.create_task(pipeline._enqueue(_make_event(offset=2)))
        await asyncio.sleep(0.05)
        assert not enqueue_task.done(), "enqueue should block when queue is full"

        # Clean up
        enqueue_task.cancel()
        for worker in pipeline._partition_workers.values():
            worker.cancel()

    async def test_messages_drain_when_sink_catches_up(self):
        """Messages are processed once the sink becomes available."""
        pipeline = Pipeline(_make_pipeline(), _make_platform(max_buffered=10))

        processed = []

        async def tracking_write(*args, **kwargs):
            processed.append(args)

        sink = AsyncMock()
        sink.sink_id = "wh1"
        type(sink).flushed_offsets = PropertyMock(return_value={})
        sink.write.side_effect = tracking_write
        pipeline._sinks = [sink]

        for i in range(5):
            await pipeline._enqueue(_make_event(offset=i))

        # Let workers drain
        for q in pipeline._partition_queues.values():
            await asyncio.wait_for(q.join(), timeout=2.0)

        assert len(processed) == 5

        for worker in pipeline._partition_workers.values():
            worker.cancel()

    async def test_queue_size_respects_max_buffered(self):
        """Queue maxsize matches config."""
        pipeline = Pipeline(_make_pipeline(), _make_platform(max_buffered=42))
        pipeline._sinks = [_slow_sink(delay=10)]

        await pipeline._enqueue(_make_event())

        tp = ("cdc.public.t", 0)
        assert pipeline._partition_queues[tp].maxsize == 42

        for worker in pipeline._partition_workers.values():
            worker.cancel()

    async def test_backpressure_does_not_drop_messages(self):
        """All enqueued messages are eventually dispatched."""
        pipeline = Pipeline(_make_pipeline(), _make_platform(max_buffered=3))

        offsets_written: list[int] = []

        async def track_write(_k, _v, _topic, _part, offset):
            offsets_written.append(offset)
            await asyncio.sleep(0.01)

        sink = AsyncMock()
        sink.sink_id = "wh1"
        type(sink).flushed_offsets = PropertyMock(return_value={})
        sink.write.side_effect = track_write
        pipeline._sinks = [sink]

        for i in range(10):
            await pipeline._enqueue(_make_event(offset=i))

        for q in pipeline._partition_queues.values():
            await asyncio.wait_for(q.join(), timeout=5.0)

        assert sorted(offsets_written) == list(range(10))

        for worker in pipeline._partition_workers.values():
            worker.cancel()
