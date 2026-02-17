"""Unit tests for bounded queue backpressure in the pipeline runner."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest

from cdc_platform.config.models import (
    PipelineConfig,
    SinkConfig,
    SinkType,
    SourceConfig,
    WebhookSinkConfig,
)
from cdc_platform.pipeline.runner import Pipeline


def _make_pipeline(max_buffered: int = 5) -> PipelineConfig:
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
        max_buffered_messages=max_buffered,
    )


def _mock_message(topic: str = "cdc.public.t", partition: int = 0, offset: int = 0) -> MagicMock:
    msg = MagicMock()
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.key.return_value = b"k"
    msg.value.return_value = b"v"
    return msg


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
        config = _make_pipeline(max_buffered=1)
        pipeline = Pipeline(config)
        pipeline._sinks = [_slow_sink(delay=10)]  # very slow sink

        # First enqueue: worker pulls it immediately into slow write
        msg1 = _mock_message(offset=0)
        await pipeline._enqueue({"k": 1}, {"v": 1}, msg1)
        await asyncio.sleep(0.01)  # let worker pick it up

        # Second enqueue: fills the queue (maxsize=1)
        msg2 = _mock_message(offset=1)
        await pipeline._enqueue({"k": 2}, {"v": 2}, msg2)

        # Third enqueue should block (queue full, worker busy with sink)
        msg3 = _mock_message(offset=2)
        enqueue_task = asyncio.create_task(pipeline._enqueue({"k": 3}, {"v": 3}, msg3))
        await asyncio.sleep(0.05)
        assert not enqueue_task.done(), "enqueue should block when queue is full"

        # Clean up
        enqueue_task.cancel()
        for worker in pipeline._partition_workers.values():
            worker.cancel()

    async def test_messages_drain_when_sink_catches_up(self):
        """Messages are processed once the sink becomes available."""
        config = _make_pipeline(max_buffered=10)
        pipeline = Pipeline(config)

        processed = []

        async def tracking_write(*args, **kwargs):
            processed.append(args)

        sink = AsyncMock()
        sink.sink_id = "wh1"
        type(sink).flushed_offsets = PropertyMock(return_value={})
        sink.write.side_effect = tracking_write
        pipeline._sinks = [sink]

        for i in range(5):
            msg = _mock_message(offset=i)
            await pipeline._enqueue({"k": i}, {"v": i}, msg)

        # Let workers drain
        for q in pipeline._partition_queues.values():
            await asyncio.wait_for(q.join(), timeout=2.0)

        assert len(processed) == 5

        for worker in pipeline._partition_workers.values():
            worker.cancel()

    async def test_queue_size_respects_max_buffered(self):
        """Queue maxsize matches config."""
        config = _make_pipeline(max_buffered=42)
        pipeline = Pipeline(config)
        pipeline._sinks = [_slow_sink(delay=10)]

        msg = _mock_message()
        await pipeline._enqueue(None, None, msg)

        tp = ("cdc.public.t", 0)
        assert pipeline._partition_queues[tp].maxsize == 42

        for worker in pipeline._partition_workers.values():
            worker.cancel()

    async def test_backpressure_does_not_drop_messages(self):
        """All enqueued messages are eventually dispatched."""
        config = _make_pipeline(max_buffered=3)
        pipeline = Pipeline(config)

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
            msg = _mock_message(offset=i)
            await pipeline._enqueue({"k": i}, {"v": i}, msg)

        for q in pipeline._partition_queues.values():
            await asyncio.wait_for(q.join(), timeout=5.0)

        assert sorted(offsets_written) == list(range(10))

        for worker in pipeline._partition_workers.values():
            worker.cancel()
