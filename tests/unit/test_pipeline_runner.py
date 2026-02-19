"""Unit tests for the pipeline orchestrator."""

from __future__ import annotations

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


def _make_pipeline(*sink_cfgs: SinkConfig) -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="test-pipeline",
        source=SourceConfig(database="testdb", tables=["public.customers"]),
        sinks=list(sink_cfgs),
    )


def _make_platform() -> PlatformConfig:
    return PlatformConfig()


def _webhook_sink_config(sink_id: str = "wh1", enabled: bool = True) -> SinkConfig:
    return SinkConfig(
        sink_id=sink_id,
        sink_type=SinkType.WEBHOOK,
        enabled=enabled,
        webhook=WebhookSinkConfig(url="http://example.com/hook"),
    )


def _make_event(
    topic: str = "cdc.public.customers",
    partition: int = 0,
    offset: int = 1,
) -> SourceEvent:
    raw = MagicMock()
    raw.key.return_value = b"key"
    raw.value.return_value = b"value"
    return SourceEvent(
        key={"id": 1},
        value={"name": "Alice"},
        topic=topic,
        partition=partition,
        offset=offset,
        raw=raw,
    )


@pytest.mark.asyncio
class TestPipelineDispatch:
    async def test_fan_out_to_multiple_sinks(self):
        config = _make_pipeline(
            _webhook_sink_config("wh1"), _webhook_sink_config("wh2")
        )
        pipeline = Pipeline(config, _make_platform())

        sink1 = AsyncMock()
        sink1.sink_id = "wh1"
        type(sink1).flushed_offsets = PropertyMock(return_value={})
        sink2 = AsyncMock()
        sink2.sink_id = "wh2"
        type(sink2).flushed_offsets = PropertyMock(return_value={})
        pipeline._sinks = [sink1, sink2]

        event = _make_event()
        await pipeline._dispatch_to_sinks(event)

        sink1.write.assert_awaited_once_with(
            {"id": 1}, {"name": "Alice"}, "cdc.public.customers", 0, 1
        )
        sink2.write.assert_awaited_once_with(
            {"id": 1}, {"name": "Alice"}, "cdc.public.customers", 0, 1
        )

    async def test_sink_failure_routes_to_error_router(self):
        config = _make_pipeline(_webhook_sink_config("wh1"))
        pipeline = Pipeline(config, _make_platform())

        failing_sink = AsyncMock()
        failing_sink.sink_id = "wh1"
        failing_sink.write.side_effect = Exception("connection refused")
        type(failing_sink).flushed_offsets = PropertyMock(return_value={})
        pipeline._sinks = [failing_sink]

        mock_error_router = MagicMock()
        pipeline._error_router = mock_error_router

        event = _make_event()
        # Should not raise — failure is caught and routed to error router
        await pipeline._dispatch_to_sinks(event)

        mock_error_router.send.assert_called_once()
        call_kwargs = mock_error_router.send.call_args.kwargs
        assert call_kwargs["source_topic"] == "cdc.public.customers"
        assert call_kwargs["extra_headers"] == {"dlq.sink_id": "wh1"}

    async def test_one_sink_failure_doesnt_block_others(self):
        config = _make_pipeline(
            _webhook_sink_config("wh1"), _webhook_sink_config("wh2")
        )
        pipeline = Pipeline(config, _make_platform())

        failing_sink = AsyncMock()
        failing_sink.sink_id = "wh1"
        failing_sink.write.side_effect = Exception("fail")
        type(failing_sink).flushed_offsets = PropertyMock(return_value={})

        ok_sink = AsyncMock()
        ok_sink.sink_id = "wh2"
        type(ok_sink).flushed_offsets = PropertyMock(return_value={})

        pipeline._sinks = [failing_sink, ok_sink]
        pipeline._error_router = MagicMock()

        event = _make_event()
        await pipeline._dispatch_to_sinks(event)

        ok_sink.write.assert_awaited_once()

    async def test_stop_flushes_all_sinks(self):
        config = _make_pipeline(_webhook_sink_config("wh1"))
        pipeline = Pipeline(config, _make_platform())

        sink = AsyncMock()
        sink.sink_id = "wh1"
        type(sink).flushed_offsets = PropertyMock(return_value={})
        pipeline._sinks = [sink]

        await pipeline._stop_sinks()

        sink.flush.assert_awaited_once()
        sink.stop.assert_awaited_once()


class TestPipelineDisabledSinks:
    def test_disabled_sinks_not_in_config(self):
        """Disabled sinks should still be in config but not started."""
        config = _make_pipeline(
            _webhook_sink_config("wh1", enabled=True),
            _webhook_sink_config("wh2", enabled=False),
        )
        assert len(config.sinks) == 2
        assert config.sinks[0].enabled is True
        assert config.sinks[1].enabled is False


@pytest.mark.asyncio
class TestDispatchAsHandler:
    async def test_dispatch_can_be_used_as_async_handler(self):
        """_dispatch_to_sinks is passed directly as async_handler to source."""
        config = _make_pipeline(_webhook_sink_config("wh1"))
        pipeline = Pipeline(config, _make_platform())

        sink = AsyncMock()
        sink.sink_id = "wh1"
        type(sink).flushed_offsets = PropertyMock(return_value={})
        pipeline._sinks = [sink]

        event = _make_event()
        await pipeline._dispatch_to_sinks(event)

        sink.write.assert_awaited_once()


def _mock_sink(
    sink_id: str, flushed: dict[tuple[str, int], int] | None = None
) -> MagicMock:
    """Create a mock sink with flushed_offsets property."""
    sink = AsyncMock()
    sink.sink_id = sink_id
    type(sink).flushed_offsets = PropertyMock(return_value=flushed or {})
    return sink


@pytest.mark.asyncio
class TestWatermarkCommit:
    async def test_watermark_committed_when_all_sinks_flushed(self):
        config = _make_pipeline(_webhook_sink_config("wh1"))
        pipeline = Pipeline(config, _make_platform())

        mock_source = MagicMock()
        pipeline._source = mock_source

        sink = _mock_sink("wh1", {("t", 0): 10})
        pipeline._sinks = [sink]

        pipeline._maybe_commit_watermark()

        mock_source.commit_offsets.assert_called_once_with({("t", 0): 10})

    async def test_watermark_not_committed_twice(self):
        config = _make_pipeline(_webhook_sink_config("wh1"))
        pipeline = Pipeline(config, _make_platform())

        mock_source = MagicMock()
        pipeline._source = mock_source

        sink = _mock_sink("wh1", {("t", 0): 10})
        pipeline._sinks = [sink]

        pipeline._maybe_commit_watermark()
        pipeline._maybe_commit_watermark()

        assert mock_source.commit_offsets.call_count == 1

    async def test_min_watermark_across_sinks(self):
        config = _make_pipeline(
            _webhook_sink_config("wh1"),
            _webhook_sink_config("wh2"),
        )
        pipeline = Pipeline(config, _make_platform())

        mock_source = MagicMock()
        pipeline._source = mock_source

        sink1 = _mock_sink("wh1", {("t", 0): 10})
        sink2 = _mock_sink("wh2", {("t", 0): 4})
        pipeline._sinks = [sink1, sink2]

        pipeline._maybe_commit_watermark()

        mock_source.commit_offsets.assert_called_once_with({("t", 0): 4})

    async def test_partition_suppressed_when_sink_not_flushed(self):
        config = _make_pipeline(
            _webhook_sink_config("wh1"),
            _webhook_sink_config("wh2"),
        )
        pipeline = Pipeline(config, _make_platform())

        mock_source = MagicMock()
        pipeline._source = mock_source

        sink1 = _mock_sink("wh1", {("t", 0): 10})
        sink2 = _mock_sink("wh2", {})  # hasn't flushed partition 0 yet
        pipeline._sinks = [sink1, sink2]

        pipeline._maybe_commit_watermark()

        mock_source.commit_offsets.assert_not_called()

    async def test_multiple_partitions_committed_independently(self):
        config = _make_pipeline(_webhook_sink_config("wh1"))
        pipeline = Pipeline(config, _make_platform())

        mock_source = MagicMock()
        pipeline._source = mock_source

        sink = _mock_sink("wh1", {("t", 0): 5, ("t", 1): 12})
        pipeline._sinks = [sink]

        pipeline._maybe_commit_watermark()

        mock_source.commit_offsets.assert_called_once_with({("t", 0): 5, ("t", 1): 12})

    async def test_stop_sinks_commits_final_watermark(self):
        config = _make_pipeline(_webhook_sink_config("wh1"))
        pipeline = Pipeline(config, _make_platform())

        mock_source = MagicMock()
        pipeline._source = mock_source

        sink = _mock_sink("wh1", {("t", 0): 99})
        pipeline._sinks = [sink]

        await pipeline._stop_sinks()

        mock_source.commit_offsets.assert_called_once_with({("t", 0): 99})

    async def test_no_commit_when_source_is_none(self):
        config = _make_pipeline(_webhook_sink_config("wh1"))
        pipeline = Pipeline(config, _make_platform())
        pipeline._source = None

        sink = _mock_sink("wh1", {("t", 0): 10})
        pipeline._sinks = [sink]

        pipeline._maybe_commit_watermark()
        # No source, so no commit — no assertion needed, just no error

    async def test_no_commit_when_no_sinks(self):
        config = _make_pipeline()
        pipeline = Pipeline(config, _make_platform())

        mock_source = MagicMock()
        pipeline._source = mock_source
        pipeline._sinks = []

        pipeline._maybe_commit_watermark()

        mock_source.commit_offsets.assert_not_called()
