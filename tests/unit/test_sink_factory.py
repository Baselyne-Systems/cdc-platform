"""Unit tests for the sink factory."""

import pytest

from cdc_platform.config.models import (
    PostgresSinkConfig,
    SinkConfig,
    SinkType,
    WebhookSinkConfig,
)
from cdc_platform.sinks.factory import create_sink
from cdc_platform.sinks.postgres import PostgresSink
from cdc_platform.sinks.webhook import WebhookSink


class TestCreateSink:
    def test_creates_webhook_sink(self):
        cfg = SinkConfig(
            sink_id="wh",
            sink_type=SinkType.WEBHOOK,
            webhook=WebhookSinkConfig(url="http://example.com"),
        )
        sink = create_sink(cfg)
        assert isinstance(sink, WebhookSink)
        assert sink.sink_id == "wh"

    def test_creates_postgres_sink(self):
        cfg = SinkConfig(
            sink_id="pg",
            sink_type=SinkType.POSTGRES,
            postgres=PostgresSinkConfig(database="db", target_table="public.events"),
        )
        sink = create_sink(cfg)
        assert isinstance(sink, PostgresSink)
        assert sink.sink_id == "pg"

    def test_unknown_type_raises(self):
        """Passing a type not in the registry should raise ValueError."""
        cfg = SinkConfig(
            sink_id="x",
            sink_type=SinkType.WEBHOOK,
            webhook=WebhookSinkConfig(url="http://example.com"),
        )
        # Monkey-patch to simulate unknown type
        cfg.sink_type = "unknown"  # type: ignore[assignment]
        with pytest.raises(ValueError, match="Unknown sink type"):
            create_sink(cfg)
