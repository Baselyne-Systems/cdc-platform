"""Unit tests for the DLQ handler."""

from unittest.mock import MagicMock

from cdc_platform.config.models import DLQConfig
from cdc_platform.streaming.dlq import DLQHandler


class TestDLQHandler:
    def test_send_produces_with_diagnostic_headers(self):
        producer = MagicMock()
        handler = DLQHandler(producer, DLQConfig())

        handler.send(
            source_topic="cdc.public.customers",
            partition=0,
            offset=42,
            key=b"key",
            value=b"value",
            error=ValueError("bad data"),
        )

        producer.produce.assert_called_once()
        kwargs = producer.produce.call_args
        assert kwargs.kwargs["topic"] == "cdc.public.customers.dlq"
        assert kwargs.kwargs["key"] == b"key"
        assert kwargs.kwargs["value"] == b"value"

        headers = dict(kwargs.kwargs["headers"])
        assert headers["dlq.source.topic"] == b"cdc.public.customers"
        assert headers["dlq.source.partition"] == b"0"
        assert headers["dlq.source.offset"] == b"42"
        assert headers["dlq.error.type"] == b"ValueError"
        assert "dlq.timestamp" in headers

    def test_send_noop_when_disabled(self):
        producer = MagicMock()
        handler = DLQHandler(producer, DLQConfig(enabled=False))

        handler.send(
            source_topic="t",
            partition=0,
            offset=0,
            key=None,
            value=None,
            error=RuntimeError("x"),
        )

        producer.produce.assert_not_called()

    def test_headers_excluded_when_include_headers_false(self):
        producer = MagicMock()
        handler = DLQHandler(producer, DLQConfig(include_headers=False))

        handler.send(
            source_topic="t",
            partition=0,
            offset=0,
            key=None,
            value=None,
            error=RuntimeError("x"),
        )

        kwargs = producer.produce.call_args
        assert kwargs.kwargs["headers"] == []
