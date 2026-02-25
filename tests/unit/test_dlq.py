"""Unit tests for the DLQ handler."""

from unittest.mock import MagicMock, patch

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

    def test_produce_failure_does_not_raise(self):
        """DLQ write failure is caught and logged, not propagated."""
        producer = MagicMock()
        producer.produce.side_effect = RuntimeError("broker down")
        handler = DLQHandler(producer, DLQConfig())

        # Should not raise
        handler.send(
            source_topic="t",
            partition=0,
            offset=0,
            key=None,
            value=None,
            error=ValueError("original error"),
        )

    def test_flush_failure_does_not_raise(self):
        """DLQ flush failure is caught and logged, not propagated."""
        producer = MagicMock()
        producer.flush.side_effect = RuntimeError("flush timeout")
        handler = DLQHandler(producer, DLQConfig())

        handler.send(
            source_topic="t",
            partition=0,
            offset=0,
            key=None,
            value=None,
            error=ValueError("original error"),
        )

    def test_produce_failure_logs_error(self):
        """DLQ write failure is logged at error level."""
        producer = MagicMock()
        producer.produce.side_effect = RuntimeError("broker down")
        handler = DLQHandler(producer, DLQConfig())

        with patch("cdc_platform.streaming.dlq.logger") as mock_logger:
            handler.send(
                source_topic="t",
                partition=0,
                offset=0,
                key=None,
                value=None,
                error=ValueError("bad data"),
            )
            mock_logger.error.assert_called_once()
            call_kwargs = mock_logger.error.call_args.kwargs
            assert call_kwargs["dlq_error"] == "broker down"
            assert call_kwargs["original_error"] == "bad data"


class TestDLQAsyncFlush:
    def test_sync_flush_when_interval_eq0(self):
        """Default behavior: synchronous flush after each produce."""
        producer = MagicMock()
        handler = DLQHandler(producer, DLQConfig(flush_interval_seconds=0.0))

        handler.send(
            source_topic="t",
            partition=0,
            offset=0,
            key=None,
            value=None,
            error=ValueError("err"),
        )

        producer.flush.assert_called_once_with(timeout=10)
        producer.poll.assert_not_called()

    def test_poll_when_interval_gt0(self):
        """When flush_interval > 0, use poll(0) instead of sync flush."""
        producer = MagicMock()
        handler = DLQHandler(producer, DLQConfig(flush_interval_seconds=5.0))

        handler.send(
            source_topic="t",
            partition=0,
            offset=0,
            key=None,
            value=None,
            error=ValueError("err"),
        )

        producer.poll.assert_called_once_with(0)
        producer.flush.assert_not_called()

    def test_explicit_flush_drains_pending(self):
        """Explicit flush() calls producer.flush for shutdown."""
        producer = MagicMock()
        handler = DLQHandler(producer, DLQConfig(flush_interval_seconds=5.0))

        handler.flush(timeout=15.0)

        producer.flush.assert_called_once_with(timeout=15.0)
