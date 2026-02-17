"""Unit tests for the webhook sink connector."""

from __future__ import annotations

import httpx
import pytest
import respx

from cdc_platform.config.models import (
    RetryConfig,
    SinkConfig,
    SinkType,
    WebhookSinkConfig,
)
from cdc_platform.sinks.webhook import WebhookSink


def _make_sink(
    url: str = "http://example.com/hook",
    auth_token: str | None = None,
    max_attempts: int = 2,
) -> WebhookSink:
    cfg = SinkConfig(
        sink_id="test-webhook",
        sink_type=SinkType.WEBHOOK,
        retry=RetryConfig(
            max_attempts=max_attempts,
            initial_wait_seconds=0.01,
            max_wait_seconds=0.1,
        ),
        webhook=WebhookSinkConfig(url=url, auth_token=auth_token),
    )
    return WebhookSink(cfg)


@pytest.mark.asyncio
class TestWebhookSink:
    async def test_write_posts_event(self, respx_mock: respx.MockRouter):
        sink = _make_sink()
        respx_mock.post("http://example.com/hook").mock(
            return_value=httpx.Response(200)
        )

        await sink.start()
        try:
            await sink.write(
                key={"id": 1},
                value={"name": "Alice"},
                topic="cdc.public.customers",
                partition=0,
                offset=42,
            )

            assert respx_mock.calls.call_count == 1
            request = respx_mock.calls[0].request
            body = request.read()
            import json

            payload = json.loads(body)
            assert payload["key"] == {"id": 1}
            assert payload["value"] == {"name": "Alice"}
            assert payload["metadata"]["topic"] == "cdc.public.customers"
            assert payload["metadata"]["partition"] == 0
            assert payload["metadata"]["offset"] == 42
        finally:
            await sink.stop()

    async def test_auth_header_set(self, respx_mock: respx.MockRouter):
        sink = _make_sink(auth_token="my-secret-token")
        respx_mock.post("http://example.com/hook").mock(
            return_value=httpx.Response(200)
        )

        await sink.start()
        try:
            await sink.write(key=None, value={"x": 1}, topic="t", partition=0, offset=0)

            request = respx_mock.calls[0].request
            assert request.headers["Authorization"] == "Bearer my-secret-token"
        finally:
            await sink.stop()

    async def test_retries_on_5xx(self, respx_mock: respx.MockRouter):
        sink = _make_sink(max_attempts=3)
        route = respx_mock.post("http://example.com/hook")
        route.side_effect = [
            httpx.Response(503),
            httpx.Response(502),
            httpx.Response(200),
        ]

        await sink.start()
        try:
            await sink.write(key=None, value={"x": 1}, topic="t", partition=0, offset=0)
            assert respx_mock.calls.call_count == 3
        finally:
            await sink.stop()

    async def test_raises_after_max_retries(self, respx_mock: respx.MockRouter):
        sink = _make_sink(max_attempts=2)
        respx_mock.post("http://example.com/hook").mock(
            return_value=httpx.Response(500)
        )

        await sink.start()
        try:
            with pytest.raises(httpx.HTTPStatusError):
                await sink.write(
                    key=None, value={"x": 1}, topic="t", partition=0, offset=0
                )
        finally:
            await sink.stop()

    async def test_health_running(self):
        sink = _make_sink()
        await sink.start()
        h = await sink.health()
        assert h["status"] == "running"
        assert h["sink_id"] == "test-webhook"
        await sink.stop()

    async def test_health_stopped(self):
        sink = _make_sink()
        h = await sink.health()
        assert h["status"] == "stopped"

    async def test_write_before_start_raises(self):
        sink = _make_sink()
        with pytest.raises(RuntimeError, match="not started"):
            await sink.write(key=None, value=None, topic="t", partition=0, offset=0)

    async def test_flush_is_noop(self):
        sink = _make_sink()
        await sink.start()
        await sink.flush()  # Should not raise
        await sink.stop()

    async def test_sink_id(self):
        sink = _make_sink()
        assert sink.sink_id == "test-webhook"

    async def test_flushed_offsets_advances_per_write(
        self, respx_mock: respx.MockRouter
    ):
        sink = _make_sink()
        respx_mock.post("http://example.com/hook").mock(
            return_value=httpx.Response(200)
        )

        await sink.start()
        try:
            assert sink.flushed_offsets == {}

            await sink.write(key=None, value={"x": 1}, topic="t", partition=0, offset=5)
            assert sink.flushed_offsets == {("t", 0): 5}

            await sink.write(
                key=None, value={"x": 2}, topic="t", partition=0, offset=10
            )
            assert sink.flushed_offsets == {("t", 0): 10}

            await sink.write(key=None, value={"x": 3}, topic="t", partition=1, offset=3)
            assert sink.flushed_offsets == {("t", 0): 10, ("t", 1): 3}
        finally:
            await sink.stop()

    async def test_flushed_offsets_not_updated_on_failure(
        self, respx_mock: respx.MockRouter
    ):
        sink = _make_sink(max_attempts=1)
        respx_mock.post("http://example.com/hook").mock(
            return_value=httpx.Response(500)
        )

        await sink.start()
        try:
            with pytest.raises(httpx.HTTPStatusError):
                await sink.write(
                    key=None, value={"x": 1}, topic="t", partition=0, offset=5
                )

            assert sink.flushed_offsets == {}
        finally:
            await sink.stop()
