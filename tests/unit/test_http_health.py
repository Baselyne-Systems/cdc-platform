"""Tests for the async HTTP health server."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from cdc_platform.observability.http_health import HealthServer


async def _healthy_check() -> dict[str, Any]:
    return {
        "pipeline_id": "test",
        "source": {"status": "running"},
        "sinks": [{"sink_id": "s1", "status": "running"}],
        "consumer_lag": [],
    }


async def _unhealthy_check() -> dict[str, Any]:
    return {
        "pipeline_id": "test",
        "source": {"status": "error", "error": "connection refused"},
        "sinks": [{"sink_id": "s1", "status": "running"}],
        "consumer_lag": [],
    }


async def _request(port: int, path: str) -> tuple[int, dict[str, Any]]:
    """Send a minimal HTTP GET and return (status_code, json_body)."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
    await writer.drain()
    data = await reader.read(4096)
    writer.close()
    await writer.wait_closed()

    text = data.decode()
    status_line = text.split("\r\n")[0]
    status_code = int(status_line.split(" ")[1])
    body = json.loads(text.split("\r\n\r\n", 1)[1])
    return status_code, body


@pytest.fixture
async def free_port() -> int:
    """Find a free TCP port."""
    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()
    return port


async def test_liveness_returns_200(free_port: int) -> None:
    srv = HealthServer(port=free_port, readiness_check=_healthy_check)
    await srv.start()
    try:
        code, body = await _request(free_port, "/healthz")
        assert code == 200
        assert body == {"status": "ok"}
    finally:
        await srv.stop()


async def test_readiness_returns_200_when_healthy(free_port: int) -> None:
    srv = HealthServer(port=free_port, readiness_check=_healthy_check)
    await srv.start()
    try:
        code, body = await _request(free_port, "/readyz")
        assert code == 200
        assert body["pipeline_id"] == "test"
    finally:
        await srv.stop()


async def test_readiness_returns_503_when_unhealthy(free_port: int) -> None:
    srv = HealthServer(port=free_port, readiness_check=_unhealthy_check)
    await srv.start()
    try:
        code, body = await _request(free_port, "/readyz")
        assert code == 503
        assert body["source"]["status"] == "error"
    finally:
        await srv.stop()


async def test_unknown_path_returns_404(free_port: int) -> None:
    srv = HealthServer(port=free_port, readiness_check=_healthy_check)
    await srv.start()
    try:
        code, body = await _request(free_port, "/foo")
        assert code == 404
    finally:
        await srv.stop()


async def test_start_stop_lifecycle(free_port: int) -> None:
    srv = HealthServer(port=free_port, readiness_check=_healthy_check)
    await srv.start()
    # Verify serving
    code, _ = await _request(free_port, "/healthz")
    assert code == 200
    # Stop
    await srv.stop()
    # Verify closed — connection should be refused
    with pytest.raises(OSError):
        await _request(free_port, "/healthz")
