"""Lightweight async HTTP health server for Kubernetes probes.

Zero-dependency implementation using ``asyncio.start_server``.
Exposes ``/healthz`` (liveness) and ``/readyz`` (readiness) endpoints.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

import structlog

logger = structlog.get_logger()

ReadinessCheck = Callable[[], Awaitable[dict[str, Any]]]


class HealthServer:
    """Async TCP server that responds to HTTP health probes.

    Parameters
    ----------
    port:
        TCP port to listen on.
    readiness_check:
        Async callable returning a health dict.  A ``"status": "error"`` value
        in any nested sink or source entry causes the readiness probe to return
        503.
    """

    def __init__(self, port: int, readiness_check: ReadinessCheck) -> None:
        self._port = port
        self._readiness_check = readiness_check
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle,
            host="0.0.0.0",
            port=self._port,  # noqa: S104
        )
        logger.info("health.server_started", port=self._port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            logger.info("health.server_stopped")

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            path = self._parse_path(request_line)

            if path == "/healthz":
                await self._respond(writer, 200, {"status": "ok"})
            elif path == "/readyz":
                await self._handle_readiness(writer)
            else:
                await self._respond(writer, 404, {"error": "not found"})
        except Exception:
            logger.debug("health.request_error", exc_info=True)
            with suppress(Exception):
                await self._respond(writer, 500, {"error": "internal server error"})
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _handle_readiness(self, writer: asyncio.StreamWriter) -> None:
        health = await self._readiness_check()
        has_error = self._contains_error(health)
        status_code = 503 if has_error else 200
        await self._respond(writer, status_code, health)

    @staticmethod
    def _contains_error(health: dict[str, Any]) -> bool:
        """Check if any component reports an error status."""
        for value in health.values():
            if isinstance(value, dict) and value.get("status") == "error":
                return True
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and item.get("status") == "error":
                        return True
        return False

    @staticmethod
    def _parse_path(request_line: bytes) -> str:
        parts = request_line.decode("utf-8", errors="replace").strip().split()
        if len(parts) >= 2:
            return parts[1]
        return ""

    @staticmethod
    async def _respond(
        writer: asyncio.StreamWriter, status: int, body: dict[str, Any]
    ) -> None:
        reasons = {
            200: "OK",
            404: "Not Found",
            500: "Internal Server Error",
            503: "Service Unavailable",
        }
        reason = reasons.get(status, "Unknown")
        payload = json.dumps(body).encode()
        header = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        )
        writer.write(header.encode() + payload)
        await writer.drain()
