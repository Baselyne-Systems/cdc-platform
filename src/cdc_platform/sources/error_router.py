"""ErrorRouter protocol — transport-agnostic dead-letter routing."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ErrorRouter(Protocol):
    """Routes failed events to a dead-letter destination."""

    def send(
        self,
        *,
        source_topic: str,
        partition: int,
        offset: int,
        key: bytes | None,
        value: bytes | None,
        error: Exception,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        """Send a failed event to the error destination."""
        ...
