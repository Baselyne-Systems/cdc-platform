"""Abstract sink connector protocol.

New sink types implement this protocol to plug into the platform
without modifying core code.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SinkConnector(Protocol):
    """Protocol that every CDC sink connector must satisfy."""

    @property
    def sink_id(self) -> str:
        """Unique identifier for this sink instance."""
        ...

    @property
    def flushed_offsets(self) -> dict[tuple[str, int], int]:
        """Max offset durably written per (topic, partition)."""
        ...

    async def start(self) -> None:
        """Initialize resources (connections, HTTP clients, etc.)."""
        ...

    async def write(
        self,
        key: dict[str, Any] | None,
        value: dict[str, Any] | None,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """Write a single CDC event to the sink."""
        ...

    async def flush(self) -> None:
        """Flush any buffered writes."""
        ...

    async def stop(self) -> None:
        """Release resources (close connections, flush remaining)."""
        ...

    async def health(self) -> dict[str, Any]:
        """Return a health-check status dict."""
        ...
