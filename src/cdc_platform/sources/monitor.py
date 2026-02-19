"""SourceMonitor protocol — transport-agnostic monitoring."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SourceMonitor(Protocol):
    """Background monitor for schema changes and consumer lag."""

    async def start(self) -> None:
        """Start background monitoring tasks."""
        ...

    async def stop(self) -> None:
        """Stop background monitoring tasks."""
        ...

    async def get_lag(self) -> list[dict[str, Any]]:
        """Return current consumer lag information."""
        ...
