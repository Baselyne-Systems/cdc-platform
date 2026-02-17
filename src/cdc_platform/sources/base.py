"""Abstract source connector protocol.

New source types (MySQL, MongoDB, etc.) implement this protocol to plug
into the platform without modifying core code.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from cdc_platform.config.models import PipelineConfig


@runtime_checkable
class SourceConnector(Protocol):
    """Protocol that every CDC source connector must satisfy."""

    async def register(self, pipeline: PipelineConfig) -> dict[str, Any]:
        """Deploy / register the connector, returning its status."""
        ...

    async def status(self, connector_name: str) -> dict[str, Any]:
        """Return the current connector status."""
        ...

    async def delete(self, connector_name: str) -> None:
        """Remove the connector."""
        ...

    async def pause(self, connector_name: str) -> None:
        """Pause the connector."""
        ...

    async def resume(self, connector_name: str) -> None:
        """Resume a paused connector."""
        ...

    async def restart(self, connector_name: str) -> None:
        """Restart the connector (and all its tasks)."""
        ...
