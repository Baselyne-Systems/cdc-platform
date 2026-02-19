"""Transport-agnostic event source protocol.

Defines SourceEvent (the universal event envelope) and EventSource
(the protocol every transport — Kafka, Pub/Sub, embedded PG — must satisfy).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(slots=True)
class SourceEvent:
    """Universal CDC event envelope — transport-agnostic.

    Every transport adapter converts its native message into this dataclass
    before handing it to the pipeline.
    """

    key: dict[str, Any] | None
    value: dict[str, Any] | None
    topic: str  # logical channel (Kafka topic, PG slot name, Pub/Sub subscription)
    partition: int  # shard (Kafka partition, 0 for non-partitioned)
    offset: int  # position (Kafka offset, PG LSN, etc.)
    raw: Any = field(default=None, repr=False)  # original transport message


@runtime_checkable
class EventSource(Protocol):
    """Protocol that every event transport must satisfy.

    The pipeline calls these methods without knowing which transport is in use.
    """

    async def start(
        self,
        handler: Any,
        on_assign: Any | None = None,
        on_revoke: Any | None = None,
    ) -> None:
        """Begin consuming events; call *handler(event)* for each one."""
        ...

    def commit_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        """Commit processed offsets back to the transport."""
        ...

    def stop(self) -> None:
        """Signal the source to stop consuming."""
        ...

    async def health(self) -> dict[str, Any]:
        """Return transport-specific health information."""
        ...
