"""WalPublisher protocol — abstract interface for WAL change publishing."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class WalPublisher(Protocol):
    """Publishes decoded WAL changes to a downstream transport.

    Implementations: PubSubWalPublisher, KinesisWalPublisher.
    """

    async def publish(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        ordering_key: str | None = None,
    ) -> None:
        """Publish a single WAL change event."""
        ...

    async def flush(self) -> None:
        """Flush any buffered messages."""
        ...

    async def close(self) -> None:
        """Close the publisher and release resources."""
        ...
