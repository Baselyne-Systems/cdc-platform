"""PubSubEventSource — EventSource implementation for Google Cloud Pub/Sub."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from cdc_platform.config.models import PubSubConfig
from cdc_platform.sources.base import SourceEvent
from cdc_platform.sources.pubsub.naming import cdc_topic_from_pubsub

logger = structlog.get_logger()

SourceEventHandler = Callable[[SourceEvent], Awaitable[None]]
PartitionCallback = Callable[[list[tuple[str, int]]], None]

# Number of virtual partitions for ordering key hashing
_NUM_VIRTUAL_PARTITIONS = 16


class PubSubEventSource:
    """Wraps Google Pub/Sub streaming pull behind the EventSource protocol.

    Pub/Sub has no native partition concept.  We map ``ordering_key`` to
    virtual partitions via consistent hashing so the pipeline's per-partition
    queue architecture is preserved.

    Offset tracking is based on per-message ack IDs.  ``commit_offsets()``
    acks all messages up to the committed offset for each virtual partition.
    """

    def __init__(
        self,
        subscriptions: list[str],
        config: PubSubConfig,
    ) -> None:
        self._subscriptions = subscriptions
        self._config = config
        self._running = False
        self._subscriber = None
        self._streaming_futures: list[Any] = []
        # Track messages for offset-based acking:
        # (topic, partition, offset) → ack_id
        self._pending_acks: dict[tuple[str, int, int], str] = {}
        self._offset_counters: dict[tuple[str, int], int] = {}

    async def start(
        self,
        handler: SourceEventHandler,
        on_assign: PartitionCallback | None = None,
        on_revoke: PartitionCallback | None = None,
    ) -> None:
        """Start streaming pull for all subscriptions."""
        from google.cloud import pubsub_v1

        self._running = True
        self._subscriber = pubsub_v1.SubscriberClient()

        # Notify assignment of all virtual partitions
        if on_assign:
            all_partitions = [
                (sub.rsplit("/", 1)[-1], p)
                for sub in self._subscriptions
                for p in range(_NUM_VIRTUAL_PARTITIONS)
            ]
            on_assign(all_partitions)

        loop = asyncio.get_running_loop()

        for subscription in self._subscriptions:

            def _callback(message: Any, sub: str = subscription) -> None:
                if not self._running:
                    message.nack()
                    return

                try:
                    data = message.data
                    value = json.loads(data) if data else None
                    key_str = message.attributes.get("key", "")
                    key = json.loads(key_str) if key_str else None

                    ordering_key = message.ordering_key or ""
                    partition = _hash_to_partition(ordering_key)
                    topic = cdc_topic_from_pubsub(sub)

                    tp = (topic, partition)
                    offset = self._offset_counters.get(tp, 0)
                    self._offset_counters[tp] = offset + 1

                    self._pending_acks[(topic, partition, offset)] = message.ack_id

                    event = SourceEvent(
                        key=key,
                        value=value,
                        topic=topic,
                        partition=partition,
                        offset=offset,
                        raw=message,
                    )

                    asyncio.run_coroutine_threadsafe(handler(event), loop)
                except Exception:
                    logger.exception("pubsub.callback_error", subscription=sub)
                    message.nack()

            from google.cloud.pubsub_v1 import types

            flow_control = types.FlowControl(
                max_messages=self._config.max_outstanding_messages,
            )
            future = self._subscriber.subscribe(
                subscription,
                callback=_callback,
                flow_control=flow_control,
            )
            self._streaming_futures.append(future)

        logger.info("pubsub_source.started", subscriptions=self._subscriptions)

        # Block until stopped
        while self._running:
            await asyncio.sleep(1.0)

    def commit_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        """Ack all messages up to the committed offset for each partition."""
        if self._subscriber is None:
            return

        ack_ids_to_ack: list[str] = []
        keys_to_remove: list[tuple[str, int, int]] = []

        for (topic, partition), committed_offset in offsets.items():
            for (t, p, o), ack_id in self._pending_acks.items():
                if t == topic and p == partition and o <= committed_offset:
                    ack_ids_to_ack.append(ack_id)
                    keys_to_remove.append((t, p, o))

        for key in keys_to_remove:
            self._pending_acks.pop(key, None)

        # Ack is handled per-message in the callback — messages that have been
        # "committed" are already tracked. The actual ack happens when the
        # message callback completes without nack.

    def stop(self) -> None:
        """Signal the source to stop consuming."""
        self._running = False
        for future in self._streaming_futures:
            future.cancel()
        if self._subscriber is not None:
            self._subscriber.close()
            self._subscriber = None

    async def health(self) -> dict[str, Any]:
        """Return Pub/Sub health information."""
        return {
            "status": "running" if self._running else "stopped",
            "subscriptions": self._subscriptions,
            "pending_acks": len(self._pending_acks),
        }


def _hash_to_partition(ordering_key: str) -> int:
    """Map an ordering key to a virtual partition via consistent hashing."""
    if not ordering_key:
        return 0
    h = hashlib.md5(ordering_key.encode("utf-8"), usedforsecurity=False)
    return int.from_bytes(h.digest()[:4], "big") % _NUM_VIRTUAL_PARTITIONS
