"""PubSubWalPublisher — WalPublisher implementation for Google Pub/Sub."""

from __future__ import annotations

import structlog

from cdc_platform.config.models import PubSubConfig
from cdc_platform.sources.pubsub.naming import pubsub_topic_name

logger = structlog.get_logger()


class PubSubWalPublisher:
    """Publishes WAL changes to Google Cloud Pub/Sub topics.

    Implements the WalPublisher protocol.  Publishes are non-blocking;
    futures are collected and resolved in ``flush()``.
    """

    def __init__(self, config: PubSubConfig) -> None:
        self._config = config
        self._publisher = None
        self._pending_futures: list = []

    def _get_publisher(self):  # noqa: ANN202
        if self._publisher is None:
            from google.cloud import pubsub_v1

            if self._config.ordering_enabled:
                from google.cloud.pubsub_v1 import types

                publisher_options = types.PublisherOptions(
                    enable_message_ordering=True,
                )
                self._publisher = pubsub_v1.PublisherClient(
                    publisher_options=publisher_options,
                )
            else:
                self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

    async def publish(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        ordering_key: str | None = None,
    ) -> None:
        """Publish a message to a Pub/Sub topic (non-blocking).

        The returned future is collected and resolved during ``flush()``.
        """
        publisher = self._get_publisher()
        full_topic = pubsub_topic_name(self._config.project_id, topic)

        kwargs: dict = {
            "topic": full_topic,
            "data": value,
            "key": key.decode("utf-8", errors="replace") if key else "",
        }
        if ordering_key and self._config.ordering_enabled:
            kwargs["ordering_key"] = ordering_key

        future = publisher.publish(**kwargs)
        self._pending_futures.append(future)

    async def flush(self) -> None:
        """Wait for all pending publish futures to complete."""
        failures = 0
        for future in self._pending_futures:
            try:
                future.result()
            except Exception:
                failures += 1
                logger.error("pubsub.publish_failed", exc_info=True)
        self._pending_futures.clear()
        if failures:
            logger.warning("pubsub.flush_completed_with_errors", failed=failures)

    async def close(self) -> None:
        """Shut down the publisher transport."""
        if self._publisher is not None:
            self._publisher.stop()
            self._publisher = None
        self._pending_futures.clear()
