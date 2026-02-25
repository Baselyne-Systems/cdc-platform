"""Pub/Sub ErrorRouter — publishes failed events to a DLQ topic."""

from __future__ import annotations

import time
import traceback

import structlog

from cdc_platform.config.models import DLQConfig, PubSubConfig
from cdc_platform.sources.pubsub.naming import pubsub_dlq_topic_name

logger = structlog.get_logger()


class PubSubErrorRouter:
    """Routes failed events to a Pub/Sub DLQ topic with diagnostic attributes."""

    def __init__(self, pubsub_config: PubSubConfig, dlq_config: DLQConfig) -> None:
        self._pubsub_config = pubsub_config
        self._dlq_config = dlq_config
        self._publisher = None

    def _get_publisher(self):  # noqa: ANN202
        if self._publisher is None:
            from google.cloud import pubsub_v1

            self._publisher = pubsub_v1.PublisherClient()
        return self._publisher

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
        """Publish a failed event to the DLQ topic."""
        if not self._dlq_config.enabled:
            return

        publisher = self._get_publisher()
        dlq_topic = pubsub_dlq_topic_name(
            self._pubsub_config.project_id,
            source_topic,
            self._dlq_config.topic_suffix,
        )

        attributes: dict[str, str] = {}
        if self._dlq_config.include_headers:
            attributes = {
                "dlq.source.topic": source_topic,
                "dlq.source.partition": str(partition),
                "dlq.source.offset": str(offset),
                "dlq.error.message": str(error),
                "dlq.error.type": type(error).__name__,
                "dlq.error.stacktrace": traceback.format_exc(),
                "dlq.timestamp": str(int(time.time() * 1000)),
            }
            if extra_headers:
                attributes.update(extra_headers)

        data = value or b""

        try:
            future = publisher.publish(dlq_topic, data=data, **attributes)
            future.result()
            logger.warning(
                "pubsub_dlq.message_sent",
                topic=dlq_topic,
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                error=str(error),
            )
        except Exception as dlq_exc:
            logger.error(
                "pubsub_dlq.write_failed",
                topic=dlq_topic,
                source_topic=source_topic,
                error=str(dlq_exc),
            )

    def flush(self, timeout: float = 30.0) -> None:
        """No-op — Pub/Sub publish is synchronous with result()."""
