"""Dead Letter Queue handler."""

from __future__ import annotations

import time
import traceback

import structlog
from confluent_kafka import Producer

from cdc_platform.config.models import DLQConfig
from cdc_platform.streaming.topics import dlq_topic_name

logger = structlog.get_logger()


class DLQHandler:
    """Routes failed messages to a dead-letter topic with diagnostic headers."""

    def __init__(self, producer: Producer, config: DLQConfig | None = None) -> None:
        self._producer = producer
        self._config = config or DLQConfig()
        self._flush_interval = self._config.flush_interval_seconds

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
        """Send a failed message to the DLQ topic with diagnostic headers."""
        if not self._config.enabled:
            return

        dlq = dlq_topic_name(source_topic, self._config.topic_suffix)

        headers: dict[str, str] = {}
        if self._config.include_headers:
            headers = {
                "dlq.source.topic": source_topic,
                "dlq.source.partition": str(partition),
                "dlq.source.offset": str(offset),
                "dlq.error.message": str(error),
                "dlq.error.type": type(error).__name__,
                "dlq.error.stacktrace": traceback.format_exc(),
                "dlq.timestamp": str(int(time.time() * 1000)),
            }
            if extra_headers:
                headers.update(extra_headers)

        kafka_headers: list[tuple[str, str | bytes | None]] = [
            (k, v.encode() if isinstance(v, str) else v) for k, v in headers.items()
        ]

        try:
            self._producer.produce(
                topic=dlq,
                key=key,
                value=value,
                headers=kafka_headers,
            )
            if self._flush_interval <= 0:
                self._producer.flush(timeout=10)
            else:
                self._producer.poll(0)
        except Exception as dlq_exc:
            logger.error(
                "dlq.write_failed",
                topic=dlq,
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                original_error=str(error),
                dlq_error=str(dlq_exc),
            )
            return
        logger.warning(
            "dlq.message_sent",
            topic=dlq,
            source_topic=source_topic,
            partition=partition,
            offset=offset,
            error=str(error),
        )

    def flush(self, timeout: float = 30.0) -> None:
        """Flush all pending DLQ messages. Called during pipeline shutdown."""
        self._producer.flush(timeout=timeout)
