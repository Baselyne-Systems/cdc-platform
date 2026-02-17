"""Kafka producer — used by DLQ and extensible for future sinks."""

from __future__ import annotations

import structlog
from confluent_kafka import Producer

from cdc_platform.config.models import KafkaConfig

logger = structlog.get_logger()


def create_producer(config: KafkaConfig) -> Producer:
    """Create an idempotent Kafka producer."""
    return Producer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "enable.idempotence": config.enable_idempotence,
            "acks": config.acks,
        }
    )


def produce_message(
    producer: Producer,
    topic: str,
    value: bytes,
    *,
    key: bytes | None = None,
    headers: dict[str, str] | None = None,
) -> None:
    """Produce a single message and poll for delivery."""
    kafka_headers: list[tuple[str, str | bytes | None]] = [
        (k, v.encode()) for k, v in (headers or {}).items()
    ]
    producer.produce(
        topic=topic,
        value=value,
        key=key,
        headers=kafka_headers,
    )
    producer.flush(timeout=10)
