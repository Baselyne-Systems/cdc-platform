"""Kafka consumer with Avro deserialization and DLQ routing."""

from __future__ import annotations

import signal
from collections.abc import Callable
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from cdc_platform.config.models import DLQConfig, KafkaConfig
from cdc_platform.streaming.dlq import DLQHandler
from cdc_platform.streaming.producer import create_producer

logger = structlog.get_logger()

# Callback type: receives the deserialized key and value dicts
MessageHandler = Callable[[dict[str, Any] | None, dict[str, Any] | None, Message], None]


class CDCConsumer:
    """High-level CDC consumer with Avro deser, manual commit, and DLQ."""

    def __init__(
        self,
        topics: list[str],
        kafka_config: KafkaConfig,
        handler: MessageHandler,
        *,
        dlq_config: DLQConfig | None = None,
    ) -> None:
        self._topics = topics
        self._kafka_config = kafka_config
        self._handler = handler
        self._running = False

        registry = SchemaRegistryClient({"url": kafka_config.schema_registry_url})
        self._key_deser = AvroDeserializer(registry)
        self._value_deser = AvroDeserializer(registry)

        self._consumer = Consumer(
            {
                "bootstrap.servers": kafka_config.bootstrap_servers,
                "group.id": kafka_config.group_id,
                "auto.offset.reset": kafka_config.auto_offset_reset,
                "enable.auto.commit": False,
            }
        )

        dlq_cfg = dlq_config or DLQConfig()
        if dlq_cfg.enabled:
            producer = create_producer(kafka_config)
            self._dlq = DLQHandler(producer, dlq_cfg)
        else:
            self._dlq = None

    def _deserialize(self, msg: Message) -> tuple[Any, Any]:
        topic = msg.topic()
        key = None
        value = None
        if msg.key() is not None:
            ctx = SerializationContext(topic, MessageField.KEY)
            key = self._key_deser(msg.key(), ctx)
        if msg.value() is not None:
            ctx = SerializationContext(topic, MessageField.VALUE)
            value = self._value_deser(msg.value(), ctx)
        return key, value

    def consume(self, *, poll_timeout: float = 1.0) -> None:
        """Start the consume loop. Blocks until SIGINT/SIGTERM or stop()."""
        self._running = True
        self._consumer.subscribe(self._topics)

        def _shutdown(signum: int, frame: Any) -> None:
            logger.info("consumer.shutdown_signal", signal=signum)
            self._running = False

        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

        logger.info("consumer.started", topics=self._topics)
        try:
            while self._running:
                msg = self._consumer.poll(poll_timeout)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    key, value = self._deserialize(msg)
                    self._handler(key, value, msg)
                    self._consumer.commit(message=msg)
                except Exception as exc:
                    logger.error(
                        "consumer.handler_error",
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        error=str(exc),
                    )
                    if self._dlq:
                        self._dlq.send(
                            source_topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                            key=msg.key(),
                            value=msg.value(),
                            error=exc,
                        )
                    self._consumer.commit(message=msg)
        finally:
            self._consumer.close()
            logger.info("consumer.stopped")

    def stop(self) -> None:
        """Signal the consume loop to stop."""
        self._running = False
