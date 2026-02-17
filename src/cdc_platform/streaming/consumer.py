"""Kafka consumer with Avro deserialization and DLQ routing."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from cdc_platform.config.models import DLQConfig, KafkaConfig
from cdc_platform.streaming.dlq import DLQHandler
from cdc_platform.streaming.producer import create_producer

logger = structlog.get_logger()

# Callback types
AsyncMessageHandler = Callable[
    [dict[str, Any] | None, dict[str, Any] | None, Message], Awaitable[None]
]


PartitionCallback = Callable[[list[tuple[str, int]]], None]


class CDCConsumer:
    """High-level CDC consumer with Avro deser, manual commit, and DLQ."""

    def __init__(
        self,
        topics: list[str],
        kafka_config: KafkaConfig,
        handler: AsyncMessageHandler,
        *,
        dlq_config: DLQConfig | None = None,
        on_assign: PartitionCallback | None = None,
        on_revoke: PartitionCallback | None = None,
    ) -> None:
        self._handler = handler

        self._on_assign = on_assign
        self._on_revoke = on_revoke

        self._topics = topics
        self._kafka_config = kafka_config
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

        self._dlq: DLQHandler | None
        dlq_cfg = dlq_config or DLQConfig()
        if dlq_cfg.enabled:
            producer = create_producer(kafka_config)
            self._dlq = DLQHandler(producer, dlq_cfg)
        else:
            self._dlq = None

    def _deserialize(
        self, msg: Message
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        topic = msg.topic()
        assert topic is not None
        key = None
        value = None
        if msg.key() is not None:
            ctx = SerializationContext(topic, MessageField.KEY)
            key = self._key_deser(msg.key(), ctx)
        if msg.value() is not None:
            ctx = SerializationContext(topic, MessageField.VALUE)
            value = self._value_deser(msg.value(), ctx)
        return key, value

    def _handle_assign(self, consumer: Any, partitions: list[Any]) -> None:
        if self._on_assign:
            self._on_assign([(tp.topic, tp.partition) for tp in partitions])

    def _handle_revoke(self, consumer: Any, partitions: list[Any]) -> None:
        if self._on_revoke:
            self._on_revoke([(tp.topic, tp.partition) for tp in partitions])

    async def consume(self, *, poll_timeout: float = 1.0) -> None:
        """Async consume loop — polls in a thread, awaits async handler."""
        self._running = True
        self._consumer.subscribe(
            self._topics,
            on_assign=self._handle_assign,
            on_revoke=self._handle_revoke,
        )
        self._install_signal_handlers()

        loop = asyncio.get_running_loop()
        logger.info("consumer.started", topics=self._topics)
        try:
            while self._running:
                polled_message = await loop.run_in_executor(
                    None, self._consumer.poll, poll_timeout
                )
                if polled_message is None:
                    continue

                err = polled_message.error()
                if err and err.code() == KafkaError._PARTITION_EOF:  # type: ignore[attr-defined]
                    continue
                if err:
                    raise KafkaException(err)

                try:
                    key, value = self._deserialize(polled_message)
                    await self._handler(key, value, polled_message)
                except Exception as exc:
                    self._handle_error(polled_message, exc)
        finally:
            self._consumer.close()
            logger.info("consumer.stopped")

    def _install_signal_handlers(self) -> None:
        def _shutdown(signum: int, frame: Any) -> None:
            logger.info("consumer.shutdown_signal", signal=signum)
            self._running = False

        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

    def _handle_error(self, msg: Message, exc: Exception) -> None:
        logger.error(
            "consumer.handler_error",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            error=str(exc),
        )
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        if (
            self._dlq
            and topic is not None
            and partition is not None
            and offset is not None
        ):
            self._dlq.send(
                source_topic=topic,
                partition=partition,
                offset=offset,
                key=msg.key(),
                value=msg.value(),
                error=exc,
            )
        self._consumer.commit(message=msg)

    def commit_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        """Commit specific offsets for (topic, partition) pairs."""
        topic_partitions = [
            TopicPartition(topic, partition, offset + 1)  # committed = next-to-fetch
            for (topic, partition), offset in offsets.items()
        ]
        if topic_partitions:
            self._consumer.commit(offsets=topic_partitions, asynchronous=False)

    def stop(self) -> None:
        """Signal the consume loop to stop."""
        self._running = False
