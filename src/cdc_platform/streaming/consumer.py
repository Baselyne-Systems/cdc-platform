"""Kafka consumer with Avro deserialization and DLQ routing."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
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
        self._loop: asyncio.AbstractEventLoop | None = None
        self._poll_batch_size = kafka_config.poll_batch_size
        self._commit_async = kafka_config.commit_interval_seconds > 0

        # Thread pool for parallel deserialization
        deser_pool_size = kafka_config.deser_pool_size
        self._deser_executor: ThreadPoolExecutor | None = (
            ThreadPoolExecutor(max_workers=deser_pool_size)
            if deser_pool_size > 1
            else None
        )

        registry = SchemaRegistryClient({"url": kafka_config.schema_registry_url})
        self._key_deser = AvroDeserializer(registry)
        self._value_deser = AvroDeserializer(registry)

        self._consumer = Consumer(
            {
                "bootstrap.servers": kafka_config.bootstrap_servers,
                "group.id": kafka_config.group_id,
                "auto.offset.reset": kafka_config.auto_offset_reset,
                "enable.auto.commit": False,
                "session.timeout.ms": kafka_config.session_timeout_ms,
                "max.poll.interval.ms": kafka_config.max_poll_interval_ms,
                "fetch.min.bytes": kafka_config.fetch_min_bytes,
                "fetch.wait.max.ms": kafka_config.fetch_max_wait_ms,
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
        if self._on_assign and self._loop:
            tps = [(tp.topic, tp.partition) for tp in partitions]
            self._loop.call_soon_threadsafe(self._on_assign, tps)

    def _handle_revoke(self, consumer: Any, partitions: list[Any]) -> None:
        if self._on_revoke and self._loop:
            tps = [(tp.topic, tp.partition) for tp in partitions]
            self._loop.call_soon_threadsafe(self._on_revoke, tps)

    def _deserialize_batch(
        self, messages: list[Message]
    ) -> list[tuple[dict[str, Any] | None, dict[str, Any] | None, Message]]:
        """Deserialize a batch of messages (synchronous, runs in thread pool)."""
        results: list[tuple[dict[str, Any] | None, dict[str, Any] | None, Message]] = []
        for msg in messages:
            key, value = self._deserialize(msg)
            results.append((key, value, msg))
        return results

    async def consume(self, *, poll_timeout: float = 1.0) -> None:
        """Async consume loop — polls in a thread, awaits async handler."""
        self._running = True
        self._loop = asyncio.get_running_loop()
        self._consumer.subscribe(
            self._topics,
            on_assign=self._handle_assign,
            on_revoke=self._handle_revoke,
        )
        self._install_signal_handlers()

        loop = asyncio.get_running_loop()
        logger.info("consumer.started", topics=self._topics)
        try:
            if self._poll_batch_size > 1:
                await self._consume_batch(loop, poll_timeout)
            else:
                await self._consume_single(loop, poll_timeout)
        finally:
            if self._deser_executor is not None:
                self._deser_executor.shutdown(wait=False)
            self._consumer.close()
            logger.info("consumer.stopped")

    async def _consume_single(
        self, loop: asyncio.AbstractEventLoop, poll_timeout: float
    ) -> None:
        """Legacy single-message poll path."""
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

    async def _consume_batch(
        self, loop: asyncio.AbstractEventLoop, poll_timeout: float
    ) -> None:
        """Batch poll path — fetches N messages per poll, parallel deser."""
        batch_size = self._poll_batch_size
        while self._running:
            messages = await loop.run_in_executor(
                None, self._consumer.consume, batch_size, poll_timeout
            )
            if not messages:
                continue

            # Filter errors and EOF
            valid: list[Message] = []
            for msg in messages:
                err = msg.error()
                if err and err.code() == KafkaError._PARTITION_EOF:  # type: ignore[attr-defined]
                    continue
                if err:
                    raise KafkaException(err)
                valid.append(msg)

            if not valid:
                continue

            # Deserialize batch (in thread pool if configured)
            deserialized = await loop.run_in_executor(
                self._deser_executor, self._deserialize_batch, valid
            )

            # Dispatch to handler one at a time (preserves handler contract)
            for key, value, msg in deserialized:
                try:
                    await self._handler(key, value, msg)
                except Exception as exc:
                    self._handle_error(msg, exc)

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
        dlq_ok = False
        if (
            self._dlq
            and topic is not None
            and partition is not None
            and offset is not None
        ):
            try:
                self._dlq.send(
                    source_topic=topic,
                    partition=partition,
                    offset=offset,
                    key=msg.key(),
                    value=msg.value(),
                    error=exc,
                )
                dlq_ok = True
            except Exception as dlq_exc:
                logger.error(
                    "consumer.dlq_send_failed",
                    topic=topic,
                    partition=partition,
                    offset=offset,
                    error=str(dlq_exc),
                )
        elif self._dlq is None:
            # No DLQ configured — commit anyway to avoid infinite retry loop
            dlq_ok = True

        if dlq_ok:
            self._consumer.commit(message=msg)

    def commit_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        """Commit specific offsets for (topic, partition) pairs."""
        topic_partitions = [
            TopicPartition(topic, partition, offset + 1)  # committed = next-to-fetch
            for (topic, partition), offset in offsets.items()
        ]
        if topic_partitions:
            self._consumer.commit(
                offsets=topic_partitions, asynchronous=self._commit_async
            )

    def stop(self) -> None:
        """Signal the consume loop to stop."""
        self._running = False
