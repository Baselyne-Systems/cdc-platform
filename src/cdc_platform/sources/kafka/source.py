"""Kafka EventSource — wraps CDCConsumer behind the EventSource protocol."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from confluent_kafka import Message

from cdc_platform.config.models import DLQConfig, KafkaConfig
from cdc_platform.sources.base import SourceEvent
from cdc_platform.sources.debezium.client import DebeziumClient
from cdc_platform.sources.debezium.config import connector_name
from cdc_platform.streaming.consumer import CDCConsumer

# Handler type for the pipeline: receives a SourceEvent
SourceEventHandler = Callable[[SourceEvent], Awaitable[None]]
PartitionCallback = Callable[[list[tuple[str, int]]], None]


class KafkaEventSource:
    """Wraps CDCConsumer, converting confluent_kafka.Message to SourceEvent."""

    def __init__(
        self,
        topics: list[str],
        kafka_config: KafkaConfig,
        dlq_config: DLQConfig | None = None,
        connector_config: Any = None,
        pipeline: Any = None,
    ) -> None:
        self._topics = topics
        self._kafka_config = kafka_config
        self._dlq_config = dlq_config
        self._connector_config = connector_config
        self._pipeline = pipeline
        self._consumer: CDCConsumer | None = None

    async def start(
        self,
        handler: SourceEventHandler,
        on_assign: PartitionCallback | None = None,
        on_revoke: PartitionCallback | None = None,
    ) -> None:
        """Build CDCConsumer and start consuming."""

        async def _adapted_handler(
            key: dict[str, Any] | None,
            value: dict[str, Any] | None,
            msg: Message,
        ) -> None:
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            assert topic is not None
            assert partition is not None
            assert offset is not None
            event = SourceEvent(
                key=key,
                value=value,
                topic=topic,
                partition=partition,
                offset=offset,
                raw=msg,
            )
            await handler(event)

        self._consumer = CDCConsumer(
            topics=self._topics,
            kafka_config=self._kafka_config,
            handler=_adapted_handler,
            dlq_config=self._dlq_config,
            on_assign=on_assign,
            on_revoke=on_revoke,
        )
        await self._consumer.consume()

    def commit_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        if self._consumer is not None:
            self._consumer.commit_offsets(offsets)

    def stop(self) -> None:
        if self._consumer is not None:
            self._consumer.stop()

    async def health(self) -> dict[str, Any]:
        """Check source connector (Debezium) status."""
        if self._connector_config is None or self._pipeline is None:
            return {"status": "unknown", "detail": "no connector config"}
        try:
            name = connector_name(self._pipeline)
            async with DebeziumClient(self._connector_config) as client:
                return await client.get_connector_status(name)
        except Exception as exc:
            return {"status": "error", "error": str(exc)}
