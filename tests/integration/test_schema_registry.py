"""Verify that Debezium registers Avro schemas in Schema Registry."""

from __future__ import annotations

import time
from typing import Any

import httpx
import pytest
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


def _consume_one(topic: str, *, timeout: float = 60.0) -> dict[str, Any] | None:
    """Consume one message from a topic with Avro deserialization."""
    sr_client = SchemaRegistryClient({"url": "http://localhost:8081"})
    deserializer = AvroDeserializer(sr_client)
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"schema-test-{time.time()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() in (
                    KafkaError._PARTITION_EOF,
                    KafkaError.UNKNOWN_TOPIC_OR_PART,
                ):
                    continue
                raise Exception(msg.error())
            raw = msg.value()
            if raw is not None:
                ctx = SerializationContext(topic, MessageField.VALUE)
                value = deserializer(raw, ctx)
                return value
    finally:
        consumer.close()
    return None


@pytest.mark.integration
class TestSchemaRegistry:
    def test_avro_schemas_registered(self, docker_services, _register_connector):
        """Debezium should register Avro schemas for key and value subjects."""
        # Consume at least one message to ensure schemas are registered
        message = _consume_one("cdc.public.customers", timeout=60)
        assert message is not None, "No messages consumed from cdc.public.customers"

        # Verify schemas exist in Schema Registry
        resp = httpx.get("http://localhost:8081/subjects", timeout=10)
        assert resp.status_code == 200
        subjects = resp.json()
        assert "cdc.public.customers-value" in subjects
        assert "cdc.public.customers-key" in subjects

    def test_message_has_envelope_fields(self, docker_services, _register_connector):
        """After connector starts, messages should contain Debezium envelope fields."""
        message = _consume_one("cdc.public.customers", timeout=60)

        assert message is not None, "No messages consumed from cdc.public.customers"
        assert "op" in message, f"Missing 'op' field, got keys: {list(message.keys())}"
        assert "after" in message, (
            f"Missing 'after' field, got keys: {list(message.keys())}"
        )

    def test_message_contains_expected_columns(
        self, docker_services, _register_connector
    ):
        """The message 'after' field should contain expected column names."""
        message = _consume_one("cdc.public.customers", timeout=60)

        assert message is not None, "No messages consumed"
        after = message.get("after", {})
        assert "email" in after, f"Missing 'email' column, got: {list(after.keys())}"
        assert "full_name" in after, (
            f"Missing 'full_name' column, got: {list(after.keys())}"
        )
