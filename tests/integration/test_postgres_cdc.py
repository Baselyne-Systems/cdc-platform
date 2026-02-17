"""End-to-end: PG insert → Debezium → Kafka → consume → assert."""

from __future__ import annotations

import time
from typing import Any

import psycopg2
import pytest
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


def _make_avro_consumer(
    topic: str,
) -> tuple[Consumer, AvroDeserializer]:
    """Create a Kafka consumer and Avro deserializer for the given topic."""
    sr_client = SchemaRegistryClient({"url": "http://localhost:8081"})
    deserializer = AvroDeserializer(sr_client)
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"test-{time.time()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    return consumer, deserializer


def _consume_messages(
    topic: str,
    *,
    timeout: float = 30.0,
    max_messages: int = 10,
) -> list[dict[str, Any]]:
    """Consume up to max_messages from a topic with Avro deserialization."""
    consumer, deserializer = _make_avro_consumer(topic)
    messages: list[dict[str, Any]] = []
    deadline = time.time() + timeout
    try:
        while time.time() < deadline and len(messages) < max_messages:
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
                messages.append(value)
    finally:
        consumer.close()
    return messages


@pytest.mark.integration
class TestPostgresCDC:
    def test_snapshot_produces_existing_rows(
        self, docker_services, _register_connector, pipeline
    ):
        """The initial snapshot should produce events for seed data."""
        messages = _consume_messages("cdc.public.customers", timeout=60)
        # init.sql seeds 2 customers
        assert len(messages) >= 2
        emails = {m.get("after", {}).get("email") for m in messages if m.get("after")}
        assert "alice@example.com" in emails

    def test_insert_captured(self, docker_services, _register_connector, pg_dsn: str):
        """A live INSERT should produce a CDC event."""
        conn = psycopg2.connect(pg_dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO customers (email, full_name) VALUES (%s, %s)",
                ("charlie@example.com", "Charlie Brown"),
            )
        conn.close()

        messages = _consume_messages("cdc.public.customers", timeout=30)
        emails = {m.get("after", {}).get("email") for m in messages if m.get("after")}
        assert "charlie@example.com" in emails

    def test_update_captured(self, docker_services, _register_connector, pg_dsn: str):
        """An UPDATE should produce a CDC event with before/after."""
        conn = psycopg2.connect(pg_dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE customers SET full_name = %s WHERE email = %s",
                ("Alice Updated", "alice@example.com"),
            )
        conn.close()

        messages = _consume_messages("cdc.public.customers", timeout=30)
        updates = [m for m in messages if m.get("op") == "u"]
        assert len(updates) >= 1

    def test_delete_produces_event(
        self, docker_services, _register_connector, pg_dsn: str
    ):
        """A DELETE should produce a CDC event."""
        conn = psycopg2.connect(pg_dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM customers WHERE email = %s", ("charlie@example.com",)
            )
        conn.close()

        messages = _consume_messages("cdc.public.customers", timeout=30)
        deletes = [m for m in messages if m.get("op") == "d"]
        assert len(deletes) >= 1
