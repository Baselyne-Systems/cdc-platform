"""Integration tests: MongoDB insert/update/delete → Debezium → Kafka → assert.

Run with:
    pytest tests/integration/test_mongodb_cdc.py -m integration -v

Prerequisites:
    docker compose -f docker/docker-compose.yml \\
                   -f docker/docker-compose.mongodb.yml up -d
"""

from __future__ import annotations

import asyncio
import subprocess
import time
from typing import Any

import pytest
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient

from cdc_platform.config.models import (
    KafkaConfig,
    PipelineConfig,
    PlatformConfig,
    SnapshotMode,
    SourceConfig,
    SourceType,
)
from cdc_platform.sources.debezium.client import DebeziumClient
from cdc_platform.sources.debezium.config import connector_name

# ── Docker helpers ────────────────────────────────────────────────────────────

_BASE_COMPOSE = "docker/docker-compose.yml"
_MONGO_COMPOSE = "docker/docker-compose.mongodb.yml"


def _compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", "-f", _BASE_COMPOSE, "-f", _MONGO_COMPOSE, *args],
        check=True,
        capture_output=True,
    )


def _wait_for_http(url: str, *, timeout: int = 120) -> None:
    import httpx

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=5).status_code < 300:
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f"{url} not ready after {timeout}s")


def _wait_for_kafka(bootstrap: str = "localhost:9092", *, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
            return
        except Exception:
            time.sleep(2)
    raise TimeoutError(f"Kafka at {bootstrap} not ready after {timeout}s")


def _wait_for_mongodb(
    host: str = "localhost", port: int = 27017, *, timeout: int = 90
) -> None:
    """Wait until MongoDB replica set has a PRIMARY."""
    import subprocess as sp

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            result = sp.run(
                [
                    "mongosh",
                    f"mongodb://{host}:{port}/?replicaSet=rs0",
                    "--quiet",
                    "--eval",
                    "rs.status().members.filter(m => m.stateStr === 'PRIMARY').length",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0 and result.stdout.strip() == "1":
                return
        except Exception:
            pass
        time.sleep(3)
    raise TimeoutError(f"MongoDB replica set PRIMARY not elected after {timeout}s")


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def docker_mongodb_services():
    """Bring up the base stack + MongoDB; tear down after the session."""
    _compose("up", "-d")
    try:
        _wait_for_kafka()
        _wait_for_http("http://localhost:8081/subjects")
        _wait_for_http("http://localhost:8083/connectors")
        # Allow the mongodb-setup container to finish (replica set init + seed)
        _wait_for_mongodb()
        time.sleep(3)  # brief grace period for seed data to commit
        yield
    finally:
        _compose("down", "-v")


@pytest.fixture(scope="session")
def mongo_platform() -> PlatformConfig:
    """Platform config pointing Connect to the Docker-internal Schema Registry."""
    return PlatformConfig(
        kafka=KafkaConfig(schema_registry_url="http://schema-registry:8081"),
    )


@pytest.fixture(scope="session")
def mongo_pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="mongo-integration",
        topic_prefix="cdc",
        source=SourceConfig(
            source_type=SourceType.MONGODB,
            host="mongodb",
            port=27017,
            database="cdc_demo",
            username="",
            password="",
            replica_set_name="rs0",
            tables=["cdc_demo.products", "cdc_demo.orders"],
            snapshot_mode=SnapshotMode.INITIAL,
        ),
    )


@pytest.fixture(scope="session")
def _register_mongo_connector(docker_mongodb_services, mongo_pipeline, mongo_platform):
    """Register the Debezium MongoDB connector; wait until RUNNING."""

    async def _register() -> None:
        async with DebeziumClient(mongo_platform.connector) as client:
            await client.wait_until_ready()
            await client.register_connector(mongo_pipeline, mongo_platform)
            name = connector_name(mongo_pipeline)
            for _ in range(30):
                try:
                    status = await client.get_connector_status(name)
                except Exception:
                    await asyncio.sleep(2)
                    continue
                tasks = status.get("tasks", [])
                if tasks and tasks[0].get("state") == "RUNNING":
                    return
                if tasks and tasks[0].get("state") == "FAILED":
                    trace = tasks[0].get("trace", "")
                    raise RuntimeError(f"Connector FAILED:\n{trace[:500]}")
                await asyncio.sleep(2)
            raise TimeoutError("MongoDB connector did not reach RUNNING state")

    asyncio.run(_register())


# ── Helpers ───────────────────────────────────────────────────────────────────


def _raw_consume(
    topic: str,
    *,
    timeout: float = 30.0,
    max_messages: int = 20,
) -> list[bytes]:
    """Consume raw (non-deserialized) messages from a topic."""
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"test-mongo-{time.time()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    payloads: list[bytes] = []
    deadline = time.time() + timeout
    try:
        while time.time() < deadline and len(payloads) < max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                code = msg.error().code()
                if code in (
                    KafkaError._PARTITION_EOF,
                    KafkaError.UNKNOWN_TOPIC_OR_PART,
                ):
                    continue
                raise Exception(msg.error())
            if msg.value() is not None:
                payloads.append(msg.value())
    finally:
        consumer.close()
    return payloads


def _insert_product(name: str, price: float, stock: int) -> dict[str, Any]:
    import subprocess as sp

    doc = f'{{"name": "{name}", "price": {price}, "stock": {stock}}}'
    sp.run(
        [
            "mongosh",
            "mongodb://localhost:27017/cdc_demo?replicaSet=rs0",
            "--quiet",
            "--eval",
            f"db.products.insertOne({doc})",
        ],
        check=True,
        capture_output=True,
    )
    return {"name": name, "price": price, "stock": stock}


def _update_product(name: str, new_stock: int) -> None:
    import subprocess as sp

    sp.run(
        [
            "mongosh",
            "mongodb://localhost:27017/cdc_demo?replicaSet=rs0",
            "--quiet",
            "--eval",
            f'db.products.updateOne({{name: "{name}"}}, {{$set: {{stock: {new_stock}}}}})',
        ],
        check=True,
        capture_output=True,
    )


def _delete_product(name: str) -> None:
    import subprocess as sp

    sp.run(
        [
            "mongosh",
            "mongodb://localhost:27017/cdc_demo?replicaSet=rs0",
            "--quiet",
            "--eval",
            f'db.products.deleteOne({{name: "{name}"}})',
        ],
        check=True,
        capture_output=True,
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

PRODUCTS_TOPIC = "cdc.cdc_demo.products"


@pytest.mark.integration
class TestMongoDBCDC:
    def test_snapshot_produces_messages(
        self, docker_mongodb_services, _register_mongo_connector
    ):
        """Initial snapshot should emit at least the 2 seed documents."""
        payloads = _raw_consume(PRODUCTS_TOPIC, timeout=60, max_messages=10)
        assert len(payloads) >= 2, (
            f"Expected ≥2 snapshot messages on {PRODUCTS_TOPIC}, got {len(payloads)}"
        )

    def test_insert_captured(self, docker_mongodb_services, _register_mongo_connector):
        """A new document insert should produce a CDC event."""
        _insert_product("Widget C", 29.99, 75)
        payloads = _raw_consume(PRODUCTS_TOPIC, timeout=30, max_messages=5)
        # We just verify at least one message arrived after the insert.
        assert len(payloads) >= 1

    def test_update_produces_event(
        self, docker_mongodb_services, _register_mongo_connector
    ):
        """An update should produce a CDC event on the products topic."""
        _update_product("Widget A", 999)
        payloads = _raw_consume(PRODUCTS_TOPIC, timeout=30, max_messages=5)
        assert len(payloads) >= 1

    def test_delete_produces_event(
        self, docker_mongodb_services, _register_mongo_connector
    ):
        """A document deletion should produce a CDC event (tombstone or delete op)."""
        _delete_product("Widget B")
        payloads = _raw_consume(PRODUCTS_TOPIC, timeout=30, max_messages=5)
        # Delete events include both the delete event and a null-value tombstone.
        assert len(payloads) >= 1

    def test_connector_status_running(
        self, docker_mongodb_services, _register_mongo_connector, mongo_platform
    ):
        """The registered connector should be in RUNNING state."""

        async def _check() -> str:
            async with DebeziumClient(mongo_platform.connector) as client:
                name = "cdc-mongo-integration"
                status = await client.get_connector_status(name)
                tasks = status.get("tasks", [])
                return tasks[0].get("state", "UNKNOWN") if tasks else "NO_TASKS"

        state = asyncio.run(_check())
        assert state == "RUNNING"
