"""Integration tests: SQL Server INSERT/UPDATE/DELETE → Debezium → Kafka → assert.

Run with:
    pytest tests/integration/test_sqlserver_cdc.py -m integration -v

Prerequisites:
    docker compose -f docker/docker-compose.yml \\
                   -f docker/docker-compose.sqlserver.yml up -d

SQL Server CDC requires:
  - SQL Server Developer or Enterprise edition (Express does not support Agent)
  - CDC enabled on the database via sys.sp_cdc_enable_db
  - CDC enabled per table via sys.sp_cdc_enable_table
  - SQL Server Agent running (manages CDC capture / cleanup jobs)
"""

from __future__ import annotations

import asyncio
import subprocess
import time
from typing import Any

import pytest
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

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

# ── Connection constants ──────────────────────────────────────────────────────

_SA_PASSWORD = "Cdc_Password1!"
_CDC_USER = "sa"
_SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"

# ── Docker helpers ────────────────────────────────────────────────────────────

_BASE_COMPOSE = "docker/docker-compose.yml"
_SQLSERVER_COMPOSE = "docker/docker-compose.sqlserver.yml"


def _compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", "-f", _BASE_COMPOSE, "-f", _SQLSERVER_COMPOSE, *args],
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


def _wait_for_sqlserver(*, timeout: int = 120) -> None:
    """Wait until the sqlcmd healthcheck passes from inside a Docker container."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            result = subprocess.run(
                [
                    "docker", "exec", "cdc-sqlserver",
                    _SQLCMD,
                    "-S", "localhost", "-U", _CDC_USER, "-P", _SA_PASSWORD,
                    "-Q", "SELECT 1", "-No", "-b",
                ],
                capture_output=True,
                timeout=15,
            )
            if result.returncode == 0:
                return
        except Exception:
            pass
        time.sleep(3)
    raise TimeoutError(f"SQL Server not ready after {timeout}s")


def _wait_for_sqlserver_cdc_enabled(*, timeout: int = 60) -> None:
    """Wait until the sqlserver-init container has run init.sql (CDC enabled)."""
    deadline = time.time() + timeout
    query = (
        "SELECT is_cdc_enabled FROM sys.databases "
        "WHERE name = 'cdc_demo' AND is_cdc_enabled = 1"
    )
    while time.time() < deadline:
        try:
            result = subprocess.run(
                [
                    "docker", "exec", "cdc-sqlserver",
                    _SQLCMD,
                    "-S", "localhost", "-U", _CDC_USER, "-P", _SA_PASSWORD,
                    "-Q", query, "-No", "-b", "-h", "-1",
                ],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if result.returncode == 0 and "1" in result.stdout:
                return
        except Exception:
            pass
        time.sleep(3)
    raise TimeoutError(f"CDC not enabled on cdc_demo after {timeout}s")


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def docker_sqlserver_services():
    """Bring up the base stack + SQL Server; tear down after the session."""
    _compose("up", "-d")
    try:
        _wait_for_kafka()
        _wait_for_http("http://localhost:8081/subjects")
        _wait_for_http("http://localhost:8083/connectors")
        _wait_for_sqlserver()
        _wait_for_sqlserver_cdc_enabled()
        yield
    finally:
        _compose("down", "-v")


@pytest.fixture(scope="session")
def sqlserver_platform() -> PlatformConfig:
    return PlatformConfig(
        kafka=KafkaConfig(schema_registry_url="http://schema-registry:8081"),
    )


@pytest.fixture(scope="session")
def sqlserver_pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="sqlserver-integration",
        topic_prefix="cdc",
        source=SourceConfig(
            source_type=SourceType.SQLSERVER,
            host="sqlserver",
            port=1433,
            database="cdc_demo",
            username=_CDC_USER,
            password=_SA_PASSWORD,
            tables=["dbo.customers"],
            snapshot_mode=SnapshotMode.INITIAL,
        ),
    )


@pytest.fixture(scope="session")
def _register_sqlserver_connector(
    docker_sqlserver_services, sqlserver_pipeline, sqlserver_platform
):
    """Register the Debezium SQL Server connector; wait until RUNNING."""

    async def _register() -> None:
        async with DebeziumClient(sqlserver_platform.connector) as client:
            await client.wait_until_ready()
            await client.register_connector(sqlserver_pipeline, sqlserver_platform)
            name = connector_name(sqlserver_pipeline)
            for _ in range(40):
                try:
                    status = await client.get_connector_status(name)
                except Exception:
                    await asyncio.sleep(3)
                    continue
                tasks = status.get("tasks", [])
                if tasks and tasks[0].get("state") == "RUNNING":
                    return
                if tasks and tasks[0].get("state") == "FAILED":
                    trace = tasks[0].get("trace", "")
                    raise RuntimeError(f"Connector FAILED:\n{trace[:500]}")
                await asyncio.sleep(3)
            raise TimeoutError("SQL Server connector did not reach RUNNING state")

    asyncio.run(_register())


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_avro_consumer(topic: str) -> tuple[Consumer, AvroDeserializer]:
    sr_client = SchemaRegistryClient({"url": "http://localhost:8081"})
    deserializer = AvroDeserializer(sr_client)
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"test-sqlserver-{time.time()}",
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
    consumer, deserializer = _make_avro_consumer(topic)
    messages: list[dict[str, Any]] = []
    deadline = time.time() + timeout
    try:
        while time.time() < deadline and len(messages) < max_messages:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                code = msg.error().code()
                if code in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    continue
                raise Exception(msg.error())
            raw = msg.value()
            if raw is not None:
                ctx = SerializationContext(topic, MessageField.VALUE)
                value = deserializer(raw, ctx)
                if value:
                    messages.append(value)
    finally:
        consumer.close()
    return messages


def _sqlcmd(query: str) -> None:
    """Execute a T-SQL statement against cdc_demo via sqlcmd inside Docker."""
    subprocess.run(
        [
            "docker", "exec", "cdc-sqlserver",
            _SQLCMD,
            "-S", "localhost",
            "-U", _CDC_USER,
            "-P", _SA_PASSWORD,
            "-d", "cdc_demo",
            "-Q", query,
            "-No", "-b",
        ],
        check=True,
        capture_output=True,
    )


# ── Tests ─────────────────────────────────────────────────────────────────────
# SQL Server Debezium topic format: <prefix>.<database>.<schema>.<table>
CUSTOMERS_TOPIC = "cdc.cdc_demo.dbo.customers"


@pytest.mark.integration
class TestSQLServerCDC:
    def test_snapshot_produces_existing_rows(
        self, docker_sqlserver_services, _register_sqlserver_connector
    ):
        """The initial snapshot should emit events for the 2 seed rows."""
        messages = _consume_messages(CUSTOMERS_TOPIC, timeout=60, max_messages=10)
        assert len(messages) >= 2, (
            f"Expected ≥2 snapshot messages on {CUSTOMERS_TOPIC}, "
            f"got {len(messages)}"
        )
        emails = {
            m.get("after", {}).get("email")
            for m in messages
            if isinstance(m.get("after"), dict)
        }
        assert "alice@example.com" in emails

    def test_insert_captured(
        self, docker_sqlserver_services, _register_sqlserver_connector
    ):
        """A live INSERT should produce a CDC event with op='c' (create)."""
        _sqlcmd(
            "INSERT INTO dbo.customers (email, full_name) "
            "VALUES ('charlie@example.com', 'Charlie Brown')"
        )
        messages = _consume_messages(CUSTOMERS_TOPIC, timeout=30, max_messages=5)
        emails = {
            m.get("after", {}).get("email")
            for m in messages
            if isinstance(m.get("after"), dict)
        }
        assert "charlie@example.com" in emails

    def test_update_captured(
        self, docker_sqlserver_services, _register_sqlserver_connector
    ):
        """An UPDATE should produce a CDC event containing before and after."""
        _sqlcmd(
            "UPDATE dbo.customers SET full_name = 'Alice Updated' "
            "WHERE email = 'alice@example.com'"
        )
        messages = _consume_messages(CUSTOMERS_TOPIC, timeout=30, max_messages=5)
        updates = [m for m in messages if m.get("op") == "u"]
        assert len(updates) >= 1

    def test_delete_produces_event(
        self, docker_sqlserver_services, _register_sqlserver_connector
    ):
        """A DELETE should produce a CDC event with op='d' (delete)."""
        _sqlcmd(
            "DELETE FROM dbo.customers WHERE email = 'charlie@example.com'"
        )
        messages = _consume_messages(CUSTOMERS_TOPIC, timeout=30, max_messages=5)
        deletes = [m for m in messages if m.get("op") == "d"]
        assert len(deletes) >= 1

    def test_connector_status_running(
        self,
        docker_sqlserver_services,
        _register_sqlserver_connector,
        sqlserver_platform,
        sqlserver_pipeline,
    ):
        """The registered connector should be in RUNNING state."""

        async def _check() -> str:
            async with DebeziumClient(sqlserver_platform.connector) as client:
                name = connector_name(sqlserver_pipeline)
                status = await client.get_connector_status(name)
                tasks = status.get("tasks", [])
                return tasks[0].get("state", "UNKNOWN") if tasks else "NO_TASKS"

        state = asyncio.run(_check())
        assert state == "RUNNING"
