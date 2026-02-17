"""Docker Compose fixtures for integration tests."""

from __future__ import annotations

import asyncio
import subprocess
import time

import httpx
import pytest
from confluent_kafka.admin import AdminClient

from cdc_platform.config.models import (
    KafkaConfig,
    PipelineConfig,
    PlatformConfig,
    SourceConfig,
)
from cdc_platform.sources.debezium.client import DebeziumClient
from cdc_platform.sources.debezium.config import connector_name

COMPOSE_FILE = "docker/docker-compose.yml"


def _compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, *args],
        check=True,
        capture_output=True,
    )


def _wait_for_http(url: str, *, timeout: int = 120) -> None:
    """Poll an HTTP endpoint until it returns 2xx."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = httpx.get(url, timeout=5)
            if resp.status_code < 300:
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f"{url} not ready after {timeout}s")


def _wait_for_kafka(bootstrap: str = "localhost:9092", *, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap})
            admin.list_topics(timeout=5)
            return
        except Exception:
            time.sleep(2)
    raise TimeoutError(f"Kafka at {bootstrap} not ready after {timeout}s")


@pytest.fixture(scope="session")
def docker_services():
    """Start Docker Compose services and wait until healthy."""
    _compose("up", "-d")
    try:
        _wait_for_kafka()
        _wait_for_http("http://localhost:8081/subjects")
        _wait_for_http("http://localhost:8083/connectors")
        yield
    finally:
        _compose("down", "-v")


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    return "postgresql://cdc_user:cdc_password@localhost:5432/cdc_demo"


@pytest.fixture(scope="session")
def platform() -> PlatformConfig:
    """Shared platform config for integration tests.

    The Schema Registry URL uses the Docker-internal hostname because the
    connector config is sent to Kafka Connect which runs inside Docker.
    """
    return PlatformConfig(
        kafka=KafkaConfig(schema_registry_url="http://schema-registry:8081"),
    )


@pytest.fixture(scope="session")
def pipeline() -> PipelineConfig:
    """Shared pipeline config for integration tests."""
    return PipelineConfig(
        pipeline_id="integration-test",
        source=SourceConfig(
            host="postgres",
            database="cdc_demo",
            password="cdc_password",
            tables=["public.customers"],
        ),
    )


@pytest.fixture(scope="session")
def _register_connector(docker_services, pipeline, platform):
    """Register the Debezium connector once for the entire session."""

    async def _register():
        async with DebeziumClient(platform.connector) as client:
            await client.wait_until_ready()
            await client.register_connector(pipeline, platform)
            name = connector_name(pipeline)
            for _ in range(30):
                try:
                    status = await client.get_connector_status(name)
                except Exception:
                    # Connector may not be visible yet (404)
                    await asyncio.sleep(2)
                    continue
                tasks = status.get("tasks", [])
                if tasks and tasks[0].get("state") == "RUNNING":
                    return
                if tasks and tasks[0].get("state") == "FAILED":
                    trace = tasks[0].get("trace", "")
                    raise RuntimeError(
                        f"Connector task FAILED:\n{trace[:500]}"
                    )
                await asyncio.sleep(2)
            raise TimeoutError("Connector did not reach RUNNING state")

    asyncio.run(_register())
