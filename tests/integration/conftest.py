"""Docker Compose fixtures for integration tests."""

from __future__ import annotations

import subprocess
import time

import httpx
import pytest
from confluent_kafka.admin import AdminClient

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
def pipeline():
    """Shared pipeline config for integration tests."""
    from cdc_platform.config.models import PipelineConfig, SourceConfig

    return PipelineConfig(
        pipeline_id="integration-test",
        source=SourceConfig(
            database="cdc_demo",
            password="cdc_password",
            tables=["public.customers"],
        ),
    )


@pytest.fixture(scope="session")
def _register_connector(docker_services, pipeline):
    """Register the Debezium connector once for the entire session."""
    import asyncio

    from cdc_platform.sources.debezium.client import DebeziumClient
    from cdc_platform.sources.debezium.config import connector_name

    async def _register():
        async with DebeziumClient(pipeline.connector) as client:
            await client.wait_until_ready()
            await client.register_connector(pipeline)
            name = connector_name(pipeline)
            for _ in range(30):
                status = await client.get_connector_status(name)
                if status.get("connector", {}).get("state") == "RUNNING":
                    return
                await asyncio.sleep(2)
            raise TimeoutError("Connector did not reach RUNNING state")

    asyncio.run(_register())
