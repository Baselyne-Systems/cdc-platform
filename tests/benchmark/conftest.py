from collections.abc import Generator
from typing import Any

import pytest
import structlog
from confluent_kafka.admin import AdminClient

from cdc_platform.config.models import PlatformConfig
from cdc_platform.sources.debezium.client import DebeziumClient

from .helpers import BenchmarkReport, BenchmarkResult

logger = structlog.get_logger()

# Global list to accumulate results across all tests
SESSION_RESULTS: list[BenchmarkResult] = []


def pytest_configure(config: pytest.Config) -> None:
    """Configure structlog for benchmarks."""
    processors: list[Any] = [
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    if not config.getoption("-s"):
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )


def pytest_sessionfinish(
    session: pytest.Session, exitstatus: int | pytest.ExitCode
) -> None:
    """Print consolidated benchmark report at end of session."""
    if not SESSION_RESULTS:
        return

    report = BenchmarkReport()
    report.results = SESSION_RESULTS
    print("\n")
    report.print_summary()

    import os

    # Force exit to avoid segfaults in C-extension teardown (fastavro/confluent-kafka)
    os._exit(int(exitstatus))


@pytest.fixture(scope="session")
def docker_services() -> Generator[None, None, None]:
    """Manage Docker Compose lifecycle for benchmarks."""
    import os
    import subprocess

    if os.environ.get("BENCHMARK_SKIP_DOCKER"):
        logger.info("benchmark.docker.skipped_lifecycle")
        yield
        return

    logger.info("benchmark.docker.starting")

    # Start up
    subprocess.run(
        ["docker", "compose", "-f", "docker/docker-compose.yml", "up", "-d", "--wait"],
        check=True,
    )
    logger.info("benchmark.docker.ready")

    yield

    # Tear down
    logger.info("benchmark.docker.teardown")
    subprocess.run(
        ["docker", "compose", "-f", "docker/docker-compose.yml", "down", "-v"],
        check=True,
    )


@pytest.fixture(scope="session")
def kafka_admin(docker_services: Any) -> Generator[AdminClient, None, None]:
    """Return a Kafka AdminClient."""
    yield AdminClient({"bootstrap.servers": "localhost:9092"})


@pytest.fixture(scope="session")
def pg_dsn(docker_services: None) -> str:
    """Return PostgreSQL DSN."""
    return "postgresql://cdc_user:cdc_password@localhost:5432/cdc_demo"


@pytest.fixture(scope="session")
def platform_config() -> PlatformConfig:
    """Return PlatformConfig customized for Docker environment."""
    from cdc_platform.config.models import ConnectorConfig, KafkaConfig

    return PlatformConfig(
        kafka=KafkaConfig(
            bootstrap_servers="localhost:9092",
            schema_registry_url="http://localhost:8081",
        ),
        connector=ConnectorConfig(
            connect_url="http://localhost:8083",
        ),
    )


@pytest.fixture(scope="session")
async def _register_connector(
    docker_services: Any, platform_config: PlatformConfig, pg_dsn: str
) -> str:
    """Register Debezium connector and wait for it to be running."""
    logger.info("benchmark.connector.registering")

    # We need to use the DebeziumClient to register the connector
    # But strictly speaking, the test description says:
    # "_register_connector fixture... waits for RUNNING task state"

    # Since we are running outside the app context, we can construct the config manually
    # or verify with a helper. The source DB is accessible as localhost:5432 from host,
    # but "postgres" from inside docker network.
    # Debezium runs inside Docker, so it needs to talk to "postgres" hostname.

    connector_name = "cdc_demo"
    client = DebeziumClient(platform_config.connector)

    # Create config dict
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "cdc_user",
        "database.password": "cdc_password",
        "database.dbname": "cdc_demo",
        "topic.prefix": "cdc",
        "plugin.name": "pgoutput",
        "slot.name": "cdc_slot_bench",  # Different slot than default to be safe
        "snapshot.mode": "initial",
    }

    import asyncio

    async def register() -> None:
        # Check if exists, delete if so
        try:
            await client.delete_connector(connector_name)
            await asyncio.sleep(2)  # Grace period
        except Exception:
            pass

        # DebeziumClient doesn't expose a raw create method, so we access the underlying client
        # or use the register_connector with a dummy pipeline.
        # Here we use the underlying client to PUT the config directly.
        await client._client.put(
            f"/connectors/{connector_name}/config",
            json=config,
        )

        # Wait for running
        for _ in range(60):
            try:
                status = await client.get_connector_status(connector_name)
                if (
                    status.get("connector", {}).get("state") == "RUNNING"
                    and len(status.get("tasks", [])) > 0
                    and status["tasks"][0].get("state") == "RUNNING"
                ):
                    logger.info("benchmark.connector.running")
                    return
            except Exception:
                pass
            await asyncio.sleep(1)
        raise RuntimeError("Connector failed to start")

    await register()
    return connector_name


@pytest.fixture
def benchmark_report(
    request: pytest.FixtureRequest,
) -> Generator[BenchmarkReport, None, None]:
    """Yield a BenchmarkReport and print summary on teardown."""
    report = BenchmarkReport(test_name=request.node.name)
    yield report

    # Add results to session list
    SESSION_RESULTS.extend(report.results)

    # Print per-test summary
    report.print_summary()
