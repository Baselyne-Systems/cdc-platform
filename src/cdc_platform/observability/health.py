"""Health probes for platform components."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

import httpx
import structlog
from confluent_kafka.admin import AdminClient

logger = structlog.get_logger()


class Status(StrEnum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class ComponentHealth:
    name: str
    status: Status = Status.UNKNOWN
    detail: str = ""


@dataclass
class PlatformHealth:
    components: list[ComponentHealth] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return all(c.status == Status.HEALTHY for c in self.components)

    @property
    def summary(self) -> dict[str, str]:
        return {c.name: c.status.value for c in self.components}


def check_kafka(bootstrap_servers: str) -> ComponentHealth:
    """Probe Kafka broker connectivity."""
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        meta = admin.list_topics(timeout=5)
        return ComponentHealth(
            name="kafka",
            status=Status.HEALTHY,
            detail=f"{len(meta.brokers)} broker(s)",
        )
    except Exception as exc:
        return ComponentHealth(
            name="kafka", status=Status.UNHEALTHY, detail=str(exc)
        )


def check_schema_registry(url: str) -> ComponentHealth:
    """Probe Schema Registry HTTP endpoint."""
    try:
        resp = httpx.get(f"{url}/subjects", timeout=5)
        resp.raise_for_status()
        return ComponentHealth(
            name="schema-registry",
            status=Status.HEALTHY,
            detail=f"{len(resp.json())} subject(s)",
        )
    except Exception as exc:
        return ComponentHealth(
            name="schema-registry", status=Status.UNHEALTHY, detail=str(exc)
        )


def check_connect(url: str) -> ComponentHealth:
    """Probe Kafka Connect REST API."""
    try:
        resp = httpx.get(f"{url}/connectors", timeout=5)
        resp.raise_for_status()
        connectors = resp.json()
        return ComponentHealth(
            name="kafka-connect",
            status=Status.HEALTHY,
            detail=f"{len(connectors)} connector(s)",
        )
    except Exception as exc:
        return ComponentHealth(
            name="kafka-connect", status=Status.UNHEALTHY, detail=str(exc)
        )


def check_platform_health(
    bootstrap_servers: str = "localhost:9092",
    schema_registry_url: str = "http://localhost:8081",
    connect_url: str = "http://localhost:8083",
) -> PlatformHealth:
    """Run all health checks and return aggregated result."""
    return PlatformHealth(
        components=[
            check_kafka(bootstrap_servers),
            check_schema_registry(schema_registry_url),
            check_connect(connect_url),
        ]
    )
