"""Health probes for platform components."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import httpx
import structlog
from confluent_kafka.admin import AdminClient

from cdc_platform.config.models import KafkaConfig, PlatformConfig, TransportMode
from cdc_platform.streaming.auth import build_kafka_auth_config

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


def check_kafka(
    bootstrap_servers: str, kafka_config: KafkaConfig | None = None
) -> ComponentHealth:
    """Probe Kafka broker connectivity."""
    try:
        admin_conf: dict[str, Any] = {"bootstrap.servers": bootstrap_servers}
        if kafka_config is not None:
            admin_conf.update(build_kafka_auth_config(kafka_config))
        admin = AdminClient(admin_conf)
        meta = admin.list_topics(timeout=5)
        return ComponentHealth(
            name="kafka",
            status=Status.HEALTHY,
            detail=f"{len(meta.brokers)} broker(s)",
        )
    except Exception as exc:
        return ComponentHealth(name="kafka", status=Status.UNHEALTHY, detail=str(exc))


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


def check_pubsub(project_id: str) -> ComponentHealth:
    """Probe Google Pub/Sub connectivity."""
    try:
        from google.cloud import pubsub_v1

        publisher = pubsub_v1.PublisherClient()
        project_path = f"projects/{project_id}"
        topics = list(publisher.list_topics(request={"project": project_path}))
        return ComponentHealth(
            name="pubsub",
            status=Status.HEALTHY,
            detail=f"{len(topics)} topic(s) in {project_id}",
        )
    except Exception as exc:
        return ComponentHealth(name="pubsub", status=Status.UNHEALTHY, detail=str(exc))


def check_kinesis(region: str) -> ComponentHealth:
    """Probe Amazon Kinesis connectivity."""
    try:
        import boto3

        client = boto3.client("kinesis", region_name=region)
        resp = client.list_streams()
        streams = resp.get("StreamNames", [])
        return ComponentHealth(
            name="kinesis",
            status=Status.HEALTHY,
            detail=f"{len(streams)} stream(s) in {region}",
        )
    except Exception as exc:
        return ComponentHealth(name="kinesis", status=Status.UNHEALTHY, detail=str(exc))


def check_platform_health(platform: PlatformConfig | None = None) -> PlatformHealth:
    """Run all health checks and return aggregated result."""
    cfg = platform or PlatformConfig()
    components: list[ComponentHealth] = []

    if cfg.transport_mode == TransportMode.KAFKA and cfg.kafka and cfg.connector:
        components.append(check_kafka(cfg.kafka.bootstrap_servers, cfg.kafka))
        components.append(check_schema_registry(cfg.kafka.schema_registry_url))
        components.append(check_connect(cfg.connector.connect_url))
    elif cfg.transport_mode == TransportMode.PUBSUB and cfg.pubsub:
        components.append(check_pubsub(cfg.pubsub.project_id))
    elif cfg.transport_mode == TransportMode.KINESIS and cfg.kinesis:
        components.append(check_kinesis(cfg.kinesis.region))
    else:
        components.append(
            ComponentHealth(
                name="transport",
                status=Status.UNKNOWN,
                detail=f"no health checks for transport mode: {cfg.transport_mode}",
            )
        )

    return PlatformHealth(components=components)
