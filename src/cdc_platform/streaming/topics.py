"""Topic naming conventions and Kafka admin utilities."""

from __future__ import annotations

import structlog
from confluent_kafka.admin import AdminClient, NewTopic

from cdc_platform.config.models import PipelineConfig, PlatformConfig

logger = structlog.get_logger()


def cdc_topic_name(prefix: str, schema: str, table: str) -> str:
    """Build a CDC topic name: ``<prefix>.<schema>.<table>``."""
    return f"{prefix}.{schema}.{table}"


def dlq_topic_name(source_topic: str, suffix: str = "dlq") -> str:
    """Build a DLQ topic name: ``<source_topic>.<suffix>``."""
    return f"{source_topic}.{suffix}"


def topics_for_pipeline(
    pipeline: PipelineConfig, platform: PlatformConfig
) -> list[str]:
    """Return all CDC + DLQ topics for a pipeline."""
    topics: list[str] = []
    for table in pipeline.source.tables:
        schema, tbl = table.split(".")
        t = cdc_topic_name(pipeline.topic_prefix, schema, tbl)
        topics.append(t)
        if platform.dlq.enabled:
            topics.append(dlq_topic_name(t, platform.dlq.topic_suffix))
    return topics


def ensure_topics(
    bootstrap_servers: str,
    topics: list[str],
    *,
    num_partitions: int = 1,
    replication_factor: int = 1,
) -> None:
    """Create topics if they don't already exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [
        NewTopic(t, num_partitions=num_partitions, replication_factor=replication_factor)
        for t in topics
        if t not in existing
    ]
    if not to_create:
        logger.info("topics.all_exist", count=len(topics))
        return
    futures = admin.create_topics(to_create)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("topic.created", topic=topic)
        except Exception as exc:
            logger.warning("topic.create_failed", topic=topic, error=str(exc))
