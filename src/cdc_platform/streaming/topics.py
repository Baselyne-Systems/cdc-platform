"""Topic naming conventions and Kafka admin utilities."""

from __future__ import annotations

import structlog
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from cdc_platform.config.models import PipelineConfig, PlatformConfig, SourceType

logger = structlog.get_logger()


def cdc_topic_name(prefix: str, schema: str, table: str) -> str:
    """Build a CDC topic name: ``<prefix>.<schema>.<table>``."""
    return f"{prefix}.{schema}.{table}"


def dlq_topic_name(source_topic: str, suffix: str = "dlq") -> str:
    """Build a DLQ topic name: ``<source_topic>.<suffix>``."""
    return f"{source_topic}.{suffix}"


def _cdc_topic_for_entry(pipeline: PipelineConfig, qualified_name: str) -> str:
    """Return the Debezium-generated topic name for one table/collection entry.

    Topic naming varies by source type because Debezium embeds the database
    name differently depending on the connector:

    - PostgreSQL  ``<prefix>.<schema>.<table>``     e.g. ``cdc.public.customers``
    - MySQL       ``<prefix>.<db>.<table>``          e.g. ``cdc.mydb.customers``
    - MongoDB     ``<prefix>.<db>.<collection>``     e.g. ``cdc.mydb.events``
    - SQL Server  ``<prefix>.<database>.<schema>.<table>``
                                                     e.g. ``cdc.cdc_demo.dbo.customers``

    PostgreSQL, MySQL, and MongoDB all use the 3-part form where the first
    component of *qualified_name* (``schema`` / ``db``) is already the right
    second segment.  SQL Server adds an extra level because Debezium 2.x
    prepends ``database.names`` before the schema+table.
    """
    namespace, name = qualified_name.split(".")
    src = pipeline.source

    if src.source_type == SourceType.SQLSERVER:
        # Debezium SqlServerConnector (v2.x): <prefix>.<database>.<schema>.<table>
        return f"{pipeline.topic_prefix}.{src.database}.{namespace}.{name}"

    # PostgreSQL / MySQL / MongoDB: <prefix>.<namespace>.<name>
    return cdc_topic_name(pipeline.topic_prefix, namespace, name)


def topics_for_pipeline(
    pipeline: PipelineConfig, platform: PlatformConfig
) -> list[str]:
    """Return all CDC + DLQ topics for a pipeline."""
    topics: list[str] = []
    for entry in pipeline.source.tables:
        t = _cdc_topic_for_entry(pipeline, entry)
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
        NewTopic(
            t, num_partitions=num_partitions, replication_factor=replication_factor
        )
        for t in topics
        if t not in existing
    ]
    if not to_create:
        logger.info("topics.all_exist", count=len(topics))
        return
    futures = admin.create_topics(to_create)
    failed: list[str] = []
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("topic.created", topic=topic)
        except Exception as exc:
            logger.error("topic.create_failed", topic=topic, error=str(exc))
            failed.append(f"{topic}: {exc}")
    if failed:
        msg = f"Failed to create {len(failed)} topic(s): {'; '.join(failed)}"
        raise RuntimeError(msg)
