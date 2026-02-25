"""Build Debezium connector JSON config from pipeline + platform configs."""

from __future__ import annotations

from typing import Any

from cdc_platform.config.models import (
    PipelineConfig,
    PlatformConfig,
    SourceType,
)


def connector_name(pipeline: PipelineConfig) -> str:
    """Derive the Kafka Connect connector name from a pipeline."""
    return f"{pipeline.topic_prefix}-{pipeline.pipeline_id}"


def build_connector_config(
    pipeline: PipelineConfig, platform: PlatformConfig
) -> dict[str, Any]:
    """Return the Debezium connector config for the pipeline's source type."""
    if pipeline.source.source_type == SourceType.MYSQL:
        return build_mysql_connector_config(pipeline, platform)
    return build_postgres_connector_config(pipeline, platform)


def build_postgres_connector_config(
    pipeline: PipelineConfig, platform: PlatformConfig
) -> dict[str, Any]:
    """Return the Debezium Postgres connector configuration dict."""
    src = pipeline.source
    name = connector_name(pipeline)

    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": src.host,
        "database.port": str(src.port),
        "database.user": src.username,
        "database.password": src.password.get_secret_value(),
        "database.dbname": src.database,
        "topic.prefix": pipeline.topic_prefix,
        "plugin.name": "pgoutput",
        "publication.name": src.publication_name,
        "slot.name": src.slot_name,
        "snapshot.mode": src.snapshot_mode.value,
        "table.include.list": ",".join(src.tables) if src.tables else "",
        # Converters — Avro with Schema Registry
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": platform.kafka.schema_registry_url,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": platform.kafka.schema_registry_url,
        # Heartbeat
        "heartbeat.interval.ms": "10000",
        "heartbeat.action.query": (
            f"INSERT INTO heartbeat (connector, ts) VALUES ('{name}', now()) "
            f"ON CONFLICT (connector) DO UPDATE SET ts = now()"
        ),
        # Misc
        "decimal.handling.mode": "string",
        "tombstones.on.delete": "true",
    }


def build_mysql_connector_config(
    pipeline: PipelineConfig, platform: PlatformConfig
) -> dict[str, Any]:
    """Return the Debezium MySQL connector configuration dict."""
    src = pipeline.source

    return {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": src.host,
        "database.port": str(src.port),
        "database.user": src.username,
        "database.password": src.password.get_secret_value(),
        "database.server.id": "1",
        "topic.prefix": pipeline.topic_prefix,
        "database.include.list": src.database,
        "table.include.list": ",".join(src.tables) if src.tables else "",
        "snapshot.mode": src.snapshot_mode.value,
        "schema.history.internal.kafka.bootstrap.servers": (
            platform.kafka.bootstrap_servers
        ),
        "schema.history.internal.kafka.topic": (
            f"_schema-history.{pipeline.topic_prefix}.{pipeline.pipeline_id}"
        ),
        # Converters — Avro with Schema Registry
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": platform.kafka.schema_registry_url,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": platform.kafka.schema_registry_url,
        # Misc
        "decimal.handling.mode": "string",
        "tombstones.on.delete": "true",
        "include.schema.changes": "false",
    }
