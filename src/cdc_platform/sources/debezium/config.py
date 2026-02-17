"""Build Debezium connector JSON config from a PipelineConfig."""

from __future__ import annotations

from typing import Any

from cdc_platform.config.models import PipelineConfig


def connector_name(pipeline: PipelineConfig) -> str:
    """Derive the Kafka Connect connector name from a pipeline."""
    return f"{pipeline.topic_prefix}-{pipeline.pipeline_id}"


def build_postgres_connector_config(pipeline: PipelineConfig) -> dict[str, Any]:
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
        # Schema Registry / Avro
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": pipeline.kafka.schema_registry_url,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": pipeline.kafka.schema_registry_url,
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
