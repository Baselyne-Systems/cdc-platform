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
    dispatch = {
        SourceType.MYSQL: build_mysql_connector_config,
        SourceType.MONGODB: build_mongodb_connector_config,
        SourceType.SQLSERVER: build_sqlserver_connector_config,
    }
    builder = dispatch.get(pipeline.source.source_type)
    if builder is not None:
        return builder(pipeline, platform)
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
        "database.server.id": str(src.mysql_server_id),
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


def build_mongodb_connector_config(
    pipeline: PipelineConfig, platform: PlatformConfig
) -> dict[str, Any]:
    """Return the Debezium MongoDB connector configuration dict.

    Uses the Debezium 2.x ``MongoDbConnector`` which requires a MongoDB
    connection string.  Credentials are embedded in the URI so that the
    connector can authenticate without a separate JAAS config.

    Prerequisites on the MongoDB side:
    - The source must be a replica set or sharded cluster (change streams
      require oplog; standalone nodes are not supported).
    - The CDC user needs ``read`` on the watched databases plus
      ``readAnyDatabase`` / ``changeStream`` privileges.
    """
    src = pipeline.source

    # Build the connection string.
    # replica_set_name is appended as a query parameter when provided so the
    # driver discovers all replica-set members automatically.
    rs_part = (
        f"?replicaSet={src.replica_set_name}&authSource={src.auth_source}"
        if src.replica_set_name
        else f"?authSource={src.auth_source}"
    )
    connection_string = (
        f"mongodb://{src.username}:{src.password.get_secret_value()}"
        f"@{src.host}:{src.port}/{rs_part}"
    )

    return {
        "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
        "mongodb.connection.string": connection_string,
        "topic.prefix": pipeline.topic_prefix,
        # MongoDB uses collection.include.list (format: <db>.<collection>)
        "collection.include.list": ",".join(src.tables) if src.tables else "",
        "snapshot.mode": src.snapshot_mode.value,
        # Emit full replacement document on updates (not just the diff)
        "capture.mode": "change_streams_update_full",
        # Converters — Avro with Schema Registry
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": platform.kafka.schema_registry_url,
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": platform.kafka.schema_registry_url,
        # Misc
        "tombstones.on.delete": "true",
    }


def build_sqlserver_connector_config(
    pipeline: PipelineConfig, platform: PlatformConfig
) -> dict[str, Any]:
    """Return the Debezium SQL Server connector configuration dict.

    Prerequisites on the SQL Server side:
    - SQL Server CDC must be enabled on the database:
        ``EXEC sys.sp_cdc_enable_db``
    - CDC must be enabled on each captured table:
        ``EXEC sys.sp_cdc_enable_table @source_schema, @source_name, @role_name``
    - The CDC user needs ``db_datareader`` and ``EXECUTE`` on the CDC
      stored procedures, plus access to the cdc schema.
    - The SQL Server Agent must be running (manages the capture / cleanup jobs).
    """
    src = pipeline.source

    return {
        "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
        "database.hostname": src.host,
        "database.port": str(src.port),
        "database.user": src.username,
        "database.password": src.password.get_secret_value(),
        # Debezium 2.x uses database.names (supports multiple databases)
        "database.names": src.database,
        "topic.prefix": pipeline.topic_prefix,
        "table.include.list": ",".join(src.tables) if src.tables else "",
        "snapshot.mode": src.snapshot_mode.value,
        # Schema history — required for DDL tracking
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
