"""Pydantic configuration models for CDC pipelines."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator


class TransportMode(StrEnum):
    """Supported event transport modes."""

    KAFKA = "kafka"
    PUBSUB = "pubsub"
    KINESIS = "kinesis"


class KafkaAuthMechanism(StrEnum):
    """Kafka SASL authentication mechanisms."""

    NONE = "none"
    SASL_PLAIN = "sasl_plain"
    SASL_SCRAM_256 = "sasl_scram_256"
    SASL_SCRAM_512 = "sasl_scram_512"
    SASL_IAM = "sasl_iam"
    SASL_OAUTHBEARER = "sasl_oauthbearer"


class SourceType(StrEnum):
    """Supported CDC source types."""

    POSTGRES = "postgres"
    MYSQL = "mysql"
    MONGODB = "mongodb"
    SQLSERVER = "sqlserver"


class SnapshotMode(StrEnum):
    """Debezium snapshot modes."""

    INITIAL = "initial"
    NEVER = "never"
    WHEN_NEEDED = "when_needed"
    NO_DATA = "no_data"


class SourceConfig(BaseModel):
    """Configuration for a CDC source database.

    Common fields apply to all source types.  Source-specific fields are
    optional and ignored when they don't apply to the selected source_type:

    - PostgreSQL: slot_name, publication_name
    - MySQL:      mysql_server_id
    - MongoDB:    replica_set_name, auth_source
    - SQL Server: (uses common fields only; CDC must be pre-enabled on the DB)
    """

    source_type: SourceType = SourceType.POSTGRES
    host: str = "localhost"
    # Default port is for PostgreSQL; set explicitly for other source types:
    # MySQL → 3306, MongoDB → 27017, SQL Server → 1433
    port: int = 5432
    database: str
    username: str = "cdc_user"
    password: SecretStr = SecretStr("cdc_password")
    # For RDBMS sources: schema-qualified table names (e.g. "public.customers").
    # For MongoDB: database-qualified collection names (e.g. "mydb.events").
    tables: list[str] = Field(default_factory=list)
    snapshot_mode: SnapshotMode = SnapshotMode.INITIAL

    # -- PostgreSQL-specific ---------------------------------------------------
    slot_name: str = "cdc_slot"
    publication_name: str = "cdc_publication"

    # -- MySQL-specific --------------------------------------------------------
    # Must be unique across all MySQL servers in the replication topology.
    mysql_server_id: int = Field(default=1, ge=1)

    # -- MongoDB-specific ------------------------------------------------------
    # Replica set name (e.g. "rs0").  Leave None for standalone / Atlas clusters
    # that embed the replica set in the connection string.
    replica_set_name: str | None = None
    # MongoDB database used to authenticate the CDC user (default: "admin").
    auth_source: str = "admin"

    @field_validator("tables")
    @classmethod
    def validate_qualified_names(cls, v: list[str]) -> list[str]:
        """Validate that table/collection names are schema- or db-qualified.

        Accepted format: ``<namespace>.<name>`` where both parts start with a
        letter or underscore and contain only word characters.  This covers
        ``public.customers`` (RDBMS) and ``mydb.events`` (MongoDB) alike.
        """
        pattern = re.compile(r"^[a-zA-Z_]\w*\.[a-zA-Z_]\w*$")
        for table in v:
            if not pattern.match(table):
                msg = (
                    f"Table/collection '{table}' must be schema- or "
                    f"db-qualified (e.g. 'public.customers' or 'mydb.events')"
                )
                raise ValueError(msg)
        return v


class KafkaConfig(BaseModel):
    """Kafka broker and producer/consumer settings."""

    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    group_id: str = "cdc-platform"
    auto_offset_reset: str = "earliest"
    enable_idempotence: bool = True
    acks: str = "all"
    topic_num_partitions: int = Field(default=1, ge=1)
    topic_replication_factor: int = Field(default=1, ge=1)
    # Consumer tuning
    session_timeout_ms: int = Field(default=45000, ge=1000)
    max_poll_interval_ms: int = Field(default=300000, ge=1000)
    fetch_min_bytes: int = Field(default=1, ge=1)
    fetch_max_wait_ms: int = Field(default=500, ge=0)
    # High-throughput tuning
    poll_batch_size: int = Field(default=1, ge=1)
    deser_pool_size: int = Field(default=1, ge=1)
    commit_interval_seconds: float = Field(default=0.0, ge=0.0)
    # Auth / security
    security_protocol: str = "PLAINTEXT"
    auth_mechanism: KafkaAuthMechanism = KafkaAuthMechanism.NONE
    sasl_username: str | None = None
    sasl_password: SecretStr | None = None
    ssl_ca_location: str | None = None
    ssl_certificate_location: str | None = None
    ssl_key_location: str | None = None
    aws_region: str | None = None
    gcp_project_id: str | None = None

    @model_validator(mode="after")
    def check_auth_requirements(self) -> Self:
        """Validate that auth-specific fields are present."""
        mech = self.auth_mechanism
        if mech == KafkaAuthMechanism.SASL_IAM and not self.aws_region:
            msg = "aws_region is required when auth_mechanism is 'sasl_iam'"
            raise ValueError(msg)
        if mech in (
            KafkaAuthMechanism.SASL_PLAIN,
            KafkaAuthMechanism.SASL_SCRAM_256,
            KafkaAuthMechanism.SASL_SCRAM_512,
        ) and (not self.sasl_username or not self.sasl_password):
            msg = (
                "sasl_username and sasl_password are required "
                f"when auth_mechanism is '{mech.value}'"
            )
            raise ValueError(msg)
        return self


class WalReaderConfig(BaseModel):
    """Direct WAL reader configuration for non-Kafka transports."""

    publication_name: str = "cdc_publication"
    slot_name: str = "cdc_slot"
    status_interval_seconds: float = Field(default=10.0, gt=0)
    batch_size: int = Field(default=100, ge=1)
    batch_timeout_seconds: float = Field(default=1.0, gt=0)


class PubSubConfig(BaseModel):
    """Google Cloud Pub/Sub transport configuration."""

    project_id: str
    ordering_enabled: bool = True
    ack_deadline_seconds: int = Field(default=600, ge=10, le=600)
    max_messages_per_pull: int = Field(default=100, ge=1)
    group_id: str = "cdc-platform"
    max_outstanding_messages: int = Field(default=1000, ge=1)
    max_delivery_attempts: int = Field(default=5, ge=1)


class KinesisConfig(BaseModel):
    """Amazon Kinesis Data Streams transport configuration."""

    region: str = "us-east-1"
    shard_count: int = Field(default=1, ge=1)
    group_id: str = "cdc-platform"
    iterator_type: str = "TRIM_HORIZON"
    checkpoint_table_name: str = "cdc-kinesis-checkpoints"
    poll_interval_seconds: float = Field(default=1.0, gt=0)
    max_records_per_shard: int = Field(default=100, ge=1)
    dlq_stream_suffix: str = "dlq"


class ConnectorConfig(BaseModel):
    """Kafka Connect (Debezium) REST API settings."""

    connect_url: str = "http://localhost:8083"
    timeout_seconds: float = Field(default=30.0, gt=0)
    retry_max_attempts: int = Field(default=5, ge=1)
    retry_wait_seconds: float = Field(default=2.0, gt=0)


class DLQConfig(BaseModel):
    """Dead Letter Queue settings."""

    enabled: bool = True
    topic_suffix: str = Field(default="dlq", min_length=1)
    max_retries: int = Field(default=3, ge=0)
    include_headers: bool = True
    flush_interval_seconds: float = Field(default=0.0, ge=0.0)


class RetryConfig(BaseModel):
    """Retry / backoff configuration."""

    max_attempts: int = Field(default=5, ge=1)
    initial_wait_seconds: float = Field(default=1.0, gt=0)
    max_wait_seconds: float = Field(default=60.0, gt=0)
    multiplier: float = Field(default=2.0, ge=1.0)
    jitter: bool = True


TopicPrefix = Annotated[str, Field(pattern=r"^[a-zA-Z][a-zA-Z0-9._-]*$")]


class SinkType(StrEnum):
    """Supported sink types."""

    WEBHOOK = "webhook"
    POSTGRES = "postgres"
    ICEBERG = "iceberg"


class WebhookSinkConfig(BaseModel):
    """Configuration for a webhook (HTTP POST) sink."""

    url: str
    method: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: float = 30.0
    auth_token: SecretStr | None = None


class PostgresSinkConfig(BaseModel):
    """Configuration for a PostgreSQL destination sink."""

    host: str = "localhost"
    port: int = 5432
    database: str
    username: str = "cdc_user"
    password: SecretStr = SecretStr("cdc_password")
    target_table: str
    batch_size: int = Field(default=100, ge=1)
    upsert: bool = False

    @field_validator("target_table")
    @classmethod
    def validate_schema_qualified_table(cls, v: str) -> str:
        pattern = re.compile(r"^[a-zA-Z_]\w*\.[a-zA-Z_]\w*$")
        if not pattern.match(v):
            msg = (
                f"target_table '{v}' must be schema-qualified "
                f"(e.g. 'public.cdc_events')"
            )
            raise ValueError(msg)
        return v


class TableMaintenanceConfig(BaseModel):
    """Configuration for Iceberg table maintenance (compaction + snapshot expiry)."""

    enabled: bool = False
    expire_snapshots_interval_seconds: float = 3600.0
    expire_snapshots_older_than_seconds: float = 86400.0
    compaction_interval_seconds: float = 7200.0
    compaction_file_threshold: int = 10
    compaction_max_rows_per_batch: int = 500_000


class IcebergSinkConfig(BaseModel):
    """Configuration for an Apache Iceberg lakehouse sink."""

    catalog_name: str = "default"
    catalog_uri: str
    warehouse: str
    table_namespace: str = "default"
    table_name: str
    write_mode: Literal["append", "upsert"] = "append"
    batch_size: int = Field(default=1000, ge=1)
    auto_create_table: bool = True
    partition_by: list[str] = Field(default_factory=list)
    s3_endpoint: str | None = None
    s3_access_key_id: str | None = None
    s3_secret_access_key: SecretStr | None = None
    s3_region: str = "us-east-1"
    maintenance: TableMaintenanceConfig = TableMaintenanceConfig()
    # High-throughput tuning
    flush_interval_seconds: float = Field(default=0.0, ge=0.0)
    write_executor_threads: int = Field(default=0, ge=0)


class SinkConfig(BaseModel):
    """Configuration for a single sink destination."""

    sink_id: str
    sink_type: SinkType
    enabled: bool = True
    retry: RetryConfig = RetryConfig()
    webhook: WebhookSinkConfig | None = None
    postgres: PostgresSinkConfig | None = None
    iceberg: IcebergSinkConfig | None = None

    @model_validator(mode="after")
    def check_matching_sub_config(self) -> Self:
        """Ensure the sub-config matching sink_type is provided."""
        if self.sink_type == SinkType.WEBHOOK and self.webhook is None:
            msg = "webhook config is required when sink_type is 'webhook'"
            raise ValueError(msg)
        if self.sink_type == SinkType.POSTGRES and self.postgres is None:
            msg = "postgres config is required when sink_type is 'postgres'"
            raise ValueError(msg)
        if self.sink_type == SinkType.ICEBERG and self.iceberg is None:
            msg = "iceberg config is required when sink_type is 'iceberg'"
            raise ValueError(msg)
        return self


class PipelineConfig(BaseModel, extra="forbid"):
    """Per-pipeline configuration — source database and sink destinations."""

    pipeline_id: str
    topic_prefix: TopicPrefix = "cdc"
    source: SourceConfig
    sinks: list[SinkConfig] = Field(default_factory=list)


class PlatformConfig(BaseModel):
    """Platform infrastructure configuration — Kafka, Debezium, DLQ, tuning."""

    transport_mode: TransportMode = TransportMode.KAFKA
    kafka: KafkaConfig | None = KafkaConfig()
    connector: ConnectorConfig | None = ConnectorConfig()
    pubsub: PubSubConfig | None = None
    kinesis: KinesisConfig | None = None
    wal_reader: WalReaderConfig | None = None
    dlq: DLQConfig = DLQConfig()
    retry: RetryConfig = RetryConfig()
    max_buffered_messages: int = Field(default=1000, ge=1)
    schema_monitor_interval_seconds: float = Field(default=30.0, gt=0)
    lag_monitor_interval_seconds: float = Field(default=15.0, gt=0)
    stop_on_incompatible_schema: bool = False
    health_port: int = 8080
    health_enabled: bool = True

    @model_validator(mode="after")
    def check_transport_requirements(self) -> Self:
        """Ensure transport-specific config is present."""
        if self.transport_mode == TransportMode.KAFKA:
            if self.kafka is None:
                msg = "kafka config is required when transport_mode is 'kafka'"
                raise ValueError(msg)
            if self.connector is None:
                msg = "connector config is required when transport_mode is 'kafka'"
                raise ValueError(msg)
        elif self.transport_mode == TransportMode.PUBSUB:
            if self.pubsub is None:
                msg = "pubsub config is required when transport_mode is 'pubsub'"
                raise ValueError(msg)
            if self.wal_reader is None:
                msg = "wal_reader config is required when transport_mode is 'pubsub'"
                raise ValueError(msg)
        elif self.transport_mode == TransportMode.KINESIS:
            if self.kinesis is None:
                msg = "kinesis config is required when transport_mode is 'kinesis'"
                raise ValueError(msg)
            if self.wal_reader is None:
                msg = "wal_reader config is required when transport_mode is 'kinesis'"
                raise ValueError(msg)
        return self
