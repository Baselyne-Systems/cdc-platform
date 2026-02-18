"""Pydantic configuration models for CDC pipelines."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator


class SourceType(StrEnum):
    """Supported CDC source types."""

    POSTGRES = "postgres"
    MYSQL = "mysql"


class SnapshotMode(StrEnum):
    """Debezium snapshot modes."""

    INITIAL = "initial"
    NEVER = "never"
    WHEN_NEEDED = "when_needed"
    NO_DATA = "no_data"


class SourceConfig(BaseModel):
    """Configuration for a CDC source database."""

    source_type: SourceType = SourceType.POSTGRES
    host: str = "localhost"
    port: int = 5432
    database: str
    username: str = "cdc_user"
    password: SecretStr = SecretStr("cdc_password")
    tables: list[str] = Field(default_factory=list)
    slot_name: str = "cdc_slot"
    publication_name: str = "cdc_publication"
    snapshot_mode: SnapshotMode = SnapshotMode.INITIAL

    @field_validator("tables")
    @classmethod
    def validate_schema_qualified_tables(cls, v: list[str]) -> list[str]:
        pattern = re.compile(r"^[a-zA-Z_]\w*\.[a-zA-Z_]\w*$")
        for table in v:
            if not pattern.match(table):
                msg = (
                    f"Table '{table}' must be schema-qualified "
                    f"(e.g. 'public.customers')"
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


class ConnectorConfig(BaseModel):
    """Kafka Connect (Debezium) REST API settings."""

    connect_url: str = "http://localhost:8083"
    timeout_seconds: float = 30.0
    retry_max_attempts: int = 5
    retry_wait_seconds: float = 2.0


class DLQConfig(BaseModel):
    """Dead Letter Queue settings."""

    enabled: bool = True
    topic_suffix: str = "dlq"
    max_retries: int = 3
    include_headers: bool = True


class RetryConfig(BaseModel):
    """Retry / backoff configuration."""

    max_attempts: int = 5
    initial_wait_seconds: float = 1.0
    max_wait_seconds: float = 60.0
    multiplier: float = 2.0
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
    batch_size: int = 100
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
    batch_size: int = 1000
    auto_create_table: bool = True
    partition_by: list[str] = Field(default_factory=list)
    s3_endpoint: str | None = None
    s3_access_key_id: str | None = None
    s3_secret_access_key: SecretStr | None = None
    s3_region: str = "us-east-1"
    maintenance: TableMaintenanceConfig = TableMaintenanceConfig()


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

    kafka: KafkaConfig = KafkaConfig()
    connector: ConnectorConfig = ConnectorConfig()
    dlq: DLQConfig = DLQConfig()
    retry: RetryConfig = RetryConfig()
    max_buffered_messages: int = 1000
    partition_concurrency: int = 0
    schema_monitor_interval_seconds: float = 30.0
    lag_monitor_interval_seconds: float = 15.0
    stop_on_incompatible_schema: bool = False
