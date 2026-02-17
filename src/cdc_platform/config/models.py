"""Pydantic configuration models for CDC pipelines."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration — composes all sub-configs."""

    pipeline_id: str
    topic_prefix: TopicPrefix = "cdc"
    source: SourceConfig
    kafka: KafkaConfig = KafkaConfig()
    connector: ConnectorConfig = ConnectorConfig()
    dlq: DLQConfig = DLQConfig()
    retry: RetryConfig = RetryConfig()


class PlatformSettings(BaseSettings):
    """Environment-driven platform settings (CDC_ prefix)."""

    model_config = SettingsConfigDict(
        env_prefix="CDC_",
        env_nested_delimiter="__",
    )

    log_level: str = "INFO"
    source: SourceConfig = SourceConfig(database="cdc_demo")
    kafka: KafkaConfig = KafkaConfig()
    connector: ConnectorConfig = ConnectorConfig()
