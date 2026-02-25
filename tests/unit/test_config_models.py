"""Unit tests for configuration Pydantic models."""

import pytest
from pydantic import ValidationError

from cdc_platform.config.models import (
    DLQConfig,
    KafkaConfig,
    PipelineConfig,
    PlatformConfig,
    SourceConfig,
    SourceType,
    TransportMode,
)


class TestSourceConfig:
    def test_defaults(self):
        cfg = SourceConfig(database="mydb")
        assert cfg.source_type == SourceType.POSTGRES
        assert cfg.host == "localhost"
        assert cfg.port == 5432

    def test_valid_schema_qualified_tables(self):
        cfg = SourceConfig(
            database="mydb",
            tables=["public.customers", "sales.orders"],
        )
        assert len(cfg.tables) == 2

    def test_invalid_table_name_raises(self):
        with pytest.raises(ValidationError, match="schema-qualified"):
            SourceConfig(database="mydb", tables=["customers"])

    def test_password_is_secret(self):
        cfg = SourceConfig(database="mydb", password="s3cret")
        assert cfg.password.get_secret_value() == "s3cret"
        assert "s3cret" not in str(cfg)
        assert "s3cret" not in repr(cfg)
        assert "s3cret" not in cfg.model_dump_json()


class TestKafkaConfig:
    def test_defaults(self):
        cfg = KafkaConfig()
        assert cfg.bootstrap_servers == "localhost:9092"
        assert cfg.enable_idempotence is True
        assert cfg.topic_num_partitions == 1
        assert cfg.topic_replication_factor == 1

    def test_custom_topic_partitions_and_replication(self):
        cfg = KafkaConfig(topic_num_partitions=6, topic_replication_factor=3)
        assert cfg.topic_num_partitions == 6
        assert cfg.topic_replication_factor == 3

    def test_consumer_tuning_defaults(self):
        cfg = KafkaConfig()
        assert cfg.session_timeout_ms == 45000
        assert cfg.max_poll_interval_ms == 300000
        assert cfg.fetch_min_bytes == 1
        assert cfg.fetch_max_wait_ms == 500

    def test_consumer_tuning_custom(self):
        cfg = KafkaConfig(
            session_timeout_ms=60000,
            max_poll_interval_ms=600000,
            fetch_min_bytes=1024,
            fetch_max_wait_ms=1000,
        )
        assert cfg.session_timeout_ms == 60000
        assert cfg.fetch_min_bytes == 1024

    def test_rejects_zero_partitions(self):
        with pytest.raises(ValidationError):
            KafkaConfig(topic_num_partitions=0)

    def test_rejects_negative_replication_factor(self):
        with pytest.raises(ValidationError):
            KafkaConfig(topic_replication_factor=-1)

    def test_rejects_low_session_timeout(self):
        with pytest.raises(ValidationError):
            KafkaConfig(session_timeout_ms=500)


class TestDLQConfig:
    def test_defaults(self):
        cfg = DLQConfig()
        assert cfg.enabled is True
        assert cfg.topic_suffix == "dlq"
        assert cfg.max_retries == 3

    def test_rejects_empty_topic_suffix(self):
        with pytest.raises(ValidationError):
            DLQConfig(topic_suffix="")


class TestPipelineConfig:
    def test_minimal_valid_config(self):
        cfg = PipelineConfig(
            pipeline_id="test-pipeline",
            source=SourceConfig(database="testdb"),
        )
        assert cfg.pipeline_id == "test-pipeline"
        assert cfg.topic_prefix == "cdc"
        assert cfg.sinks == []

    def test_invalid_topic_prefix(self):
        with pytest.raises(ValidationError, match="topic_prefix"):
            PipelineConfig(
                pipeline_id="test",
                topic_prefix="123invalid",
                source=SourceConfig(database="testdb"),
            )

    def test_rejects_platform_fields(self):
        """Pipeline YAML with kafka/connector/dlq keys raises validation error."""
        with pytest.raises(ValidationError):
            PipelineConfig(
                pipeline_id="test",
                source=SourceConfig(database="testdb"),
                kafka={"bootstrap_servers": "broker:9092"},
            )


class TestTransportMode:
    def test_default_is_kafka(self):
        cfg = PlatformConfig()
        assert cfg.transport_mode == TransportMode.KAFKA

    def test_explicit_kafka(self):
        cfg = PlatformConfig(transport_mode="kafka")
        assert cfg.transport_mode == TransportMode.KAFKA

    def test_kafka_requires_kafka_config(self):
        with pytest.raises(ValidationError, match="kafka config is required"):
            PlatformConfig(transport_mode="kafka", kafka=None)

    def test_kafka_requires_connector_config(self):
        with pytest.raises(ValidationError, match="connector config is required"):
            PlatformConfig(transport_mode="kafka", connector=None)


class TestPlatformConfig:
    def test_all_defaults(self):
        cfg = PlatformConfig()
        assert cfg.transport_mode == TransportMode.KAFKA
        assert cfg.kafka.bootstrap_servers == "localhost:9092"
        assert cfg.connector.connect_url == "http://localhost:8083"
        assert cfg.dlq.enabled is True
        assert cfg.max_buffered_messages == 1000
        assert cfg.schema_monitor_interval_seconds == 30.0
        assert cfg.stop_on_incompatible_schema is False

    def test_override_kafka(self):
        cfg = PlatformConfig(kafka=KafkaConfig(bootstrap_servers="broker:29092"))
        assert cfg.kafka.bootstrap_servers == "broker:29092"
        assert cfg.kafka.auto_offset_reset == "earliest"

    def test_override_tuning(self):
        cfg = PlatformConfig(
            max_buffered_messages=500, stop_on_incompatible_schema=True
        )
        assert cfg.max_buffered_messages == 500
        assert cfg.stop_on_incompatible_schema is True

    def test_rejects_zero_max_buffered_messages(self):
        with pytest.raises(ValidationError):
            PlatformConfig(max_buffered_messages=0)

    def test_rejects_negative_monitor_interval(self):
        with pytest.raises(ValidationError):
            PlatformConfig(schema_monitor_interval_seconds=-1)


class TestRetryConfigBounds:
    def test_rejects_zero_max_attempts(self):
        from cdc_platform.config.models import RetryConfig

        with pytest.raises(ValidationError):
            RetryConfig(max_attempts=0)

    def test_rejects_zero_initial_wait(self):
        from cdc_platform.config.models import RetryConfig

        with pytest.raises(ValidationError):
            RetryConfig(initial_wait_seconds=0)

    def test_rejects_multiplier_below_one(self):
        from cdc_platform.config.models import RetryConfig

        with pytest.raises(ValidationError):
            RetryConfig(multiplier=0.5)
