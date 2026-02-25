"""Unit tests for configuration Pydantic models."""

import pytest
from pydantic import ValidationError

from cdc_platform.config.models import (
    DLQConfig,
    IcebergSinkConfig,
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
        with pytest.raises(ValidationError, match="qualified"):
            SourceConfig(database="mydb", tables=["customers"])

    def test_password_is_secret(self):
        cfg = SourceConfig(database="mydb", password="s3cret")
        assert cfg.password.get_secret_value() == "s3cret"
        assert "s3cret" not in str(cfg)
        assert "s3cret" not in repr(cfg)
        assert "s3cret" not in cfg.model_dump_json()


class TestMongoDBSourceConfig:
    def test_defaults(self):
        cfg = SourceConfig(source_type=SourceType.MONGODB, database="mydb")
        assert cfg.source_type == SourceType.MONGODB
        assert cfg.auth_source == "admin"
        assert cfg.replica_set_name is None

    def test_custom_port(self):
        cfg = SourceConfig(
            source_type=SourceType.MONGODB,
            database="mydb",
            host="mongo-host",
            port=27017,
        )
        assert cfg.port == 27017
        assert cfg.host == "mongo-host"

    def test_replica_set_name(self):
        cfg = SourceConfig(
            source_type=SourceType.MONGODB,
            database="mydb",
            replica_set_name="rs0",
        )
        assert cfg.replica_set_name == "rs0"

    def test_custom_auth_source(self):
        cfg = SourceConfig(
            source_type=SourceType.MONGODB,
            database="mydb",
            auth_source="mydb",
        )
        assert cfg.auth_source == "mydb"

    def test_valid_db_qualified_collections(self):
        cfg = SourceConfig(
            source_type=SourceType.MONGODB,
            database="mydb",
            tables=["mydb.orders", "mydb.customers"],
        )
        assert len(cfg.tables) == 2

    def test_unqualified_collection_raises(self):
        with pytest.raises(ValidationError, match="qualified"):
            SourceConfig(
                source_type=SourceType.MONGODB,
                database="mydb",
                tables=["orders"],
            )

    def test_password_is_secret(self):
        cfg = SourceConfig(
            source_type=SourceType.MONGODB,
            database="mydb",
            password="mongosecret",
        )
        assert cfg.password.get_secret_value() == "mongosecret"
        assert "mongosecret" not in cfg.model_dump_json()


class TestSQLServerSourceConfig:
    def test_defaults(self):
        cfg = SourceConfig(source_type=SourceType.SQLSERVER, database="mydb")
        assert cfg.source_type == SourceType.SQLSERVER
        assert cfg.port == 5432  # users should override to 1433

    def test_custom_port(self):
        cfg = SourceConfig(
            source_type=SourceType.SQLSERVER,
            database="mydb",
            host="sqlserver-host",
            port=1433,
        )
        assert cfg.port == 1433

    def test_valid_schema_qualified_tables(self):
        cfg = SourceConfig(
            source_type=SourceType.SQLSERVER,
            database="mydb",
            tables=["dbo.customers", "sales.orders"],
        )
        assert len(cfg.tables) == 2

    def test_unqualified_table_raises(self):
        with pytest.raises(ValidationError, match="qualified"):
            SourceConfig(
                source_type=SourceType.SQLSERVER,
                database="mydb",
                tables=["customers"],
            )

    def test_password_is_secret(self):
        cfg = SourceConfig(
            source_type=SourceType.SQLSERVER,
            database="mydb",
            password="sqls3cret",
        )
        assert cfg.password.get_secret_value() == "sqls3cret"
        assert "sqls3cret" not in cfg.model_dump_json()


class TestMySQLSourceConfig:
    def test_mysql_server_id_default(self):
        cfg = SourceConfig(source_type=SourceType.MYSQL, database="mydb")
        assert cfg.mysql_server_id == 1

    def test_mysql_server_id_custom(self):
        cfg = SourceConfig(
            source_type=SourceType.MYSQL,
            database="mydb",
            mysql_server_id=42,
        )
        assert cfg.mysql_server_id == 42

    def test_mysql_server_id_rejects_zero(self):
        with pytest.raises(ValidationError):
            SourceConfig(
                source_type=SourceType.MYSQL,
                database="mydb",
                mysql_server_id=0,
            )


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

    def test_high_throughput_defaults(self):
        cfg = KafkaConfig()
        assert cfg.poll_batch_size == 1
        assert cfg.deser_pool_size == 1
        assert cfg.commit_interval_seconds == 0.0

    def test_high_throughput_custom(self):
        cfg = KafkaConfig(
            poll_batch_size=500,
            deser_pool_size=4,
            commit_interval_seconds=5.0,
        )
        assert cfg.poll_batch_size == 500
        assert cfg.deser_pool_size == 4
        assert cfg.commit_interval_seconds == 5.0

    def test_rejects_zero_poll_batch_size(self):
        with pytest.raises(ValidationError):
            KafkaConfig(poll_batch_size=0)

    def test_rejects_zero_deser_pool_size(self):
        with pytest.raises(ValidationError):
            KafkaConfig(deser_pool_size=0)

    def test_rejects_negative_commit_interval(self):
        with pytest.raises(ValidationError):
            KafkaConfig(commit_interval_seconds=-1.0)


class TestDLQConfig:
    def test_defaults(self):
        cfg = DLQConfig()
        assert cfg.enabled is True
        assert cfg.topic_suffix == "dlq"
        assert cfg.max_retries == 3

    def test_rejects_empty_topic_suffix(self):
        with pytest.raises(ValidationError):
            DLQConfig(topic_suffix="")

    def test_flush_interval_default(self):
        cfg = DLQConfig()
        assert cfg.flush_interval_seconds == 0.0

    def test_flush_interval_custom(self):
        cfg = DLQConfig(flush_interval_seconds=5.0)
        assert cfg.flush_interval_seconds == 5.0

    def test_rejects_negative_flush_interval(self):
        with pytest.raises(ValidationError):
            DLQConfig(flush_interval_seconds=-1.0)


class TestIcebergSinkHighThroughput:
    def test_defaults(self):
        cfg = IcebergSinkConfig(
            catalog_uri="sqlite:////tmp/cat.db",
            warehouse="file:///tmp/wh",
            table_name="events",
        )
        assert cfg.flush_interval_seconds == 0.0
        assert cfg.write_executor_threads == 0

    def test_custom(self):
        cfg = IcebergSinkConfig(
            catalog_uri="sqlite:////tmp/cat.db",
            warehouse="file:///tmp/wh",
            table_name="events",
            flush_interval_seconds=2.0,
            write_executor_threads=4,
        )
        assert cfg.flush_interval_seconds == 2.0
        assert cfg.write_executor_threads == 4

    def test_rejects_negative_flush_interval(self):
        with pytest.raises(ValidationError):
            IcebergSinkConfig(
                catalog_uri="sqlite:////tmp/cat.db",
                warehouse="file:///tmp/wh",
                table_name="events",
                flush_interval_seconds=-1.0,
            )

    def test_rejects_negative_write_executor_threads(self):
        with pytest.raises(ValidationError):
            IcebergSinkConfig(
                catalog_uri="sqlite:////tmp/cat.db",
                warehouse="file:///tmp/wh",
                table_name="events",
                write_executor_threads=-1,
            )


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
