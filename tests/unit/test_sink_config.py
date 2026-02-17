"""Unit tests for sink configuration Pydantic models."""

import pytest
from pydantic import ValidationError

from cdc_platform.config.models import (
    PipelineConfig,
    PostgresSinkConfig,
    SinkConfig,
    SinkType,
    SourceConfig,
    WebhookSinkConfig,
)


class TestSinkType:
    def test_values(self):
        assert SinkType.WEBHOOK == "webhook"
        assert SinkType.POSTGRES == "postgres"


class TestWebhookSinkConfig:
    def test_defaults(self):
        cfg = WebhookSinkConfig(url="http://example.com/hook")
        assert cfg.method == "POST"
        assert cfg.headers == {}
        assert cfg.timeout_seconds == 30.0
        assert cfg.auth_token is None

    def test_auth_token_is_secret(self):
        cfg = WebhookSinkConfig(url="http://example.com", auth_token="s3cret")
        assert cfg.auth_token.get_secret_value() == "s3cret"
        assert "s3cret" not in str(cfg)


class TestPostgresSinkConfig:
    def test_defaults(self):
        cfg = PostgresSinkConfig(database="analytics", target_table="public.cdc_events")
        assert cfg.host == "localhost"
        assert cfg.port == 5432
        assert cfg.batch_size == 100
        assert cfg.upsert is False

    def test_valid_schema_qualified_table(self):
        cfg = PostgresSinkConfig(database="db", target_table="myschema.mytable")
        assert cfg.target_table == "myschema.mytable"

    def test_invalid_target_table_raises(self):
        with pytest.raises(ValidationError, match="schema-qualified"):
            PostgresSinkConfig(database="db", target_table="no_schema")

    def test_password_is_secret(self):
        cfg = PostgresSinkConfig(
            database="db", target_table="public.t", password="secret123"
        )
        assert cfg.password.get_secret_value() == "secret123"
        assert "secret123" not in str(cfg)


class TestSinkConfig:
    def test_webhook_requires_webhook_sub_config(self):
        with pytest.raises(ValidationError, match="webhook config is required"):
            SinkConfig(sink_id="test", sink_type=SinkType.WEBHOOK)

    def test_postgres_requires_postgres_sub_config(self):
        with pytest.raises(ValidationError, match="postgres config is required"):
            SinkConfig(sink_id="test", sink_type=SinkType.POSTGRES)

    def test_valid_webhook_config(self):
        cfg = SinkConfig(
            sink_id="my-webhook",
            sink_type=SinkType.WEBHOOK,
            webhook=WebhookSinkConfig(url="http://example.com"),
        )
        assert cfg.sink_id == "my-webhook"
        assert cfg.enabled is True

    def test_valid_postgres_config(self):
        cfg = SinkConfig(
            sink_id="my-pg",
            sink_type=SinkType.POSTGRES,
            postgres=PostgresSinkConfig(database="db", target_table="public.events"),
        )
        assert cfg.sink_id == "my-pg"

    def test_disabled_by_default_false(self):
        cfg = SinkConfig(
            sink_id="x",
            sink_type=SinkType.WEBHOOK,
            enabled=False,
            webhook=WebhookSinkConfig(url="http://example.com"),
        )
        assert cfg.enabled is False


class TestPipelineConfigBackwardCompat:
    def test_sinks_defaults_to_empty(self):
        cfg = PipelineConfig(
            pipeline_id="test",
            source=SourceConfig(database="testdb"),
        )
        assert cfg.sinks == []

    def test_sinks_populated(self):
        cfg = PipelineConfig(
            pipeline_id="test",
            source=SourceConfig(database="testdb"),
            sinks=[
                SinkConfig(
                    sink_id="wh",
                    sink_type=SinkType.WEBHOOK,
                    webhook=WebhookSinkConfig(url="http://example.com"),
                ),
            ],
        )
        assert len(cfg.sinks) == 1
        assert cfg.sinks[0].sink_id == "wh"
