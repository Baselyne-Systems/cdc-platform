"""Unit tests for the YAML config loader and env var resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from cdc_platform.config.loader import (
    load_pipeline_config,
    load_platform_config,
    resolve_env_vars,
)

DEMO_CONFIG = Path(__file__).resolve().parents[2] / "examples" / "demo-config.yaml"


class TestResolveEnvVars:
    def test_plain_string_unchanged(self):
        assert resolve_env_vars("hello") == "hello"

    def test_substitutes_env_var(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("MY_HOST", "db.prod")
        assert resolve_env_vars("${MY_HOST}") == "db.prod"

    def test_default_when_var_missing(self):
        result = resolve_env_vars("${MISSING_VAR:-fallback}")
        assert result == "fallback"

    def test_env_var_overrides_default(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("MY_PORT", "9999")
        assert resolve_env_vars("${MY_PORT:-5432}") == "9999"

    def test_empty_default(self):
        assert resolve_env_vars("${MISSING_VAR:-}") == ""

    def test_missing_var_no_default_raises(self):
        with pytest.raises(ValueError, match="UNDEFINED_VAR"):
            resolve_env_vars("${UNDEFINED_VAR}")

    def test_embedded_in_string(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("DB_HOST", "prod-db")
        result = resolve_env_vars("jdbc:postgresql://${DB_HOST}:5432/mydb")
        assert result == "jdbc:postgresql://prod-db:5432/mydb"

    def test_multiple_vars_in_one_string(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("HOST", "localhost")
        monkeypatch.setenv("PORT", "8080")
        result = resolve_env_vars("http://${HOST}:${PORT}")
        assert result == "http://localhost:8080"

    def test_recursive_dict(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("DB_PASS", "secret123")
        data = {
            "host": "localhost",
            "password": "${DB_PASS}",
            "nested": {"url": "${DB_PASS:-default}"},
        }
        result = resolve_env_vars(data)
        assert result["host"] == "localhost"
        assert result["password"] == "secret123"
        assert result["nested"]["url"] == "secret123"

    def test_recursive_list(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("TABLE", "public.users")
        data = ["${TABLE}", "public.orders"]
        result = resolve_env_vars(data)
        assert result == ["public.users", "public.orders"]

    def test_non_string_values_unchanged(self):
        data = {"port": 5432, "enabled": True, "ratio": 0.5, "nothing": None}
        assert resolve_env_vars(data) == data

    def test_default_with_colons(self):
        result = resolve_env_vars("${MISSING:-http://localhost:8081}")
        assert result == "http://localhost:8081"


class TestLoadDemoConfig:
    def test_demo_config_loads_with_defaults(self):
        """The demo config should load and validate using only its default values."""
        config = load_pipeline_config(DEMO_CONFIG)
        assert config.pipeline_id == "demo"
        assert config.topic_prefix == "cdc"
        assert config.source.database == "cdc_demo"
        assert len(config.source.tables) == 2
        assert len(config.sinks) == 3

    def test_demo_config_env_override(self, monkeypatch: pytest.MonkeyPatch):
        """Env vars should override defaults in the demo config."""
        monkeypatch.setenv("CDC_SOURCE_DATABASE", "prod_db")
        monkeypatch.setenv("WEBHOOK_URL", "https://hooks.prod.example.com/cdc")
        config = load_pipeline_config(DEMO_CONFIG)
        assert config.source.database == "prod_db"
        assert config.sinks[0].webhook.url == "https://hooks.prod.example.com/cdc"

    def test_demo_config_sink_types(self):
        config = load_pipeline_config(DEMO_CONFIG)
        sink_types = {s.sink_id: s.sink_type.value for s in config.sinks}
        assert sink_types == {
            "webhook-notifications": "webhook",
            "analytics-db": "postgres",
            "iceberg-lake": "iceberg",
        }

    def test_demo_config_only_webhook_enabled(self):
        config = load_pipeline_config(DEMO_CONFIG)
        enabled = [s.sink_id for s in config.sinks if s.enabled]
        assert enabled == ["webhook-notifications"]


class TestLoadPlatformConfig:
    def test_defaults_when_no_path(self):
        """load_platform_config() with no path returns all defaults."""
        cfg = load_platform_config()
        assert cfg.kafka.bootstrap_servers == "localhost:9092"
        assert cfg.connector.connect_url == "http://localhost:8083"
        assert cfg.dlq.enabled is True
        assert cfg.max_buffered_messages == 1000

    def test_loads_from_yaml(self, tmp_path: Path):
        """load_platform_config() loads overrides from a YAML file."""
        yaml_content = (
            "kafka:\n  bootstrap_servers: broker:29092\nmax_buffered_messages: 500\n"
        )
        config_file = tmp_path / "platform.yaml"
        config_file.write_text(yaml_content)
        cfg = load_platform_config(config_file)
        assert cfg.kafka.bootstrap_servers == "broker:29092"
        assert cfg.max_buffered_messages == 500
        # non-overridden defaults preserved
        assert cfg.connector.connect_url == "http://localhost:8083"
