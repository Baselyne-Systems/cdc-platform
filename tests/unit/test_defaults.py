"""Unit tests for default config loading and merging."""

import pytest

from cdc_platform.config.defaults import (
    build_pipeline_config,
    load_defaults,
    merge_configs,
)
from cdc_platform.config.models import SourceType


class TestLoadDefaults:
    def test_loads_pipeline_defaults(self):
        defaults = load_defaults("pipeline")
        assert defaults["source"]["source_type"] == "postgres"
        assert "kafka" not in defaults
        assert "connector" not in defaults
        assert "dlq" not in defaults
        assert "retry" not in defaults

    def test_loads_platform_defaults(self):
        defaults = load_defaults("platform")
        assert defaults["transport_mode"] == "kafka"
        assert defaults["kafka"]["bootstrap_servers"] == "localhost:9092"
        assert defaults["dlq"]["enabled"] is True

    def test_missing_defaults_raises(self):
        with pytest.raises(FileNotFoundError, match="nonexistent"):
            load_defaults("nonexistent")


class TestMergeConfigs:
    def test_shallow_override(self):
        base = {"a": 1, "b": 2}
        result = merge_configs(base, {"b": 99})
        assert result == {"a": 1, "b": 99}

    def test_deep_merge(self):
        base = {"source": {"host": "localhost", "port": 5432}}
        overrides = {"source": {"host": "db.prod"}}
        result = merge_configs(base, overrides)
        assert result["source"]["host"] == "db.prod"
        assert result["source"]["port"] == 5432

    def test_non_mutating(self):
        base = {"a": {"x": 1}}
        overrides = {"a": {"y": 2}}
        merge_configs(base, overrides)
        assert "y" not in base["a"]


class TestBuildPipelineConfig:
    def test_minimal_overrides(self):
        cfg = build_pipeline_config(
            {
                "pipeline_id": "demo",
                "source": {
                    "database": "mydb",
                    "password": "secret",
                    "tables": ["public.users"],
                },
            },
        )
        assert cfg.pipeline_id == "demo"
        assert cfg.source.source_type == SourceType.POSTGRES
        assert cfg.source.database == "mydb"

    def test_override_source(self):
        cfg = build_pipeline_config(
            {
                "pipeline_id": "demo",
                "source": {
                    "database": "mydb",
                    "password": "secret",
                    "host": "db.prod",
                },
            },
        )
        assert cfg.source.host == "db.prod"
        # other source defaults preserved
        assert cfg.source.port == 5432
