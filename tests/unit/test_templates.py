"""Unit tests for template loading and config merging."""

import pytest

from cdc_platform.config.models import SourceType
from cdc_platform.config.templates import (
    build_pipeline_config,
    load_template,
    merge_configs,
)


class TestLoadTemplate:
    def test_loads_postgres_template(self):
        tpl = load_template("postgres_cdc_v1")
        assert tpl["source"]["source_type"] == "postgres"
        assert "kafka" in tpl

    def test_missing_template_raises(self):
        with pytest.raises(FileNotFoundError, match="nonexistent"):
            load_template("nonexistent")


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
        assert cfg.kafka.schema_registry_url == "http://localhost:8081"

    def test_override_kafka(self):
        cfg = build_pipeline_config(
            {
                "pipeline_id": "demo",
                "source": {"database": "mydb", "password": "secret"},
                "kafka": {"bootstrap_servers": "broker:29092"},
            },
        )
        assert cfg.kafka.bootstrap_servers == "broker:29092"
        # other kafka defaults preserved
        assert cfg.kafka.auto_offset_reset == "earliest"
