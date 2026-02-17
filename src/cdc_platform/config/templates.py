"""Template loading and config merging utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from cdc_platform.config.models import PipelineConfig

TEMPLATES_DIR = Path(__file__).parent / "templates"


def load_template(name: str = "postgres_cdc_v1") -> dict[str, Any]:
    """Load a YAML template by name from the templates directory."""
    path = TEMPLATES_DIR / f"{name}.yaml"
    if not path.exists():
        msg = f"Template '{name}' not found at {path}"
        raise FileNotFoundError(msg)
    with path.open() as f:
        return yaml.safe_load(f)  # type: ignore[no-any-return]


def merge_configs(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """Recursively deep-merge *overrides* into *base* (non-mutating)."""
    merged: dict[str, Any] = {**base}
    for key, value in overrides.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value
    return merged


def build_pipeline_config(
    overrides: dict[str, Any],
    *,
    template: str = "postgres_cdc_v1",
) -> PipelineConfig:
    """Build a validated PipelineConfig by merging a template with overrides."""
    base = load_template(template)
    merged = merge_configs(base, overrides)
    return PipelineConfig.model_validate(merged)
