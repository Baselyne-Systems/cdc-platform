"""YAML + environment variable config loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from cdc_platform.config.models import PipelineConfig
from cdc_platform.config.templates import build_pipeline_config


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML file and return its contents as a dict."""
    p = Path(path)
    if not p.exists():
        msg = f"Config file not found: {p}"
        raise FileNotFoundError(msg)
    with p.open() as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        msg = f"Expected a YAML mapping at top level, got {type(data).__name__}"
        raise TypeError(msg)
    return data


def load_pipeline_config(
    path: str | Path,
    *,
    template: str = "postgres_cdc_v1",
) -> PipelineConfig:
    """Load a user config YAML and merge with a platform template."""
    overrides = load_yaml(path)
    return build_pipeline_config(overrides, template=template)
