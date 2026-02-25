"""YAML + environment variable config loader."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import ValidationError

from cdc_platform.config.defaults import (
    DEFAULTS_DIR,
    build_pipeline_config,
    merge_configs,
)
from cdc_platform.config.models import PipelineConfig, PlatformConfig

# Matches ${VAR} or ${VAR:-default}
_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::-((?:[^}\\]|\\.)*))?}")


def _resolve_env_str(value: str) -> str:
    """Replace all ${VAR} / ${VAR:-default} references in a string."""

    def _replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        default = match.group(2)
        env_val = os.environ.get(var_name)
        if env_val is not None:
            return env_val
        if default is not None:
            return default.replace("\\}", "}")
        msg = f"Environment variable '{var_name}' is not set and no default provided"
        raise ValueError(msg)

    return _ENV_PATTERN.sub(_replace, value)


def resolve_env_vars(data: Any) -> Any:
    """Recursively resolve ${VAR} and ${VAR:-default} in parsed YAML data."""
    if isinstance(data, str):
        return _resolve_env_str(data)
    if isinstance(data, dict):
        return {k: resolve_env_vars(v) for k, v in data.items()}
    if isinstance(data, list):
        return [resolve_env_vars(item) for item in data]
    return data


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML file and return its contents as a dict."""
    p = Path(path)
    if not p.exists():
        msg = f"Config file not found: {p}"
        raise FileNotFoundError(msg)
    try:
        with p.open() as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        msg = f"Failed to parse YAML in {p}"
        if hasattr(exc, "problem_mark") and exc.problem_mark is not None:
            mark = exc.problem_mark
            msg += f" at line {mark.line + 1}, column {mark.column + 1}"
        msg += f": {exc}"
        raise ValueError(msg) from exc
    if not isinstance(data, dict):
        msg = f"Expected a YAML mapping at top level in {p}, got {type(data).__name__}"
        raise TypeError(msg)
    return cast(dict[str, Any], resolve_env_vars(data))


def _load_platform_defaults() -> dict[str, Any]:
    """Load the built-in platform.yaml defaults."""
    path = DEFAULTS_DIR / "platform.yaml"
    with path.open() as f:
        return cast(dict[str, Any], yaml.safe_load(f))


def load_platform_config(path: str | Path | None = None) -> PlatformConfig:
    """Load platform config from built-in defaults, optionally merged with overrides."""
    base = _load_platform_defaults()
    if path is not None:
        overrides = load_yaml(path)
        base = merge_configs(base, overrides)
    try:
        return PlatformConfig.model_validate(base)
    except ValidationError as exc:
        source = path or "built-in defaults"
        msg = f"Invalid platform config ({source}):\n{exc}"
        raise ValueError(msg) from exc


def load_pipeline_config(
    path: str | Path,
    *,
    defaults: str = "pipeline",
) -> PipelineConfig:
    """Load a user config YAML and merge with pipeline defaults."""
    overrides = load_yaml(path)
    try:
        return build_pipeline_config(overrides, defaults=defaults)
    except ValidationError as exc:
        msg = f"Invalid pipeline config ({path}):\n{exc}"
        raise ValueError(msg) from exc
