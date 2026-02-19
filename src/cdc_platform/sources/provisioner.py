"""Provisioner protocol — transport-agnostic resource setup/teardown."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from cdc_platform.config.models import PipelineConfig


@runtime_checkable
class Provisioner(Protocol):
    """Creates and destroys transport resources (topics, subscriptions, etc.)."""

    async def provision(self, pipeline: PipelineConfig) -> dict[str, Any]:
        """Ensure all required resources exist for the pipeline."""
        ...

    async def teardown(self, pipeline: PipelineConfig) -> None:
        """Remove resources created by provision()."""
        ...
