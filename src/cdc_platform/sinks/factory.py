"""Sink factory — maps SinkType to concrete connector classes."""

from __future__ import annotations

from cdc_platform.config.models import SinkConfig, SinkType
from cdc_platform.sinks.base import SinkConnector
from cdc_platform.sinks.iceberg import IcebergSink
from cdc_platform.sinks.postgres import PostgresSink
from cdc_platform.sinks.webhook import WebhookSink

_SINK_REGISTRY: dict[SinkType, type] = {
    SinkType.WEBHOOK: WebhookSink,
    SinkType.POSTGRES: PostgresSink,
    SinkType.ICEBERG: IcebergSink,
}


def create_sink(config: SinkConfig) -> SinkConnector:
    """Create a sink connector from configuration.

    Adding a new sink = one class + one dict entry in ``_SINK_REGISTRY``.
    """
    cls = _SINK_REGISTRY.get(config.sink_type)
    if cls is None:
        msg = f"Unknown sink type: {config.sink_type}"
        raise ValueError(msg)
    return cls(config)  # type: ignore[no-any-return]
