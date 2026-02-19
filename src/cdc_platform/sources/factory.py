"""Factory functions for transport-agnostic source components."""

from __future__ import annotations

from collections.abc import Callable

from cdc_platform.config.models import PipelineConfig, PlatformConfig, TransportMode
from cdc_platform.sources.base import EventSource
from cdc_platform.sources.error_router import ErrorRouter
from cdc_platform.sources.monitor import SourceMonitor
from cdc_platform.sources.provisioner import Provisioner
from cdc_platform.streaming.topics import topics_for_pipeline


def create_event_source(
    pipeline: PipelineConfig,
    platform: PlatformConfig,
) -> EventSource:
    """Create an EventSource for the configured transport mode."""
    if platform.transport_mode == TransportMode.KAFKA:
        assert platform.kafka is not None
        from cdc_platform.sources.kafka.source import KafkaEventSource

        all_topics = topics_for_pipeline(pipeline, platform)
        cdc_topics = [t for t in all_topics if not t.endswith(".dlq")]
        return KafkaEventSource(
            topics=cdc_topics,
            kafka_config=platform.kafka,
            dlq_config=platform.dlq,
            connector_config=platform.connector,
            pipeline=pipeline,
        )
    msg = f"Unsupported transport mode: {platform.transport_mode}"
    raise ValueError(msg)


def create_provisioner(platform: PlatformConfig) -> Provisioner:
    """Create a Provisioner for the configured transport mode."""
    if platform.transport_mode == TransportMode.KAFKA:
        from cdc_platform.sources.kafka.provisioner import KafkaProvisioner

        return KafkaProvisioner(platform)
    msg = f"Unsupported transport mode: {platform.transport_mode}"
    raise ValueError(msg)


def create_error_router(platform: PlatformConfig) -> ErrorRouter | None:
    """Create an ErrorRouter for the configured transport mode, or None if DLQ disabled."""
    if not platform.dlq.enabled:
        return None

    if platform.transport_mode == TransportMode.KAFKA:
        assert platform.kafka is not None
        from cdc_platform.streaming.dlq import DLQHandler
        from cdc_platform.streaming.producer import create_producer

        producer = create_producer(platform.kafka)
        return DLQHandler(producer, platform.dlq)

    msg = f"Unsupported transport mode: {platform.transport_mode}"
    raise ValueError(msg)


def create_source_monitor(
    pipeline: PipelineConfig,
    platform: PlatformConfig,
    on_incompatible: Callable[[], None] | None = None,
) -> SourceMonitor | None:
    """Create a SourceMonitor for the configured transport mode."""
    if platform.transport_mode == TransportMode.KAFKA:
        assert platform.kafka is not None
        from cdc_platform.sources.kafka.monitor import KafkaSourceMonitor

        all_topics = topics_for_pipeline(pipeline, platform)
        cdc_topics = [t for t in all_topics if not t.endswith(".dlq")]
        return KafkaSourceMonitor(
            kafka_config=platform.kafka,
            topics=cdc_topics,
            schema_monitor_interval=platform.schema_monitor_interval_seconds,
            lag_monitor_interval=platform.lag_monitor_interval_seconds,
            stop_on_incompatible=platform.stop_on_incompatible_schema,
            on_incompatible=on_incompatible,
        )
    msg = f"Unsupported transport mode: {platform.transport_mode}"
    raise ValueError(msg)
