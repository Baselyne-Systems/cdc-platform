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

    if platform.transport_mode == TransportMode.PUBSUB:
        assert platform.pubsub is not None
        from cdc_platform.sources.pubsub.naming import pubsub_subscription_name
        from cdc_platform.sources.pubsub.source import PubSubEventSource

        all_topics = topics_for_pipeline(pipeline, platform)
        cdc_topics = [t for t in all_topics if not t.endswith(".dlq")]
        subscriptions = [
            pubsub_subscription_name(
                platform.pubsub.project_id, t, platform.pubsub.group_id
            )
            for t in cdc_topics
        ]
        return PubSubEventSource(
            subscriptions=subscriptions,
            config=platform.pubsub,
        )

    if platform.transport_mode == TransportMode.KINESIS:
        assert platform.kinesis is not None
        from cdc_platform.sources.kinesis.naming import kinesis_stream_name
        from cdc_platform.sources.kinesis.source import KinesisEventSource

        all_topics = topics_for_pipeline(pipeline, platform)
        cdc_topics = [t for t in all_topics if not t.endswith(".dlq")]
        streams = [kinesis_stream_name(t) for t in cdc_topics]
        return KinesisEventSource(
            streams=streams,
            config=platform.kinesis,
        )

    msg = f"Unsupported transport mode: {platform.transport_mode}"
    raise ValueError(msg)


def create_provisioner(platform: PlatformConfig) -> Provisioner:
    """Create a Provisioner for the configured transport mode."""
    if platform.transport_mode == TransportMode.KAFKA:
        from cdc_platform.sources.kafka.provisioner import KafkaProvisioner

        return KafkaProvisioner(platform)

    if platform.transport_mode == TransportMode.PUBSUB:
        from cdc_platform.sources.pubsub.provisioner import PubSubProvisioner

        return PubSubProvisioner(platform)

    if platform.transport_mode == TransportMode.KINESIS:
        from cdc_platform.sources.kinesis.provisioner import KinesisProvisioner

        return KinesisProvisioner(platform)

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

    if platform.transport_mode == TransportMode.PUBSUB:
        assert platform.pubsub is not None
        from cdc_platform.sources.pubsub.error_router import PubSubErrorRouter

        return PubSubErrorRouter(platform.pubsub, platform.dlq)

    if platform.transport_mode == TransportMode.KINESIS:
        assert platform.kinesis is not None
        from cdc_platform.sources.kinesis.error_router import KinesisErrorRouter

        return KinesisErrorRouter(platform.kinesis, platform.dlq)

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

    if platform.transport_mode == TransportMode.PUBSUB:
        assert platform.pubsub is not None
        from cdc_platform.sources.pubsub.monitor import PubSubSourceMonitor
        from cdc_platform.sources.pubsub.naming import pubsub_subscription_name

        all_topics = topics_for_pipeline(pipeline, platform)
        cdc_topics = [t for t in all_topics if not t.endswith(".dlq")]
        subscriptions = [
            pubsub_subscription_name(
                platform.pubsub.project_id, t, platform.pubsub.group_id
            )
            for t in cdc_topics
        ]
        return PubSubSourceMonitor(
            config=platform.pubsub,
            subscriptions=subscriptions,
            monitor_interval=platform.lag_monitor_interval_seconds,
        )

    if platform.transport_mode == TransportMode.KINESIS:
        assert platform.kinesis is not None
        from cdc_platform.sources.kinesis.monitor import KinesisSourceMonitor
        from cdc_platform.sources.kinesis.naming import kinesis_stream_name

        all_topics = topics_for_pipeline(pipeline, platform)
        cdc_topics = [t for t in all_topics if not t.endswith(".dlq")]
        streams = [kinesis_stream_name(t) for t in cdc_topics]
        return KinesisSourceMonitor(
            config=platform.kinesis,
            streams=streams,
            monitor_interval=platform.lag_monitor_interval_seconds,
        )

    msg = f"Unsupported transport mode: {platform.transport_mode}"
    raise ValueError(msg)
