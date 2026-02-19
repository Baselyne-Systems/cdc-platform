"""Unit tests for the source factory."""

from __future__ import annotations

from cdc_platform.config.models import (
    DLQConfig,
    PipelineConfig,
    PlatformConfig,
    SourceConfig,
)
from cdc_platform.sources.base import EventSource
from cdc_platform.sources.error_router import ErrorRouter
from cdc_platform.sources.factory import (
    create_error_router,
    create_event_source,
    create_provisioner,
    create_source_monitor,
)
from cdc_platform.sources.kafka.monitor import KafkaSourceMonitor
from cdc_platform.sources.kafka.provisioner import KafkaProvisioner
from cdc_platform.sources.kafka.source import KafkaEventSource
from cdc_platform.sources.monitor import SourceMonitor
from cdc_platform.sources.provisioner import Provisioner


def _pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="test",
        source=SourceConfig(database="testdb", tables=["public.t"]),
    )


def _platform() -> PlatformConfig:
    return PlatformConfig()


class TestCreateEventSource:
    def test_returns_kafka_source_for_kafka_mode(self):
        source = create_event_source(_pipeline(), _platform())
        assert isinstance(source, KafkaEventSource)
        assert isinstance(source, EventSource)


class TestCreateProvisioner:
    def test_returns_kafka_provisioner_for_kafka_mode(self):
        provisioner = create_provisioner(_platform())
        assert isinstance(provisioner, KafkaProvisioner)
        assert isinstance(provisioner, Provisioner)


class TestCreateErrorRouter:
    def test_returns_dlq_handler_when_enabled(self):
        router = create_error_router(_platform())
        assert router is not None
        assert isinstance(router, ErrorRouter)

    def test_returns_none_when_dlq_disabled(self):
        platform = PlatformConfig(dlq=DLQConfig(enabled=False))
        router = create_error_router(platform)
        assert router is None


class TestCreateSourceMonitor:
    def test_returns_kafka_monitor_for_kafka_mode(self):
        monitor = create_source_monitor(_pipeline(), _platform())
        assert monitor is not None
        assert isinstance(monitor, KafkaSourceMonitor)
        assert isinstance(monitor, SourceMonitor)
