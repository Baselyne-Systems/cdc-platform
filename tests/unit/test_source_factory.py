"""Unit tests for the source factory."""

from __future__ import annotations

from cdc_platform.config.models import (
    DLQConfig,
    KinesisConfig,
    PipelineConfig,
    PlatformConfig,
    PubSubConfig,
    SourceConfig,
    TransportMode,
    WalReaderConfig,
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
from cdc_platform.sources.kinesis.provisioner import KinesisProvisioner
from cdc_platform.sources.kinesis.source import KinesisEventSource
from cdc_platform.sources.monitor import SourceMonitor
from cdc_platform.sources.provisioner import Provisioner
from cdc_platform.sources.pubsub.provisioner import PubSubProvisioner
from cdc_platform.sources.pubsub.source import PubSubEventSource


def _pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="test",
        source=SourceConfig(database="testdb", tables=["public.t"]),
    )


def _platform() -> PlatformConfig:
    return PlatformConfig()


def _pubsub_platform() -> PlatformConfig:
    return PlatformConfig(
        transport_mode=TransportMode.PUBSUB,
        pubsub=PubSubConfig(project_id="my-project"),
        wal_reader=WalReaderConfig(),
    )


def _kinesis_platform() -> PlatformConfig:
    return PlatformConfig(
        transport_mode=TransportMode.KINESIS,
        kinesis=KinesisConfig(),
        wal_reader=WalReaderConfig(),
    )


class TestCreateEventSource:
    def test_returns_kafka_source_for_kafka_mode(self):
        source = create_event_source(_pipeline(), _platform())
        assert isinstance(source, KafkaEventSource)
        assert isinstance(source, EventSource)

    def test_returns_pubsub_source_for_pubsub_mode(self):
        source = create_event_source(_pipeline(), _pubsub_platform())
        assert isinstance(source, PubSubEventSource)
        assert isinstance(source, EventSource)

    def test_returns_kinesis_source_for_kinesis_mode(self):
        source = create_event_source(_pipeline(), _kinesis_platform())
        assert isinstance(source, KinesisEventSource)
        assert isinstance(source, EventSource)


class TestCreateProvisioner:
    def test_returns_kafka_provisioner_for_kafka_mode(self):
        provisioner = create_provisioner(_platform())
        assert isinstance(provisioner, KafkaProvisioner)
        assert isinstance(provisioner, Provisioner)

    def test_returns_pubsub_provisioner_for_pubsub_mode(self):
        provisioner = create_provisioner(_pubsub_platform())
        assert isinstance(provisioner, PubSubProvisioner)
        assert isinstance(provisioner, Provisioner)

    def test_returns_kinesis_provisioner_for_kinesis_mode(self):
        provisioner = create_provisioner(_kinesis_platform())
        assert isinstance(provisioner, KinesisProvisioner)
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

    def test_returns_pubsub_error_router(self):
        router = create_error_router(_pubsub_platform())
        assert router is not None
        assert isinstance(router, ErrorRouter)

    def test_returns_kinesis_error_router(self):
        router = create_error_router(_kinesis_platform())
        assert router is not None
        assert isinstance(router, ErrorRouter)


class TestCreateSourceMonitor:
    def test_returns_kafka_monitor_for_kafka_mode(self):
        monitor = create_source_monitor(_pipeline(), _platform())
        assert monitor is not None
        assert isinstance(monitor, KafkaSourceMonitor)
        assert isinstance(monitor, SourceMonitor)

    def test_returns_pubsub_monitor_for_pubsub_mode(self):
        monitor = create_source_monitor(_pipeline(), _pubsub_platform())
        assert monitor is not None
        assert isinstance(monitor, SourceMonitor)

    def test_returns_kinesis_monitor_for_kinesis_mode(self):
        monitor = create_source_monitor(_pipeline(), _kinesis_platform())
        assert monitor is not None
        assert isinstance(monitor, SourceMonitor)
