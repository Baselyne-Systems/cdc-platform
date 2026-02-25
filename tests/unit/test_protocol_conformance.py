"""Protocol conformance tests — verify all implementations satisfy their protocols."""

from __future__ import annotations

from unittest.mock import MagicMock

from cdc_platform.config.models import (
    DLQConfig,
    KafkaConfig,
    KinesisConfig,
    PlatformConfig,
    PubSubConfig,
    TransportMode,
    WalReaderConfig,
)
from cdc_platform.sources.base import EventSource
from cdc_platform.sources.error_router import ErrorRouter
from cdc_platform.sources.kafka.monitor import KafkaSourceMonitor
from cdc_platform.sources.kafka.provisioner import KafkaProvisioner
from cdc_platform.sources.kafka.source import KafkaEventSource
from cdc_platform.sources.kinesis.error_router import KinesisErrorRouter
from cdc_platform.sources.kinesis.monitor import KinesisSourceMonitor
from cdc_platform.sources.kinesis.provisioner import KinesisProvisioner
from cdc_platform.sources.kinesis.source import KinesisEventSource
from cdc_platform.sources.monitor import SourceMonitor
from cdc_platform.sources.provisioner import Provisioner
from cdc_platform.sources.pubsub.error_router import PubSubErrorRouter
from cdc_platform.sources.pubsub.monitor import PubSubSourceMonitor
from cdc_platform.sources.pubsub.provisioner import PubSubProvisioner
from cdc_platform.sources.pubsub.source import PubSubEventSource
from cdc_platform.streaming.dlq import DLQHandler


class TestProtocolConformance:
    # -- Kafka -----------------------------------------------------------------
    def test_kafka_event_source_satisfies_event_source(self):
        source = KafkaEventSource(
            topics=["t"],
            kafka_config=KafkaConfig(),
        )
        assert isinstance(source, EventSource)

    def test_kafka_provisioner_satisfies_provisioner(self):
        provisioner = KafkaProvisioner(PlatformConfig())
        assert isinstance(provisioner, Provisioner)

    def test_dlq_handler_satisfies_error_router(self):
        producer = MagicMock()
        handler = DLQHandler(producer)
        assert isinstance(handler, ErrorRouter)

    def test_kafka_source_monitor_satisfies_source_monitor(self):
        monitor = KafkaSourceMonitor(
            kafka_config=KafkaConfig(),
            topics=["t"],
        )
        assert isinstance(monitor, SourceMonitor)

    # -- Pub/Sub ---------------------------------------------------------------
    def test_pubsub_event_source_satisfies_event_source(self):
        source = PubSubEventSource(
            subscriptions=["projects/p/subscriptions/s"],
            config=PubSubConfig(project_id="p"),
        )
        assert isinstance(source, EventSource)

    def test_pubsub_provisioner_satisfies_provisioner(self):
        platform = PlatformConfig(
            transport_mode=TransportMode.PUBSUB,
            pubsub=PubSubConfig(project_id="p"),
            wal_reader=WalReaderConfig(),
        )
        provisioner = PubSubProvisioner(platform)
        assert isinstance(provisioner, Provisioner)

    def test_pubsub_error_router_satisfies_error_router(self):
        router = PubSubErrorRouter(PubSubConfig(project_id="p"), DLQConfig())
        assert isinstance(router, ErrorRouter)

    def test_pubsub_source_monitor_satisfies_source_monitor(self):
        monitor = PubSubSourceMonitor(
            config=PubSubConfig(project_id="p"),
            subscriptions=["projects/p/subscriptions/s"],
        )
        assert isinstance(monitor, SourceMonitor)

    # -- Kinesis ---------------------------------------------------------------
    def test_kinesis_event_source_satisfies_event_source(self):
        source = KinesisEventSource(
            streams=["cdc-public-users"],
            config=KinesisConfig(),
        )
        assert isinstance(source, EventSource)

    def test_kinesis_provisioner_satisfies_provisioner(self):
        platform = PlatformConfig(
            transport_mode=TransportMode.KINESIS,
            kinesis=KinesisConfig(),
            wal_reader=WalReaderConfig(),
        )
        provisioner = KinesisProvisioner(platform)
        assert isinstance(provisioner, Provisioner)

    def test_kinesis_error_router_satisfies_error_router(self):
        router = KinesisErrorRouter(KinesisConfig(), DLQConfig())
        assert isinstance(router, ErrorRouter)

    def test_kinesis_source_monitor_satisfies_source_monitor(self):
        monitor = KinesisSourceMonitor(
            config=KinesisConfig(),
            streams=["cdc-public-users"],
        )
        assert isinstance(monitor, SourceMonitor)
