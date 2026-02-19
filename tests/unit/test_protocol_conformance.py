"""Protocol conformance tests — verify all implementations satisfy their protocols."""

from __future__ import annotations

from unittest.mock import MagicMock

from cdc_platform.config.models import KafkaConfig, PlatformConfig
from cdc_platform.sources.base import EventSource
from cdc_platform.sources.error_router import ErrorRouter
from cdc_platform.sources.kafka.monitor import KafkaSourceMonitor
from cdc_platform.sources.kafka.provisioner import KafkaProvisioner
from cdc_platform.sources.kafka.source import KafkaEventSource
from cdc_platform.sources.monitor import SourceMonitor
from cdc_platform.sources.provisioner import Provisioner
from cdc_platform.streaming.dlq import DLQHandler


class TestProtocolConformance:
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
