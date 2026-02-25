"""Integration tests for Google Pub/Sub transport.

Tests the full Pub/Sub source, provisioner, error router, and monitor
using mocked GCP clients.  Verifies protocol conformance, message flow,
and error handling end-to-end.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cdc_platform.config.models import (
    DLQConfig,
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
from cdc_platform.sources.monitor import SourceMonitor
from cdc_platform.sources.provisioner import Provisioner
from cdc_platform.sources.pubsub.naming import (
    cdc_topic_from_pubsub,
    pubsub_dlq_topic_name,
    pubsub_subscription_name,
    pubsub_topic_name,
)
from cdc_platform.sources.pubsub.source import PubSubEventSource, _hash_to_partition


def _pubsub_platform() -> PlatformConfig:
    return PlatformConfig(
        transport_mode=TransportMode.PUBSUB,
        pubsub=PubSubConfig(project_id="test-project"),
        wal_reader=WalReaderConfig(),
    )


def _pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="pubsub-integration",
        source=SourceConfig(database="testdb", tables=["public.customers"]),
    )


@pytest.mark.integration
class TestPubSubNamingConventions:
    """Verify topic/subscription naming is consistent and reversible."""

    def test_topic_name_replaces_dots(self):
        result = pubsub_topic_name("proj", "cdc.public.customers")
        assert "." not in result.split("/")[-1]
        assert result == "projects/proj/topics/cdc-public-customers"

    def test_subscription_includes_group_id(self):
        result = pubsub_subscription_name("proj", "cdc.public.customers", "my-group")
        assert "my-group" in result
        assert result.startswith("projects/proj/subscriptions/")

    def test_dlq_topic_appends_suffix(self):
        result = pubsub_dlq_topic_name("proj", "cdc.public.customers", "dlq")
        assert result.endswith("-dlq")

    def test_roundtrip_topic_name(self):
        original = "cdc.public.customers"
        pubsub = pubsub_topic_name("proj", original)
        recovered = cdc_topic_from_pubsub(pubsub)
        assert recovered == original


@pytest.mark.integration
class TestPubSubVirtualPartitioning:
    """Verify virtual partition hashing for ordering keys."""

    def test_deterministic_partition(self):
        """Same key always maps to same partition."""
        p1 = _hash_to_partition("public.customers")
        p2 = _hash_to_partition("public.customers")
        assert p1 == p2

    def test_different_keys_distribute(self):
        """Different keys should spread across virtual partitions."""
        partitions = set()
        for i in range(100):
            partitions.add(_hash_to_partition(f"table_{i}"))
        # With 16 virtual partitions and 100 keys, expect decent distribution
        assert len(partitions) >= 5

    def test_empty_key_maps_to_zero(self):
        assert _hash_to_partition("") == 0

    def test_partition_in_valid_range(self):
        for i in range(500):
            p = _hash_to_partition(f"key-{i}")
            assert 0 <= p < 16


@pytest.mark.integration
class TestPubSubFactoryDispatch:
    """Verify factory functions dispatch correctly for PUBSUB mode."""

    def test_create_event_source_returns_pubsub(self):
        source = create_event_source(_pipeline(), _pubsub_platform())
        assert isinstance(source, PubSubEventSource)
        assert isinstance(source, EventSource)

    def test_create_provisioner_returns_pubsub(self):
        provisioner = create_provisioner(_pubsub_platform())
        assert isinstance(provisioner, Provisioner)

    def test_create_error_router_returns_pubsub(self):
        router = create_error_router(_pubsub_platform())
        assert router is not None
        assert isinstance(router, ErrorRouter)

    def test_create_error_router_none_when_disabled(self):
        platform = PlatformConfig(
            transport_mode=TransportMode.PUBSUB,
            pubsub=PubSubConfig(project_id="p"),
            wal_reader=WalReaderConfig(),
            dlq=DLQConfig(enabled=False),
        )
        assert create_error_router(platform) is None

    def test_create_source_monitor_returns_pubsub(self):
        monitor = create_source_monitor(_pipeline(), _pubsub_platform())
        assert monitor is not None
        assert isinstance(monitor, SourceMonitor)


@pytest.mark.integration
class TestPubSubErrorRouter:
    """Verify Pub/Sub DLQ error routing."""

    def test_send_includes_diagnostic_attributes(self):
        from cdc_platform.sources.pubsub.error_router import PubSubErrorRouter

        mock_publisher = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-id"
        mock_publisher.publish.return_value = mock_future

        router = PubSubErrorRouter(
            PubSubConfig(project_id="test-proj"),
            DLQConfig(include_headers=True),
        )
        router._publisher = mock_publisher

        router.send(
            source_topic="cdc.public.customers",
            partition=0,
            offset=42,
            key=b"key",
            value=b"value",
            error=ValueError("bad data"),
        )

        mock_publisher.publish.assert_called_once()
        call_kwargs = mock_publisher.publish.call_args
        # Attributes should include diagnostic info
        assert "dlq.source.topic" in call_kwargs.kwargs or any(
            "dlq.source.topic" in str(a) for a in call_kwargs.args
        )

    def test_send_noop_when_disabled(self):
        from cdc_platform.sources.pubsub.error_router import PubSubErrorRouter

        router = PubSubErrorRouter(
            PubSubConfig(project_id="p"),
            DLQConfig(enabled=False),
        )
        # Should not raise
        router.send(
            source_topic="t",
            partition=0,
            offset=0,
            key=None,
            value=None,
            error=Exception("x"),
        )


@pytest.mark.integration
class TestPubSubSourceMonitor:
    """Verify Pub/Sub source monitor lifecycle."""

    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        from cdc_platform.sources.pubsub.monitor import PubSubSourceMonitor

        monitor = PubSubSourceMonitor(
            config=PubSubConfig(project_id="p"),
            subscriptions=["projects/p/subscriptions/s"],
            monitor_interval=0.1,
        )
        await monitor.start()
        assert monitor._task is not None
        assert not monitor._task.done()

        await monitor.stop()
        assert monitor._task.done() or monitor._task.cancelled()

    @pytest.mark.asyncio
    async def test_get_lag_returns_list(self):
        from cdc_platform.sources.pubsub.monitor import PubSubSourceMonitor

        monitor = PubSubSourceMonitor(
            config=PubSubConfig(project_id="p"),
            subscriptions=[
                "projects/p/subscriptions/s1",
                "projects/p/subscriptions/s2",
            ],
            monitor_interval=100,  # won't fire during test
        )
        lag = await monitor.get_lag()
        assert isinstance(lag, list)


@pytest.mark.integration
class TestPubSubEventSourceProtocol:
    """Verify PubSubEventSource satisfies the EventSource protocol contract."""

    def test_has_required_methods(self):
        source = PubSubEventSource(
            subscriptions=["projects/p/subscriptions/s"],
            config=PubSubConfig(project_id="p"),
        )
        assert hasattr(source, "start")
        assert hasattr(source, "commit_offsets")
        assert hasattr(source, "stop")
        assert hasattr(source, "health")

    @pytest.mark.asyncio
    async def test_health_when_stopped(self):
        source = PubSubEventSource(
            subscriptions=["projects/p/subscriptions/s"],
            config=PubSubConfig(project_id="p"),
        )
        health = await source.health()
        assert health["status"] == "stopped"

    def test_stop_when_not_started(self):
        source = PubSubEventSource(
            subscriptions=["projects/p/subscriptions/s"],
            config=PubSubConfig(project_id="p"),
        )
        # Should not raise
        source.stop()

    def test_commit_offsets_when_not_started(self):
        source = PubSubEventSource(
            subscriptions=["projects/p/subscriptions/s"],
            config=PubSubConfig(project_id="p"),
        )
        # Should not raise
        source.commit_offsets({("t", 0): 10})
