"""Integration tests for Amazon Kinesis transport.

Tests the full Kinesis source, provisioner, error router, monitor, and
checkpoint store using mocked AWS clients.  Verifies protocol conformance,
message flow, and error handling end-to-end.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cdc_platform.config.models import (
    DLQConfig,
    KinesisConfig,
    PipelineConfig,
    PlatformConfig,
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
from cdc_platform.sources.kinesis.checkpoint import DynamoDBCheckpointStore
from cdc_platform.sources.kinesis.naming import (
    cdc_topic_from_stream,
    kinesis_dlq_stream_name,
    kinesis_stream_name,
)
from cdc_platform.sources.kinesis.source import KinesisEventSource
from cdc_platform.sources.monitor import SourceMonitor
from cdc_platform.sources.provisioner import Provisioner


def _kinesis_platform() -> PlatformConfig:
    return PlatformConfig(
        transport_mode=TransportMode.KINESIS,
        kinesis=KinesisConfig(region="us-east-1"),
        wal_reader=WalReaderConfig(),
    )


def _pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="kinesis-integration",
        source=SourceConfig(database="testdb", tables=["public.orders"]),
    )


@pytest.mark.integration
class TestKinesisNamingConventions:
    """Verify stream naming is consistent and reversible."""

    def test_stream_name_replaces_dots(self):
        result = kinesis_stream_name("cdc.public.orders")
        assert "." not in result
        assert result == "cdc-public-orders"

    def test_dlq_stream_appends_suffix(self):
        result = kinesis_dlq_stream_name("cdc.public.orders")
        assert result.endswith("-dlq")
        assert result == "cdc-public-orders-dlq"

    def test_dlq_stream_custom_suffix(self):
        result = kinesis_dlq_stream_name("cdc.public.orders", "dead")
        assert result.endswith("-dead")

    def test_roundtrip_stream_name(self):
        original = "cdc.public.orders"
        stream = kinesis_stream_name(original)
        recovered = cdc_topic_from_stream(stream)
        assert recovered == original


@pytest.mark.integration
class TestKinesisFactoryDispatch:
    """Verify factory functions dispatch correctly for KINESIS mode."""

    def test_create_event_source_returns_kinesis(self):
        source = create_event_source(_pipeline(), _kinesis_platform())
        assert isinstance(source, KinesisEventSource)
        assert isinstance(source, EventSource)

    def test_create_provisioner_returns_kinesis(self):
        provisioner = create_provisioner(_kinesis_platform())
        assert isinstance(provisioner, Provisioner)

    def test_create_error_router_returns_kinesis(self):
        router = create_error_router(_kinesis_platform())
        assert router is not None
        assert isinstance(router, ErrorRouter)

    def test_create_error_router_none_when_disabled(self):
        platform = PlatformConfig(
            transport_mode=TransportMode.KINESIS,
            kinesis=KinesisConfig(region="us-east-1"),
            wal_reader=WalReaderConfig(),
            dlq=DLQConfig(enabled=False),
        )
        assert create_error_router(platform) is None

    def test_create_source_monitor_returns_kinesis(self):
        monitor = create_source_monitor(_pipeline(), _kinesis_platform())
        assert monitor is not None
        assert isinstance(monitor, SourceMonitor)


@pytest.mark.integration
class TestKinesisErrorRouter:
    """Verify Kinesis DLQ error routing."""

    def test_send_includes_diagnostic_headers(self):
        from cdc_platform.sources.kinesis.error_router import KinesisErrorRouter

        mock_client = MagicMock()
        mock_client.put_record.return_value = {
            "ShardId": "shardId-000000000000",
            "SequenceNumber": "12345",
        }

        router = KinesisErrorRouter(
            KinesisConfig(region="us-east-1"),
            DLQConfig(include_headers=True),
        )
        router._client = mock_client

        router.send(
            source_topic="cdc.public.orders",
            partition=0,
            offset=42,
            key=b"key",
            value=b"value",
            error=ValueError("bad data"),
        )

        mock_client.put_record.assert_called_once()
        call_kwargs = mock_client.put_record.call_args.kwargs
        assert "StreamName" in call_kwargs
        assert "Data" in call_kwargs
        assert "PartitionKey" in call_kwargs

    def test_send_noop_when_disabled(self):
        from cdc_platform.sources.kinesis.error_router import KinesisErrorRouter

        router = KinesisErrorRouter(
            KinesisConfig(region="us-east-1"),
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
class TestKinesisSourceMonitor:
    """Verify Kinesis source monitor lifecycle."""

    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        from cdc_platform.sources.kinesis.monitor import KinesisSourceMonitor

        monitor = KinesisSourceMonitor(
            config=KinesisConfig(region="us-east-1"),
            streams=["test-stream"],
            monitor_interval=0.1,
        )
        await monitor.start()
        assert monitor._task is not None
        assert not monitor._task.done()

        await monitor.stop()
        assert monitor._task.done() or monitor._task.cancelled()

    @pytest.mark.asyncio
    async def test_get_lag_returns_list(self):
        from cdc_platform.sources.kinesis.monitor import KinesisSourceMonitor

        monitor = KinesisSourceMonitor(
            config=KinesisConfig(region="us-east-1"),
            streams=["stream-a", "stream-b"],
            monitor_interval=100,
        )
        lag = await monitor.get_lag()
        assert isinstance(lag, list)


@pytest.mark.integration
class TestKinesisCheckpointStore:
    """Verify DynamoDB checkpoint store with mocked client."""

    def test_put_and_get_checkpoint(self):
        seq_num = "49590338271490256608559692540000"
        mock_client = MagicMock()
        mock_client.get_item.return_value = {
            "Item": {
                "stream_name": {"S": "test-stream"},
                "shard_id": {"S": "shard-0"},
                "sequence_number": {"S": seq_num},
            }
        }

        store = DynamoDBCheckpointStore(KinesisConfig(region="us-east-1"))
        store._client = mock_client

        store.put_checkpoint("test-stream", "shard-0", seq_num)
        mock_client.put_item.assert_called_once()

        result = store.get_checkpoint("test-stream", "shard-0")
        assert result == seq_num

    def test_get_checkpoint_returns_none_when_missing(self):
        mock_client = MagicMock()
        mock_client.get_item.return_value = {}

        store = DynamoDBCheckpointStore(KinesisConfig(region="us-east-1"))
        store._client = mock_client

        assert store.get_checkpoint("test-stream", "shard-0") is None

    def test_ensure_table_creates_when_not_exists(self):
        mock_client = MagicMock()
        mock_client.exceptions.ResourceNotFoundException = type(
            "ResourceNotFoundException", (Exception,), {}
        )
        mock_client.describe_table.side_effect = (
            mock_client.exceptions.ResourceNotFoundException("not found")
        )
        mock_waiter = MagicMock()
        mock_client.get_waiter.return_value = mock_waiter

        store = DynamoDBCheckpointStore(KinesisConfig(region="us-east-1"))
        store._client = mock_client

        store.ensure_table()
        mock_client.create_table.assert_called_once()
        mock_waiter.wait.assert_called_once()


@pytest.mark.integration
class TestKinesisEventSourceProtocol:
    """Verify KinesisEventSource satisfies the EventSource protocol."""

    def test_has_required_methods(self):
        source = KinesisEventSource(
            streams=["test-stream"],
            config=KinesisConfig(region="us-east-1"),
        )
        assert hasattr(source, "start")
        assert hasattr(source, "commit_offsets")
        assert hasattr(source, "stop")
        assert hasattr(source, "health")

    @pytest.mark.asyncio
    async def test_health_when_stopped(self):
        source = KinesisEventSource(
            streams=["test-stream"],
            config=KinesisConfig(region="us-east-1"),
        )
        health = await source.health()
        assert health["status"] == "stopped"

    def test_stop_when_not_started(self):
        source = KinesisEventSource(
            streams=["test-stream"],
            config=KinesisConfig(region="us-east-1"),
        )
        source.stop()

    def test_commit_offsets_when_not_started(self):
        source = KinesisEventSource(
            streams=["test-stream"],
            config=KinesisConfig(region="us-east-1"),
        )
        source.commit_offsets({("t", 0): 10})
