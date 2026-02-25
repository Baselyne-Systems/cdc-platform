"""Unit tests for Kinesis Provisioner."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

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
from cdc_platform.sources.kinesis.provisioner import KinesisProvisioner
from cdc_platform.sources.provisioner import Provisioner


def _platform(
    dlq_enabled: bool = True, dlq_shard_count: int = 1, checkpoint_ttl: int = 604800
) -> PlatformConfig:
    return PlatformConfig(
        transport_mode=TransportMode.KINESIS,
        kinesis=KinesisConfig(
            dlq_shard_count=dlq_shard_count,
            checkpoint_ttl_seconds=checkpoint_ttl,
        ),
        wal_reader=WalReaderConfig(),
        dlq=DLQConfig(enabled=dlq_enabled),
    )


def _pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="test",
        source=SourceConfig(database="testdb", tables=["public.t"]),
    )


def _mock_boto3(mock_kinesis_client):
    """Create a mock boto3 module for sys.modules patching."""
    mock_boto3_mod = MagicMock()
    mock_boto3_mod.client.return_value = mock_kinesis_client
    return {"boto3": mock_boto3_mod}


def _mock_kinesis_client():
    """Create a mock Kinesis client with exception types."""
    client = MagicMock()
    client.exceptions.ResourceInUseException = type(
        "ResourceInUseException", (Exception,), {}
    )
    return client


class TestKinesisProvisioner:
    def test_satisfies_provisioner_protocol(self):
        provisioner = KinesisProvisioner(_platform())
        assert isinstance(provisioner, Provisioner)


class TestKinesisConfigDlqShards:
    def test_dlq_shard_count_default(self):
        config = KinesisConfig()
        assert config.dlq_shard_count == 1

    def test_dlq_shard_count_custom(self):
        config = KinesisConfig(dlq_shard_count=4)
        assert config.dlq_shard_count == 4

    def test_dlq_shard_count_validation(self):
        with pytest.raises(ValueError):
            KinesisConfig(dlq_shard_count=0)


class TestKinesisConfigCheckpointTtl:
    def test_checkpoint_ttl_default(self):
        config = KinesisConfig()
        assert config.checkpoint_ttl_seconds == 604800

    def test_checkpoint_ttl_disabled(self):
        config = KinesisConfig(checkpoint_ttl_seconds=0)
        assert config.checkpoint_ttl_seconds == 0


@pytest.mark.asyncio
class TestKinesisProvisionerRollback:
    async def test_rollback_on_checkpoint_failure(self):
        """If DynamoDB checkpoint setup fails, created streams are rolled back."""
        platform = _platform()
        provisioner = KinesisProvisioner(platform)

        mock_client = _mock_kinesis_client()

        with (
            patch.dict("sys.modules", _mock_boto3(mock_client)),
            patch(
                "cdc_platform.sources.kinesis.checkpoint.DynamoDBCheckpointStore.ensure_table",
                side_effect=RuntimeError("dynamo error"),
            ),
            patch.object(
                provisioner, "_rollback_streams", new_callable=AsyncMock
            ) as mock_rb,
        ):
            with pytest.raises(RuntimeError, match="dynamo error"):
                await provisioner.provision(_pipeline())

            mock_rb.assert_awaited_once()

    async def test_dlq_uses_configured_shard_count(self):
        """DLQ stream should use dlq_shard_count from config, not hardcoded 1."""
        platform = _platform(dlq_shard_count=4)
        provisioner = KinesisProvisioner(platform)

        mock_client = _mock_kinesis_client()
        create_calls: list[dict] = []

        def capture_create(**kwargs):
            create_calls.append(kwargs)

        mock_client.create_stream.side_effect = capture_create

        with (
            patch.dict("sys.modules", _mock_boto3(mock_client)),
            patch(
                "cdc_platform.sources.kinesis.checkpoint.DynamoDBCheckpointStore.ensure_table",
            ),
            patch(
                "cdc_platform.sources.wal.slot_manager.SlotManager",
            ) as mock_slot_cls,
        ):
            mock_slot = AsyncMock()
            mock_slot_cls.return_value = mock_slot

            await provisioner.provision(_pipeline())

            dlq_calls = [c for c in create_calls if "dlq" in c["StreamName"]]
            assert len(dlq_calls) == 1
            assert dlq_calls[0]["ShardCount"] == 4


@pytest.mark.asyncio
class TestKinesisTeardownDLQ:
    async def test_teardown_deletes_dlq_streams(self):
        """Teardown should also delete DLQ streams when DLQ is enabled."""
        platform = _platform(dlq_enabled=True)
        provisioner = KinesisProvisioner(platform)

        mock_client = _mock_kinesis_client()

        with patch.dict("sys.modules", _mock_boto3(mock_client)):
            await provisioner.teardown(_pipeline())

            delete_calls = mock_client.delete_stream.call_args_list
            deleted_streams = [c.kwargs["StreamName"] for c in delete_calls]
            assert any("dlq" in s for s in deleted_streams)

    async def test_teardown_skips_dlq_when_disabled(self):
        """Teardown should not attempt DLQ deletion when DLQ is disabled."""
        platform = _platform(dlq_enabled=False)
        provisioner = KinesisProvisioner(platform)

        mock_client = _mock_kinesis_client()

        with patch.dict("sys.modules", _mock_boto3(mock_client)):
            await provisioner.teardown(_pipeline())

            assert mock_client.delete_stream.call_count == 1
