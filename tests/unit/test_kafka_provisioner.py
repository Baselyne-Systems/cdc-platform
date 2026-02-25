"""Unit tests for KafkaProvisioner."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from cdc_platform.config.models import (
    KafkaConfig,
    PipelineConfig,
    PlatformConfig,
    SourceConfig,
)
from cdc_platform.sources.kafka.provisioner import KafkaProvisioner


def _pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="test",
        source=SourceConfig(database="testdb", tables=["public.t"]),
    )


@pytest.mark.asyncio
class TestKafkaProvisioner:
    async def test_provision_creates_topics_and_registers_connector(self):
        platform = PlatformConfig()
        provisioner = KafkaProvisioner(platform)

        with (
            patch(
                "cdc_platform.sources.kafka.provisioner.ensure_topics"
            ) as mock_ensure,
            patch("cdc_platform.sources.kafka.provisioner.DebeziumClient") as mock_cls,
        ):
            mock_client = AsyncMock()
            mock_client.register_connector.return_value = {"name": "cdc-test"}
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await provisioner.provision(_pipeline())

            mock_ensure.assert_called_once()
            mock_client.wait_until_ready.assert_awaited_once()
            mock_client.register_connector.assert_awaited_once()
            assert "topics" in result
            assert "connector" in result

    async def test_provision_passes_partition_and_replication_config(self):
        platform = PlatformConfig(
            kafka=KafkaConfig(topic_num_partitions=6, topic_replication_factor=3)
        )
        provisioner = KafkaProvisioner(platform)

        with (
            patch(
                "cdc_platform.sources.kafka.provisioner.ensure_topics"
            ) as mock_ensure,
            patch("cdc_platform.sources.kafka.provisioner.DebeziumClient") as mock_cls,
        ):
            mock_client = AsyncMock()
            mock_client.register_connector.return_value = {"name": "cdc-test"}
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            await provisioner.provision(_pipeline())

            mock_ensure.assert_called_once()
            call_kwargs = mock_ensure.call_args
            assert call_kwargs.kwargs["num_partitions"] == 6
            assert call_kwargs.kwargs["replication_factor"] == 3

    async def test_teardown_deletes_connector(self):
        platform = PlatformConfig()
        provisioner = KafkaProvisioner(platform)

        with patch("cdc_platform.sources.kafka.provisioner.DebeziumClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            await provisioner.teardown(_pipeline())

            mock_client.delete_connector.assert_awaited_once()

    async def test_teardown_ignores_404(self):
        """Teardown should not raise if connector is already deleted (404)."""
        platform = PlatformConfig()
        provisioner = KafkaProvisioner(platform)

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_request = MagicMock()
        error_404 = httpx.HTTPStatusError(
            "Not Found", request=mock_request, response=mock_response
        )

        with patch("cdc_platform.sources.kafka.provisioner.DebeziumClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.delete_connector.side_effect = error_404
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            # Should NOT raise
            await provisioner.teardown(_pipeline())

    async def test_teardown_raises_non_404_errors(self):
        """Teardown should re-raise non-404 HTTP errors."""
        platform = PlatformConfig()
        provisioner = KafkaProvisioner(platform)

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_request = MagicMock()
        error_500 = httpx.HTTPStatusError(
            "Server Error", request=mock_request, response=mock_response
        )

        with patch("cdc_platform.sources.kafka.provisioner.DebeziumClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.delete_connector.side_effect = error_500
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with pytest.raises(httpx.HTTPStatusError):
                await provisioner.teardown(_pipeline())

    async def test_provision_rolls_back_topics_on_connector_failure(self):
        """If connector registration fails, created topics should be deleted."""
        platform = PlatformConfig()
        provisioner = KafkaProvisioner(platform)

        with (
            patch(
                "cdc_platform.sources.kafka.provisioner.ensure_topics"
            ) as mock_ensure,
            patch("cdc_platform.sources.kafka.provisioner.DebeziumClient") as mock_cls,
            patch.object(provisioner, "_rollback_topics") as mock_rollback,
        ):
            mock_client = AsyncMock()
            mock_client.wait_until_ready.side_effect = RuntimeError("connect down")
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with pytest.raises(RuntimeError, match="connect down"):
                await provisioner.provision(_pipeline())

            mock_ensure.assert_called_once()
            mock_rollback.assert_called_once()
            # Rollback should be called with the topic list
            rollback_topics = mock_rollback.call_args[0][0]
            assert len(rollback_topics) > 0
