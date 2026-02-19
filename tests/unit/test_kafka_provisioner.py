"""Unit tests for KafkaProvisioner."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from cdc_platform.config.models import (
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

    async def test_teardown_deletes_connector(self):
        platform = PlatformConfig()
        provisioner = KafkaProvisioner(platform)

        with patch("cdc_platform.sources.kafka.provisioner.DebeziumClient") as mock_cls:
            mock_client = AsyncMock()
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            await provisioner.teardown(_pipeline())

            mock_client.delete_connector.assert_awaited_once()
