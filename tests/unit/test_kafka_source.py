"""Unit tests for KafkaEventSource."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cdc_platform.config.models import KafkaConfig
from cdc_platform.sources.kafka.source import KafkaEventSource


class TestKafkaEventSource:
    def test_commit_offsets_delegates_to_consumer(self):
        source = KafkaEventSource(
            topics=["t"],
            kafka_config=KafkaConfig(),
        )
        mock_consumer = MagicMock()
        source._consumer = mock_consumer

        offsets = {("t", 0): 10, ("t", 1): 5}
        source.commit_offsets(offsets)

        mock_consumer.commit_offsets.assert_called_once_with(offsets)

    def test_stop_delegates_to_consumer(self):
        source = KafkaEventSource(
            topics=["t"],
            kafka_config=KafkaConfig(),
        )
        mock_consumer = MagicMock()
        source._consumer = mock_consumer

        source.stop()

        mock_consumer.stop.assert_called_once()

    def test_commit_offsets_noop_when_no_consumer(self):
        source = KafkaEventSource(
            topics=["t"],
            kafka_config=KafkaConfig(),
        )
        # Should not raise
        source.commit_offsets({("t", 0): 10})

    def test_stop_noop_when_no_consumer(self):
        source = KafkaEventSource(
            topics=["t"],
            kafka_config=KafkaConfig(),
        )
        # Should not raise
        source.stop()

    @pytest.mark.asyncio
    async def test_health_returns_unknown_without_connector_config(self):
        source = KafkaEventSource(
            topics=["t"],
            kafka_config=KafkaConfig(),
        )
        result = await source.health()
        assert result["status"] == "unknown"
