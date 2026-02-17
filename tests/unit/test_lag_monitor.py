"""Unit tests for LagMonitor."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from cdc_platform.observability.metrics import LagMonitor, PartitionLag


@pytest.mark.asyncio
class TestLagMonitor:
    async def test_calls_get_consumer_lag_periodically(self):
        """LagMonitor calls get_consumer_lag on each interval."""
        fake_lag = [
            PartitionLag(
                topic="t", partition=0, current_offset=5, high_watermark=10, lag=5
            ),
        ]
        call_count = 0

        def mock_get_lag(bs, gid, topics):
            nonlocal call_count
            call_count += 1
            return fake_lag

        monitor = LagMonitor(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            topics=["t"],
            interval=0.05,
        )

        with patch(
            "cdc_platform.observability.metrics.get_consumer_lag",
            side_effect=mock_get_lag,
        ):
            await monitor.start()
            await asyncio.sleep(0.2)
            await monitor.stop()

        assert call_count >= 2

    async def test_latest_lag_returns_most_recent(self):
        """latest_lag property returns the most recent lag data."""
        fake_lag = [
            PartitionLag(
                topic="t", partition=0, current_offset=5, high_watermark=10, lag=5
            ),
            PartitionLag(
                topic="t", partition=1, current_offset=8, high_watermark=12, lag=4
            ),
        ]

        monitor = LagMonitor(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            topics=["t"],
            interval=0.05,
        )

        with patch(
            "cdc_platform.observability.metrics.get_consumer_lag", return_value=fake_lag
        ):
            await monitor.start()
            await asyncio.sleep(0.15)
            await monitor.stop()

        lag = monitor.latest_lag
        assert len(lag) == 2
        assert lag[0].topic == "t"
        assert lag[0].lag == 5

    async def test_graceful_stop_cancels_task(self):
        """Stop cancels the internal task cleanly."""
        monitor = LagMonitor(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            topics=["t"],
            interval=100.0,
        )

        with patch(
            "cdc_platform.observability.metrics.get_consumer_lag", return_value=[]
        ):
            await monitor.start()
            assert monitor._task is not None
            assert not monitor._task.done()

            await monitor.stop()
            assert monitor._task.done()

    async def test_handles_lag_check_errors(self):
        """Errors from get_consumer_lag don't crash the monitor."""
        monitor = LagMonitor(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            topics=["t"],
            interval=0.05,
        )

        with patch(
            "cdc_platform.observability.metrics.get_consumer_lag",
            side_effect=Exception("connection refused"),
        ):
            await monitor.start()
            await asyncio.sleep(0.15)
            await monitor.stop()

        # Should still be running (no crash), lag is empty
        assert monitor.latest_lag == []
