"""Unit tests for SchemaMonitor."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest
import respx

from cdc_platform.streaming.schema_monitor import SchemaMonitor

REGISTRY = "http://localhost:8081"


@pytest.mark.asyncio
class TestSchemaMonitor:
    async def test_detects_version_change(self):
        """Version change is detected and logged."""
        monitor = SchemaMonitor(
            registry_url=REGISTRY,
            topics=["cdc.public.t"],
            interval=1.0,
        )
        # Seed baseline
        monitor._known_versions["cdc.public.t-value"] = 1
        monitor._known_schemas["cdc.public.t-value"] = '{"type":"record"}'

        with respx.mock:
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-key/versions/latest").mock(
                return_value=httpx.Response(404)
            )
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-value/versions/latest").mock(
                return_value=httpx.Response(
                    200,
                    json={"version": 2, "id": 10, "schema": '{"type":"record","v":2}'},
                )
            )

            await monitor._check_schemas()

        assert monitor.known_versions["cdc.public.t-value"] == 2

    async def test_first_observation_sets_baseline(self):
        """First poll sets baseline without raising alerts."""
        monitor = SchemaMonitor(
            registry_url=REGISTRY,
            topics=["cdc.public.t"],
            interval=1.0,
        )

        with respx.mock:
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-key/versions/latest").mock(
                return_value=httpx.Response(404)
            )
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-value/versions/latest").mock(
                return_value=httpx.Response(
                    200,
                    json={"version": 1, "id": 5, "schema": '{"type":"record"}'},
                )
            )

            await monitor._check_schemas()

        assert monitor.known_versions == {"cdc.public.t-value": 1}
        assert not monitor.incompatible_detected

    async def test_ignores_404_subjects(self):
        """Topics not yet producing (404) are silently skipped."""
        monitor = SchemaMonitor(
            registry_url=REGISTRY,
            topics=["cdc.public.missing"],
            interval=1.0,
        )

        with respx.mock:
            respx.get(
                f"{REGISTRY}/subjects/cdc.public.missing-key/versions/latest"
            ).mock(return_value=httpx.Response(404))
            respx.get(
                f"{REGISTRY}/subjects/cdc.public.missing-value/versions/latest"
            ).mock(return_value=httpx.Response(404))

            await monitor._check_schemas()

        assert monitor.known_versions == {}

    async def test_handles_registry_errors_gracefully(self):
        """Network/server errors don't crash the monitor."""
        monitor = SchemaMonitor(
            registry_url=REGISTRY,
            topics=["cdc.public.t"],
            interval=1.0,
        )

        with respx.mock:
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-key/versions/latest").mock(
                return_value=httpx.Response(500)
            )
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-value/versions/latest").mock(
                side_effect=httpx.ConnectError("refused")
            )

            # Should not raise
            await monitor._check_schemas()

        assert monitor.known_versions == {}

    async def test_incompatible_change_calls_callback(self):
        """When stop_on_incompatible=True and change is incompatible, callback fires."""
        callback = MagicMock()
        monitor = SchemaMonitor(
            registry_url=REGISTRY,
            topics=["cdc.public.t"],
            interval=1.0,
            stop_on_incompatible=True,
            on_incompatible=callback,
        )
        monitor._known_versions["cdc.public.t-value"] = 1
        monitor._known_schemas["cdc.public.t-value"] = '{"type":"record"}'

        with respx.mock:
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-key/versions/latest").mock(
                return_value=httpx.Response(404)
            )
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-value/versions/latest").mock(
                return_value=httpx.Response(
                    200,
                    json={"version": 2, "id": 11, "schema": '{"type":"record","v":2}'},
                )
            )
            # Compatibility check returns incompatible
            respx.post(
                f"{REGISTRY}/compatibility/subjects/cdc.public.t-value/versions/latest"
            ).mock(return_value=httpx.Response(200, json={"is_compatible": False}))

            await monitor._check_schemas()

        callback.assert_called_once()
        assert monitor.incompatible_detected is True

    async def test_compatible_change_does_not_call_callback(self):
        """When stop_on_incompatible=True but change is compatible, no callback."""
        callback = MagicMock()
        monitor = SchemaMonitor(
            registry_url=REGISTRY,
            topics=["cdc.public.t"],
            interval=1.0,
            stop_on_incompatible=True,
            on_incompatible=callback,
        )
        monitor._known_versions["cdc.public.t-value"] = 1
        monitor._known_schemas["cdc.public.t-value"] = '{"type":"record"}'

        with respx.mock:
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-key/versions/latest").mock(
                return_value=httpx.Response(404)
            )
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-value/versions/latest").mock(
                return_value=httpx.Response(
                    200,
                    json={"version": 2, "id": 11, "schema": '{"type":"record","v":2}'},
                )
            )
            respx.post(
                f"{REGISTRY}/compatibility/subjects/cdc.public.t-value/versions/latest"
            ).mock(return_value=httpx.Response(200, json={"is_compatible": True}))

            await monitor._check_schemas()

        callback.assert_not_called()
        assert not monitor.incompatible_detected

    async def test_incompatible_without_stop_flag_logs_only(self):
        """When stop_on_incompatible=False, incompatible changes are not checked."""
        callback = MagicMock()
        monitor = SchemaMonitor(
            registry_url=REGISTRY,
            topics=["cdc.public.t"],
            interval=1.0,
            stop_on_incompatible=False,
            on_incompatible=callback,
        )
        monitor._known_versions["cdc.public.t-value"] = 1
        monitor._known_schemas["cdc.public.t-value"] = '{"type":"record"}'

        with respx.mock:
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-key/versions/latest").mock(
                return_value=httpx.Response(404)
            )
            respx.get(f"{REGISTRY}/subjects/cdc.public.t-value/versions/latest").mock(
                return_value=httpx.Response(
                    200,
                    json={"version": 2, "id": 11, "schema": '{"type":"record","v":2}'},
                )
            )
            # No compatibility endpoint should be hit

            await monitor._check_schemas()

        callback.assert_not_called()
        assert not monitor.incompatible_detected
        assert monitor.known_versions["cdc.public.t-value"] == 2

    async def test_start_and_stop(self):
        """Start creates a task, stop cancels it."""
        monitor = SchemaMonitor(
            registry_url=REGISTRY,
            topics=[],
            interval=100.0,
        )

        await monitor.start()
        assert monitor._task is not None
        assert not monitor._task.done()

        await monitor.stop()
        assert monitor._task.done()
