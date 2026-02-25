"""Unit tests for Pub/Sub Provisioner."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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
from cdc_platform.sources.provisioner import Provisioner
from cdc_platform.sources.pubsub.provisioner import PubSubProvisioner


def _platform(dlq_enabled: bool = True) -> PlatformConfig:
    return PlatformConfig(
        transport_mode=TransportMode.PUBSUB,
        pubsub=PubSubConfig(project_id="proj"),
        wal_reader=WalReaderConfig(),
        dlq=DLQConfig(enabled=dlq_enabled),
    )


def _pipeline() -> PipelineConfig:
    return PipelineConfig(
        pipeline_id="test",
        source=SourceConfig(database="testdb", tables=["public.t"]),
    )


def _mock_gcp_modules():
    """Create mock GCP modules with consistent hierarchy."""
    mock_pubsub_v1 = MagicMock()
    mock_exceptions = MagicMock()
    mock_exceptions.AlreadyExists = type("AlreadyExists", (Exception,), {})

    mock_google = MagicMock()
    mock_cloud = MagicMock()
    mock_api_core = MagicMock()

    # Wire up the hierarchy so attribute access matches sys.modules entries
    mock_google.cloud = mock_cloud
    mock_google.api_core = mock_api_core
    mock_cloud.pubsub_v1 = mock_pubsub_v1
    mock_api_core.exceptions = mock_exceptions

    return {
        "google": mock_google,
        "google.cloud": mock_cloud,
        "google.cloud.pubsub_v1": mock_pubsub_v1,
        "google.api_core": mock_api_core,
        "google.api_core.exceptions": mock_exceptions,
    }


class TestPubSubProvisioner:
    def test_satisfies_provisioner_protocol(self):
        provisioner = PubSubProvisioner(_platform())
        assert isinstance(provisioner, Provisioner)


@pytest.mark.asyncio
class TestPubSubProvisionerRollback:
    async def test_rollback_on_slot_failure(self):
        """If PG slot creation fails, created topics/subs are rolled back."""
        platform = _platform()
        provisioner = PubSubProvisioner(platform)

        gcp_mods = _mock_gcp_modules()
        mock_pubsub_v1 = gcp_mods["google.cloud.pubsub_v1"]

        with (
            patch.dict("sys.modules", gcp_mods),
            patch(
                "cdc_platform.sources.wal.slot_manager.SlotManager",
                side_effect=RuntimeError("pg down"),
            ),
            patch.object(provisioner, "_rollback") as mock_rollback,
        ):
            with pytest.raises(RuntimeError, match="pg down"):
                await provisioner.provision(_pipeline())

            mock_rollback.assert_called_once()
            args = mock_rollback.call_args[0]
            # Verify subscriber and publisher passed to rollback
            assert args[0] is mock_pubsub_v1.SubscriberClient.return_value
            assert args[1] is mock_pubsub_v1.PublisherClient.return_value

    async def test_dlq_topic_failure_propagates(self):
        """If DLQ topic creation fails (non-AlreadyExists), error propagates."""
        platform = _platform(dlq_enabled=True)
        provisioner = PubSubProvisioner(platform)

        gcp_mods = _mock_gcp_modules()
        mock_pubsub_v1 = gcp_mods["google.cloud.pubsub_v1"]

        call_count = 0

        def create_topic_side_effect(request):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return
            raise RuntimeError("permission denied")

        mock_pubsub_v1.PublisherClient.return_value.create_topic.side_effect = (
            create_topic_side_effect
        )

        with (
            patch.dict("sys.modules", gcp_mods),
            patch.object(provisioner, "_rollback"),
            pytest.raises(RuntimeError, match="permission denied"),
        ):
            await provisioner.provision(_pipeline())


@pytest.mark.asyncio
class TestPubSubTeardownDLQ:
    async def test_teardown_deletes_dlq_topics(self):
        """Teardown should also delete DLQ topics when DLQ is enabled."""
        platform = _platform(dlq_enabled=True)
        provisioner = PubSubProvisioner(platform)

        gcp_mods = _mock_gcp_modules()
        mock_publisher = gcp_mods["google.cloud.pubsub_v1"].PublisherClient.return_value

        with patch.dict("sys.modules", gcp_mods):
            await provisioner.teardown(_pipeline())

            delete_calls = mock_publisher.delete_topic.call_args_list
            deleted_topics = [c.kwargs["request"]["topic"] for c in delete_calls]
            assert any("dlq" in t for t in deleted_topics)

    async def test_teardown_skips_dlq_when_disabled(self):
        """Teardown should not attempt DLQ deletion when DLQ is disabled."""
        platform = _platform(dlq_enabled=False)
        provisioner = PubSubProvisioner(platform)

        gcp_mods = _mock_gcp_modules()
        mock_publisher = gcp_mods["google.cloud.pubsub_v1"].PublisherClient.return_value

        with patch.dict("sys.modules", gcp_mods):
            await provisioner.teardown(_pipeline())

            assert mock_publisher.delete_topic.call_count == 1
