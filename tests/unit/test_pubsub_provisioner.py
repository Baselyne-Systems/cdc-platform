"""Unit tests for Pub/Sub Provisioner."""

from __future__ import annotations

from cdc_platform.config.models import (
    PlatformConfig,
    PubSubConfig,
    TransportMode,
    WalReaderConfig,
)
from cdc_platform.sources.provisioner import Provisioner
from cdc_platform.sources.pubsub.provisioner import PubSubProvisioner


class TestPubSubProvisioner:
    def test_satisfies_provisioner_protocol(self):
        platform = PlatformConfig(
            transport_mode=TransportMode.PUBSUB,
            pubsub=PubSubConfig(project_id="proj"),
            wal_reader=WalReaderConfig(),
        )
        provisioner = PubSubProvisioner(platform)
        assert isinstance(provisioner, Provisioner)
