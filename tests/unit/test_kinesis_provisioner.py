"""Unit tests for Kinesis Provisioner."""

from __future__ import annotations

from cdc_platform.config.models import (
    KinesisConfig,
    PlatformConfig,
    TransportMode,
    WalReaderConfig,
)
from cdc_platform.sources.kinesis.provisioner import KinesisProvisioner
from cdc_platform.sources.provisioner import Provisioner


class TestKinesisProvisioner:
    def test_satisfies_provisioner_protocol(self):
        platform = PlatformConfig(
            transport_mode=TransportMode.KINESIS,
            kinesis=KinesisConfig(),
            wal_reader=WalReaderConfig(),
        )
        provisioner = KinesisProvisioner(platform)
        assert isinstance(provisioner, Provisioner)
