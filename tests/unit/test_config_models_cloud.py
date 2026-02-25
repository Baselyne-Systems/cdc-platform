"""Unit tests for cloud transport config models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from cdc_platform.config.models import (
    KinesisConfig,
    PlatformConfig,
    PubSubConfig,
    TransportMode,
    WalReaderConfig,
)


class TestPubSubConfig:
    def test_defaults(self):
        config = PubSubConfig(project_id="my-project")
        assert config.project_id == "my-project"
        assert config.ordering_enabled is True
        assert config.ack_deadline_seconds == 600
        assert config.max_messages_per_pull == 100
        assert config.group_id == "cdc-platform"

    def test_custom_values(self):
        config = PubSubConfig(
            project_id="proj",
            ordering_enabled=False,
            ack_deadline_seconds=60,
            max_messages_per_pull=50,
            group_id="my-group",
        )
        assert config.ordering_enabled is False
        assert config.ack_deadline_seconds == 60

    def test_ack_deadline_bounds(self):
        with pytest.raises(ValidationError):
            PubSubConfig(project_id="p", ack_deadline_seconds=5)
        with pytest.raises(ValidationError):
            PubSubConfig(project_id="p", ack_deadline_seconds=601)


class TestKinesisConfig:
    def test_defaults(self):
        config = KinesisConfig()
        assert config.region == "us-east-1"
        assert config.shard_count == 1
        assert config.iterator_type == "TRIM_HORIZON"
        assert config.checkpoint_table_name == "cdc-kinesis-checkpoints"
        assert config.poll_interval_seconds == 1.0

    def test_custom_values(self):
        config = KinesisConfig(
            region="eu-west-1",
            shard_count=4,
            iterator_type="LATEST",
        )
        assert config.region == "eu-west-1"
        assert config.shard_count == 4

    def test_shard_count_ge_1(self):
        with pytest.raises(ValidationError):
            KinesisConfig(shard_count=0)


class TestWalReaderConfig:
    def test_defaults(self):
        config = WalReaderConfig()
        assert config.publication_name == "cdc_publication"
        assert config.slot_name == "cdc_slot"
        assert config.status_interval_seconds == 10.0
        assert config.batch_size == 100

    def test_batch_size_ge_1(self):
        with pytest.raises(ValidationError):
            WalReaderConfig(batch_size=0)

    def test_status_interval_gt_0(self):
        with pytest.raises(ValidationError):
            WalReaderConfig(status_interval_seconds=0)


class TestPlatformConfigTransportValidation:
    def test_pubsub_requires_pubsub_config(self):
        with pytest.raises(ValidationError, match="pubsub config"):
            PlatformConfig(transport_mode=TransportMode.PUBSUB)

    def test_pubsub_requires_wal_reader(self):
        with pytest.raises(ValidationError, match="wal_reader config"):
            PlatformConfig(
                transport_mode=TransportMode.PUBSUB,
                pubsub=PubSubConfig(project_id="proj"),
            )

    def test_pubsub_valid(self):
        config = PlatformConfig(
            transport_mode=TransportMode.PUBSUB,
            pubsub=PubSubConfig(project_id="proj"),
            wal_reader=WalReaderConfig(),
        )
        assert config.transport_mode == TransportMode.PUBSUB

    def test_kinesis_requires_kinesis_config(self):
        with pytest.raises(ValidationError, match="kinesis config"):
            PlatformConfig(transport_mode=TransportMode.KINESIS)

    def test_kinesis_requires_wal_reader(self):
        with pytest.raises(ValidationError, match="wal_reader config"):
            PlatformConfig(
                transport_mode=TransportMode.KINESIS,
                kinesis=KinesisConfig(),
            )

    def test_kinesis_valid(self):
        config = PlatformConfig(
            transport_mode=TransportMode.KINESIS,
            kinesis=KinesisConfig(),
            wal_reader=WalReaderConfig(),
        )
        assert config.transport_mode == TransportMode.KINESIS

    def test_kafka_still_works(self):
        config = PlatformConfig()
        assert config.transport_mode == TransportMode.KAFKA
