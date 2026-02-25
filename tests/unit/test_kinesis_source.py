"""Unit tests for Kinesis EventSource."""

from __future__ import annotations

from cdc_platform.config.models import KinesisConfig
from cdc_platform.sources.base import EventSource
from cdc_platform.sources.kinesis.source import KinesisEventSource


class TestKinesisEventSource:
    def test_satisfies_event_source_protocol(self):
        source = KinesisEventSource(
            streams=["cdc-public-users"],
            config=KinesisConfig(),
        )
        assert isinstance(source, EventSource)


class TestKinesisNaming:
    def test_stream_name(self):
        from cdc_platform.sources.kinesis.naming import kinesis_stream_name

        assert kinesis_stream_name("cdc.public.users") == "cdc-public-users"

    def test_dlq_stream_name(self):
        from cdc_platform.sources.kinesis.naming import kinesis_dlq_stream_name

        assert kinesis_dlq_stream_name("cdc.public.users") == "cdc-public-users-dlq"

    def test_cdc_topic_from_stream(self):
        from cdc_platform.sources.kinesis.naming import cdc_topic_from_stream

        assert cdc_topic_from_stream("cdc-public-users") == "cdc.public.users"
