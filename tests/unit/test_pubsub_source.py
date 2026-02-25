"""Unit tests for Pub/Sub EventSource."""

from __future__ import annotations

from cdc_platform.config.models import PubSubConfig
from cdc_platform.sources.base import EventSource
from cdc_platform.sources.pubsub.source import PubSubEventSource, _hash_to_partition


class TestPubSubEventSource:
    def test_satisfies_event_source_protocol(self):
        source = PubSubEventSource(
            subscriptions=["projects/p/subscriptions/sub1"],
            config=PubSubConfig(project_id="p"),
        )
        assert isinstance(source, EventSource)

    def test_hash_to_partition_deterministic(self):
        p1 = _hash_to_partition("public.users")
        p2 = _hash_to_partition("public.users")
        assert p1 == p2

    def test_hash_to_partition_empty_string(self):
        assert _hash_to_partition("") == 0

    def test_hash_to_partition_range(self):
        for i in range(100):
            p = _hash_to_partition(f"key_{i}")
            assert 0 <= p < 16


class TestPubSubNaming:
    def test_topic_name(self):
        from cdc_platform.sources.pubsub.naming import pubsub_topic_name

        result = pubsub_topic_name("my-project", "cdc.public.users")
        assert result == "projects/my-project/topics/cdc-public-users"

    def test_subscription_name(self):
        from cdc_platform.sources.pubsub.naming import pubsub_subscription_name

        result = pubsub_subscription_name(
            "my-project", "cdc.public.users", "cdc-platform"
        )
        assert (
            result == "projects/my-project/subscriptions/cdc-public-users-cdc-platform"
        )

    def test_dlq_topic_name(self):
        from cdc_platform.sources.pubsub.naming import pubsub_dlq_topic_name

        result = pubsub_dlq_topic_name("my-project", "cdc.public.users", "dlq")
        assert result == "projects/my-project/topics/cdc-public-users-dlq"

    def test_cdc_topic_from_pubsub(self):
        from cdc_platform.sources.pubsub.naming import cdc_topic_from_pubsub

        result = cdc_topic_from_pubsub("projects/my-project/topics/cdc-public-users")
        assert result == "cdc.public.users"
