"""Unit tests for topic naming conventions."""

from unittest.mock import MagicMock, patch

import pytest

from cdc_platform.config.models import (
    DLQConfig,
    PipelineConfig,
    PlatformConfig,
    SourceConfig,
)
from cdc_platform.streaming.topics import (
    cdc_topic_name,
    dlq_topic_name,
    ensure_topics,
    topics_for_pipeline,
)


class TestTopicNaming:
    def test_cdc_topic_name(self):
        assert cdc_topic_name("cdc", "public", "customers") == "cdc.public.customers"

    def test_dlq_topic_name_default(self):
        assert dlq_topic_name("cdc.public.customers") == "cdc.public.customers.dlq"

    def test_dlq_topic_name_custom_suffix(self):
        assert (
            dlq_topic_name("cdc.public.orders", suffix="dead")
            == "cdc.public.orders.dead"
        )


class TestTopicsForPipeline:
    def test_with_dlq_enabled(self):
        pipeline = PipelineConfig(
            pipeline_id="test",
            source=SourceConfig(
                database="db",
                tables=["public.customers", "public.orders"],
            ),
        )
        platform = PlatformConfig()
        topics = topics_for_pipeline(pipeline, platform)
        assert topics == [
            "cdc.public.customers",
            "cdc.public.customers.dlq",
            "cdc.public.orders",
            "cdc.public.orders.dlq",
        ]

    def test_with_dlq_disabled(self):
        pipeline = PipelineConfig(
            pipeline_id="test",
            source=SourceConfig(database="db", tables=["public.users"]),
        )
        platform = PlatformConfig(dlq=DLQConfig(enabled=False))
        topics = topics_for_pipeline(pipeline, platform)
        assert topics == ["cdc.public.users"]


class TestEnsureTopics:
    @patch("cdc_platform.streaming.topics.AdminClient")
    def test_raises_on_topic_creation_failure(self, mock_admin_cls):
        """ensure_topics must propagate topic creation failures."""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.return_value = MagicMock(topics={})

        # Simulate a topic creation future that raises
        failed_future = MagicMock()
        failed_future.result.side_effect = Exception("replication factor too high")
        mock_admin.create_topics.return_value = {"test.topic": failed_future}

        with pytest.raises(RuntimeError, match="Failed to create 1 topic"):
            ensure_topics("localhost:9092", ["test.topic"])

    @patch("cdc_platform.streaming.topics.AdminClient")
    def test_succeeds_when_topics_created(self, mock_admin_cls):
        """ensure_topics does not raise when all topics are created."""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.return_value = MagicMock(topics={})

        ok_future = MagicMock()
        ok_future.result.return_value = None
        mock_admin.create_topics.return_value = {"test.topic": ok_future}

        ensure_topics("localhost:9092", ["test.topic"])

    @patch("cdc_platform.streaming.topics.AdminClient")
    def test_skips_existing_topics(self, mock_admin_cls):
        """ensure_topics does not try to create topics that already exist."""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.return_value = MagicMock(
            topics={"existing.topic": MagicMock()}
        )

        ensure_topics("localhost:9092", ["existing.topic"])

        mock_admin.create_topics.assert_not_called()
