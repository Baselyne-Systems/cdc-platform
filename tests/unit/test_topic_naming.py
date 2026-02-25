"""Unit tests for topic naming conventions."""

from unittest.mock import MagicMock, patch

import pytest

from cdc_platform.config.models import (
    DLQConfig,
    PipelineConfig,
    PlatformConfig,
    SourceConfig,
    SourceType,
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
    def test_postgres_with_dlq_enabled(self):
        pipeline = PipelineConfig(
            pipeline_id="test",
            source=SourceConfig(
                source_type=SourceType.POSTGRES,
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

    def test_postgres_with_dlq_disabled(self):
        pipeline = PipelineConfig(
            pipeline_id="test",
            source=SourceConfig(database="db", tables=["public.users"]),
        )
        platform = PlatformConfig(dlq=DLQConfig(enabled=False))
        topics = topics_for_pipeline(pipeline, platform)
        assert topics == ["cdc.public.users"]

    def test_mysql_topic_format(self):
        """MySQL: <prefix>.<db>.<table>  (db comes from tables field, not source.database)."""
        pipeline = PipelineConfig(
            pipeline_id="test",
            source=SourceConfig(
                source_type=SourceType.MYSQL,
                database="mydb",
                tables=["mydb.customers", "mydb.orders"],
            ),
        )
        topics = topics_for_pipeline(
            pipeline, PlatformConfig(dlq=DLQConfig(enabled=False))
        )
        assert topics == ["cdc.mydb.customers", "cdc.mydb.orders"]

    def test_mongodb_topic_format(self):
        """MongoDB: <prefix>.<db>.<collection>"""
        pipeline = PipelineConfig(
            pipeline_id="test",
            source=SourceConfig(
                source_type=SourceType.MONGODB,
                database="cdc_demo",
                port=27017,
                tables=["cdc_demo.products", "cdc_demo.orders"],
            ),
        )
        topics = topics_for_pipeline(
            pipeline, PlatformConfig(dlq=DLQConfig(enabled=False))
        )
        assert topics == ["cdc.cdc_demo.products", "cdc.cdc_demo.orders"]

    def test_sqlserver_topic_format(self):
        """SQL Server Debezium 2.x: <prefix>.<database>.<schema>.<table>.

        The database name from SourceConfig is prepended, making a 4-part topic
        name distinct from the 3-part names used by the other connectors.
        """
        pipeline = PipelineConfig(
            pipeline_id="test",
            source=SourceConfig(
                source_type=SourceType.SQLSERVER,
                database="cdc_demo",
                port=1433,
                tables=["dbo.customers", "sales.orders"],
            ),
        )
        topics = topics_for_pipeline(
            pipeline, PlatformConfig(dlq=DLQConfig(enabled=False))
        )
        assert topics == [
            "cdc.cdc_demo.dbo.customers",
            "cdc.cdc_demo.sales.orders",
        ]

    def test_sqlserver_topic_with_dlq(self):
        pipeline = PipelineConfig(
            pipeline_id="test",
            source=SourceConfig(
                source_type=SourceType.SQLSERVER,
                database="cdc_demo",
                port=1433,
                tables=["dbo.customers"],
            ),
        )
        topics = topics_for_pipeline(pipeline, PlatformConfig())
        assert topics == [
            "cdc.cdc_demo.dbo.customers",
            "cdc.cdc_demo.dbo.customers.dlq",
        ]

    def test_custom_topic_prefix_applied_to_all_sources(self):
        for source_type, kwargs in [
            (SourceType.POSTGRES, {"tables": ["public.t"]}),
            (SourceType.MYSQL, {"tables": ["db.t"]}),
            (SourceType.MONGODB, {"tables": ["db.t"], "port": 27017}),
            (SourceType.SQLSERVER, {"tables": ["dbo.t"], "port": 1433}),
        ]:
            pipeline = PipelineConfig(
                pipeline_id="p",
                topic_prefix="myprefix",
                source=SourceConfig(
                    source_type=source_type,
                    database="db",
                    **kwargs,
                ),
            )
            topics = topics_for_pipeline(
                pipeline, PlatformConfig(dlq=DLQConfig(enabled=False))
            )
            assert all(t.startswith("myprefix.") for t in topics), (
                f"Expected all topics for {source_type} to start with 'myprefix.', "
                f"got {topics}"
            )


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
