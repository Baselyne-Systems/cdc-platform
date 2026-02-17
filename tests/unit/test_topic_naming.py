"""Unit tests for topic naming conventions."""

from cdc_platform.config.models import (
    DLQConfig,
    PipelineConfig,
    PlatformConfig,
    SourceConfig,
)
from cdc_platform.streaming.topics import (
    cdc_topic_name,
    dlq_topic_name,
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
