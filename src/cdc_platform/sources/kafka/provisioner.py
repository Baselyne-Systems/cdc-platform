"""Kafka Provisioner — topic creation + Debezium connector registration."""

from __future__ import annotations

from typing import Any

import httpx
import structlog

from cdc_platform.config.models import (
    PipelineConfig,
    PlatformConfig,
)
from cdc_platform.sources.debezium.client import DebeziumClient
from cdc_platform.streaming.topics import ensure_topics, topics_for_pipeline

logger = structlog.get_logger()


class KafkaProvisioner:
    """Creates Kafka topics and registers the Debezium connector."""

    def __init__(self, platform: PlatformConfig) -> None:
        self._platform = platform

    async def provision(self, pipeline: PipelineConfig) -> dict[str, Any]:
        assert self._platform.kafka is not None
        assert self._platform.connector is not None

        # 1. Ensure topics exist
        all_topics = topics_for_pipeline(pipeline, self._platform)
        ensure_topics(
            self._platform.kafka.bootstrap_servers,
            all_topics,
            num_partitions=self._platform.kafka.topic_num_partitions,
            replication_factor=self._platform.kafka.topic_replication_factor,
            kafka_config=self._platform.kafka,
        )

        # 2. Deploy Debezium connector — rollback topics on failure
        try:
            async with DebeziumClient(self._platform.connector) as client:
                await client.wait_until_ready()
                result = await client.register_connector(pipeline, self._platform)
                logger.info(
                    "pipeline.connector_deployed",
                    pipeline_id=pipeline.pipeline_id,
                )
        except Exception:
            logger.error(
                "pipeline.connector_deploy_failed",
                pipeline_id=pipeline.pipeline_id,
                topics=all_topics,
            )
            self._rollback_topics(all_topics)
            raise

        return {
            "topics": all_topics,
            "connector": result,
        }

    def _rollback_topics(self, topics: list[str]) -> None:
        """Best-effort cleanup of topics created during a failed provision."""
        assert self._platform.kafka is not None
        try:
            from confluent_kafka.admin import AdminClient

            admin = AdminClient(
                {"bootstrap.servers": self._platform.kafka.bootstrap_servers}
            )
            futures = admin.delete_topics(topics)
            for topic, fut in futures.items():
                try:
                    fut.result()
                    logger.info("pipeline.rollback_topic_deleted", topic=topic)
                except Exception as exc:
                    logger.warning(
                        "pipeline.rollback_topic_delete_failed",
                        topic=topic,
                        error=str(exc),
                    )
        except Exception as exc:
            logger.warning(
                "pipeline.rollback_failed",
                topics=topics,
                error=str(exc),
            )

    async def teardown(self, pipeline: PipelineConfig) -> None:
        assert self._platform.connector is not None
        from cdc_platform.sources.debezium.config import connector_name

        async with DebeziumClient(self._platform.connector) as client:
            name = connector_name(pipeline)
            try:
                await client.delete_connector(name)
                logger.info(
                    "pipeline.connector_deleted",
                    pipeline_id=pipeline.pipeline_id,
                )
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    logger.info(
                        "pipeline.connector_already_deleted",
                        pipeline_id=pipeline.pipeline_id,
                        connector=name,
                    )
                else:
                    raise
