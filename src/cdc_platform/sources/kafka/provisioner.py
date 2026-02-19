"""Kafka Provisioner — topic creation + Debezium connector registration."""

from __future__ import annotations

from typing import Any

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
        ensure_topics(self._platform.kafka.bootstrap_servers, all_topics)

        # 2. Deploy Debezium connector
        async with DebeziumClient(self._platform.connector) as client:
            await client.wait_until_ready()
            result = await client.register_connector(pipeline, self._platform)
            logger.info("pipeline.connector_deployed", pipeline_id=pipeline.pipeline_id)

        return {
            "topics": all_topics,
            "connector": result,
        }

    async def teardown(self, pipeline: PipelineConfig) -> None:
        assert self._platform.connector is not None
        from cdc_platform.sources.debezium.config import connector_name

        async with DebeziumClient(self._platform.connector) as client:
            name = connector_name(pipeline)
            await client.delete_connector(name)
            logger.info("pipeline.connector_deleted", pipeline_id=pipeline.pipeline_id)
