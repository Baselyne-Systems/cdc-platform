"""KinesisProvisioner — creates Kinesis streams + DynamoDB checkpoint table."""

from __future__ import annotations

from typing import Any

import structlog

from cdc_platform.config.models import PipelineConfig, PlatformConfig
from cdc_platform.sources.kinesis.checkpoint import DynamoDBCheckpointStore
from cdc_platform.sources.kinesis.naming import (
    kinesis_dlq_stream_name,
    kinesis_stream_name,
)
from cdc_platform.streaming.topics import topics_for_pipeline

logger = structlog.get_logger()


class KinesisProvisioner:
    """Creates Kinesis streams, DynamoDB checkpoint table, and PG replication slot."""

    def __init__(self, platform: PlatformConfig) -> None:
        self._platform = platform

    async def provision(self, pipeline: PipelineConfig) -> dict[str, Any]:
        assert self._platform.kinesis is not None
        assert self._platform.wal_reader is not None

        import asyncio

        import boto3

        config = self._platform.kinesis
        client = boto3.client("kinesis", region_name=config.region)
        loop = asyncio.get_running_loop()

        all_cdc_topics = topics_for_pipeline(pipeline, self._platform)
        cdc_topics = [t for t in all_cdc_topics if not t.endswith(".dlq")]

        created_streams: list[str] = []

        try:
            for cdc_topic in cdc_topics:
                stream = kinesis_stream_name(cdc_topic)
                try:
                    await loop.run_in_executor(
                        None,
                        lambda s=stream: client.create_stream(
                            StreamName=s,
                            ShardCount=config.shard_count,
                        ),
                    )
                    created_streams.append(stream)
                    logger.info("kinesis.stream_created", stream=stream)
                except client.exceptions.ResourceInUseException:
                    logger.info("kinesis.stream_exists", stream=stream)

                # Create DLQ stream
                if self._platform.dlq.enabled:
                    dlq_stream = kinesis_dlq_stream_name(
                        cdc_topic, config.dlq_stream_suffix
                    )
                    try:
                        await loop.run_in_executor(
                            None,
                            lambda s=dlq_stream: client.create_stream(
                                StreamName=s,
                                ShardCount=config.dlq_shard_count,
                            ),
                        )
                        created_streams.append(dlq_stream)
                        logger.info("kinesis.dlq_stream_created", stream=dlq_stream)
                    except client.exceptions.ResourceInUseException:
                        pass

            # Create DynamoDB checkpoint table
            checkpoint = DynamoDBCheckpointStore(config)
            checkpoint.ensure_table()

            # Set up PG replication slot + publication
            from cdc_platform.sources.wal.slot_manager import SlotManager

            source = pipeline.source
            dsn = (
                f"host={source.host} port={source.port} "
                f"dbname={source.database} user={source.username} "
                f"password={source.password.get_secret_value()}"
            )
            slot_mgr = SlotManager(
                dsn=dsn,
                slot_name=self._platform.wal_reader.slot_name,
                publication_name=self._platform.wal_reader.publication_name,
            )
            await slot_mgr.ensure_publication(source.tables)
            await slot_mgr.ensure_slot()
        except Exception:
            logger.error(
                "kinesis.provision_failed, rolling back",
                pipeline_id=pipeline.pipeline_id,
            )
            await self._rollback_streams(client, loop, created_streams)
            raise

        return {
            "streams": created_streams,
            "checkpoint_table": config.checkpoint_table_name,
        }

    async def _rollback_streams(
        self, client: Any, loop: Any, streams: list[str]
    ) -> None:
        """Best-effort cleanup of streams created during a failed provision."""
        for stream in streams:
            try:
                await loop.run_in_executor(
                    None,
                    lambda s=stream: client.delete_stream(
                        StreamName=s,
                        EnforceConsumerDeletion=True,
                    ),
                )
                logger.info("kinesis.rollback_stream_deleted", stream=stream)
            except Exception as exc:
                logger.warning(
                    "kinesis.rollback_stream_delete_failed",
                    stream=stream,
                    error=str(exc),
                )

    async def teardown(self, pipeline: PipelineConfig) -> None:
        assert self._platform.kinesis is not None

        import asyncio

        import boto3

        config = self._platform.kinesis
        client = boto3.client("kinesis", region_name=config.region)
        loop = asyncio.get_running_loop()

        all_cdc_topics = topics_for_pipeline(pipeline, self._platform)
        cdc_topics = [t for t in all_cdc_topics if not t.endswith(".dlq")]

        for cdc_topic in cdc_topics:
            stream = kinesis_stream_name(cdc_topic)
            try:
                await loop.run_in_executor(
                    None,
                    lambda s=stream: client.delete_stream(
                        StreamName=s,
                        EnforceConsumerDeletion=True,
                    ),
                )
                logger.info("kinesis.stream_deleted", stream=stream)
            except Exception as exc:
                logger.warning(
                    "kinesis.stream_delete_failed",
                    stream=stream,
                    error=str(exc),
                )

            # Delete DLQ stream
            if self._platform.dlq.enabled:
                dlq_stream = kinesis_dlq_stream_name(
                    cdc_topic, config.dlq_stream_suffix
                )
                try:
                    await loop.run_in_executor(
                        None,
                        lambda s=dlq_stream: client.delete_stream(
                            StreamName=s,
                            EnforceConsumerDeletion=True,
                        ),
                    )
                    logger.info("kinesis.dlq_stream_deleted", stream=dlq_stream)
                except Exception as exc:
                    logger.warning(
                        "kinesis.dlq_stream_delete_failed",
                        stream=dlq_stream,
                        error=str(exc),
                    )
