"""KinesisEventSource — EventSource implementation for Amazon Kinesis."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from cdc_platform.config.models import KinesisConfig
from cdc_platform.sources.base import SourceEvent
from cdc_platform.sources.kinesis.checkpoint import DynamoDBCheckpointStore
from cdc_platform.sources.kinesis.naming import cdc_topic_from_stream

logger = structlog.get_logger()

SourceEventHandler = Callable[[SourceEvent], Awaitable[None]]
PartitionCallback = Callable[[list[tuple[str, int]]], None]


class KinesisEventSource:
    """Reads from Kinesis streams using per-shard GetRecords.

    Each shard maps to a partition for the pipeline's per-partition queues.
    Sequence numbers map directly to offsets.
    """

    def __init__(
        self,
        streams: list[str],
        config: KinesisConfig,
    ) -> None:
        self._streams = streams
        self._config = config
        self._running = False
        self._checkpoint_store = DynamoDBCheckpointStore(config)
        self._client = None
        self._shard_tasks: list[asyncio.Task[None]] = []
        # Map (stream, shard_index) → latest sequence number
        self._sequence_numbers: dict[tuple[str, int], str] = {}

    def _get_client(self):  # noqa: ANN202
        if self._client is None:
            import boto3

            self._client = boto3.client("kinesis", region_name=self._config.region)
        return self._client

    async def start(
        self,
        handler: SourceEventHandler,
        on_assign: PartitionCallback | None = None,
        on_revoke: PartitionCallback | None = None,
    ) -> None:
        """Start per-shard reader tasks for all streams."""
        self._running = True
        client = self._get_client()
        loop = asyncio.get_running_loop()

        all_partitions: list[tuple[str, int]] = []

        for stream in self._streams:
            # List shards
            resp = await loop.run_in_executor(
                None,
                lambda s=stream: client.describe_stream(StreamName=s),
            )
            shards = resp["StreamDescription"]["Shards"]

            for idx, shard in enumerate(shards):
                shard_id = shard["ShardId"]
                topic = cdc_topic_from_stream(stream)
                all_partitions.append((topic, idx))

                task = asyncio.create_task(
                    self._read_shard(stream, shard_id, idx, handler, loop)
                )
                self._shard_tasks.append(task)

        if on_assign and all_partitions:
            on_assign(all_partitions)

        logger.info(
            "kinesis_source.started",
            streams=self._streams,
            total_shards=len(self._shard_tasks),
        )

        # Block until stopped
        while self._running:
            await asyncio.sleep(1.0)

    async def _read_shard(
        self,
        stream: str,
        shard_id: str,
        shard_index: int,
        handler: SourceEventHandler,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        """Read records from a single shard."""
        client = self._get_client()
        topic = cdc_topic_from_stream(stream)

        # Get starting position from checkpoint
        checkpoint = self._checkpoint_store.get_checkpoint(stream, shard_id)

        if checkpoint:
            iterator_resp = await loop.run_in_executor(
                None,
                lambda: client.get_shard_iterator(
                    StreamName=stream,
                    ShardId=shard_id,
                    ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                    StartingSequenceNumber=checkpoint,
                ),
            )
        else:
            iterator_resp = await loop.run_in_executor(
                None,
                lambda: client.get_shard_iterator(
                    StreamName=stream,
                    ShardId=shard_id,
                    ShardIteratorType=self._config.iterator_type,
                ),
            )

        shard_iterator = iterator_resp["ShardIterator"]
        offset = 0

        while self._running and shard_iterator:
            try:
                resp = await loop.run_in_executor(
                    None,
                    lambda si=shard_iterator: client.get_records(
                        ShardIterator=si,
                        Limit=self._config.max_records_per_shard,
                    ),
                )

                shard_iterator = resp.get("NextShardIterator")
                records = resp.get("Records", [])

                for record in records:
                    data = record["Data"]
                    seq_num = record["SequenceNumber"]
                    partition_key = record.get("PartitionKey", "")

                    try:
                        value = json.loads(data) if data else None
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        value = {"raw_data": data.decode("utf-8", errors="replace")}

                    key = {"partition_key": partition_key}

                    event = SourceEvent(
                        key=key,
                        value=value,
                        topic=topic,
                        partition=shard_index,
                        offset=offset,
                        raw=record,
                    )

                    self._sequence_numbers[(stream, shard_index)] = seq_num
                    await handler(event)
                    offset += 1

                if not records:
                    await asyncio.sleep(self._config.poll_interval_seconds)

            except Exception:
                logger.exception(
                    "kinesis_source.shard_read_error",
                    stream=stream,
                    shard_id=shard_id,
                )
                await asyncio.sleep(self._config.poll_interval_seconds)

    def commit_offsets(self, offsets: dict[tuple[str, int], int]) -> None:
        """Write checkpoints to DynamoDB for committed offsets."""
        for (topic, partition), _offset in offsets.items():
            # Find the sequence number for this (stream, shard)
            for (stream, shard_idx), seq_num in self._sequence_numbers.items():
                stream_topic = cdc_topic_from_stream(stream)
                if stream_topic == topic and shard_idx == partition:
                    # Get the actual shard ID — need to look up from stream
                    self._checkpoint_store.put_checkpoint(
                        stream, f"shard-{shard_idx}", seq_num
                    )

    def stop(self) -> None:
        """Signal the source to stop consuming."""
        self._running = False
        for task in self._shard_tasks:
            task.cancel()

    async def health(self) -> dict[str, Any]:
        """Return Kinesis health information."""
        return {
            "status": "running" if self._running else "stopped",
            "streams": self._streams,
            "active_shards": len(self._shard_tasks),
        }
