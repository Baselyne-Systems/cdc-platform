"""KinesisWalPublisher — WalPublisher implementation for Amazon Kinesis."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import structlog

from cdc_platform.config.models import KinesisConfig
from cdc_platform.sources.kinesis.naming import kinesis_stream_name

logger = structlog.get_logger()

MAX_BATCH_SIZE = 500  # Kinesis PutRecords limit
MAX_RECORD_BYTES = 1_048_576  # 1 MiB per record


@dataclass(slots=True)
class _BufferedRecord:
    stream: str
    data: bytes
    partition_key: str


class KinesisWalPublisher:
    """Publishes WAL changes to Amazon Kinesis Data Streams.

    Implements the WalPublisher protocol.  Records are buffered during
    ``publish()`` and sent in batches of up to 500 via ``put_records()``
    during ``flush()``.
    """

    def __init__(self, config: KinesisConfig) -> None:
        self._config = config
        self._client = None
        self._buffer: list[_BufferedRecord] = []
        self._max_batch_size = MAX_BATCH_SIZE

    def _get_client(self):  # noqa: ANN202
        if self._client is None:
            import boto3

            self._client = boto3.client("kinesis", region_name=self._config.region)
        return self._client

    async def publish(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        ordering_key: str | None = None,
    ) -> None:
        """Buffer a record for batch publishing during flush()."""
        stream = kinesis_stream_name(topic)
        partition_key = ordering_key or key.decode("utf-8", errors="replace")[:256]
        self._buffer.append(_BufferedRecord(stream, value, partition_key))

    async def flush(self) -> None:
        """Send buffered records via put_records() in chunks."""
        if not self._buffer:
            return

        client = self._get_client()
        loop = asyncio.get_running_loop()

        # Group by stream name
        by_stream: dict[str, list[_BufferedRecord]] = {}
        for rec in self._buffer:
            by_stream.setdefault(rec.stream, []).append(rec)
        self._buffer.clear()

        for stream, records in by_stream.items():
            # Send in chunks of max_batch_size
            for i in range(0, len(records), self._max_batch_size):
                chunk = records[i : i + self._max_batch_size]
                entries = [
                    {"Data": r.data, "PartitionKey": r.partition_key} for r in chunk
                ]
                await self._put_records_with_retry(loop, client, stream, entries)

    async def _put_records_with_retry(
        self,
        loop: asyncio.AbstractEventLoop,
        client: object,
        stream: str,
        entries: list[dict],
        max_retries: int = 3,
    ) -> None:
        """Send a batch via put_records, retrying partial failures."""
        remaining = entries
        for attempt in range(max_retries + 1):
            response = await loop.run_in_executor(
                None,
                lambda r=remaining: client.put_records(  # type: ignore[union-attr]
                    StreamName=stream, Records=r
                ),
            )

            failed_count = response.get("FailedRecordCount", 0)
            if failed_count == 0:
                return

            # Collect failed records for retry
            failed = []
            for entry, result in zip(remaining, response["Records"], strict=True):
                if "ErrorCode" in result:
                    failed.append(entry)

            logger.warning(
                "kinesis.partial_failure",
                stream=stream,
                failed=failed_count,
                total=len(remaining),
                attempt=attempt + 1,
            )
            remaining = failed

            if attempt < max_retries:
                await asyncio.sleep(0.1 * (2**attempt))

        if remaining:
            logger.error(
                "kinesis.records_dropped",
                stream=stream,
                count=len(remaining),
            )

    async def close(self) -> None:
        """Release the boto3 client."""
        self._buffer.clear()
        self._client = None
