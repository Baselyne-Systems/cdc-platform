"""KinesisWalPublisher — WalPublisher implementation for Amazon Kinesis."""

from __future__ import annotations

import structlog

from cdc_platform.config.models import KinesisConfig
from cdc_platform.sources.kinesis.naming import kinesis_stream_name

logger = structlog.get_logger()


class KinesisWalPublisher:
    """Publishes WAL changes to Amazon Kinesis Data Streams.

    Implements the WalPublisher protocol.
    """

    def __init__(self, config: KinesisConfig) -> None:
        self._config = config
        self._client = None

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
        """Publish a record to a Kinesis stream."""
        import asyncio

        client = self._get_client()
        stream = kinesis_stream_name(topic)
        partition_key = ordering_key or key.decode("utf-8", errors="replace")[:256]

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: client.put_record(
                StreamName=stream,
                Data=value,
                PartitionKey=partition_key,
            ),
        )

    async def flush(self) -> None:
        """No-op — Kinesis PutRecord is synchronous."""

    async def close(self) -> None:
        """Release the boto3 client."""
        self._client = None
