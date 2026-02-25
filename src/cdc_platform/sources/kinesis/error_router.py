"""Kinesis ErrorRouter — publishes failed events to a DLQ stream."""

from __future__ import annotations

import json
import time
import traceback

import structlog

from cdc_platform.config.models import DLQConfig, KinesisConfig
from cdc_platform.sources.kinesis.naming import kinesis_dlq_stream_name

logger = structlog.get_logger()


class KinesisErrorRouter:
    """Routes failed events to a Kinesis DLQ stream."""

    def __init__(self, kinesis_config: KinesisConfig, dlq_config: DLQConfig) -> None:
        self._kinesis_config = kinesis_config
        self._dlq_config = dlq_config
        self._client = None

    def _get_client(self):  # noqa: ANN202
        if self._client is None:
            import boto3

            self._client = boto3.client(
                "kinesis", region_name=self._kinesis_config.region
            )
        return self._client

    def send(
        self,
        *,
        source_topic: str,
        partition: int,
        offset: int,
        key: bytes | None,
        value: bytes | None,
        error: Exception,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        """Send a failed event to the DLQ stream."""
        if not self._dlq_config.enabled:
            return

        client = self._get_client()
        dlq_stream = kinesis_dlq_stream_name(
            source_topic, self._kinesis_config.dlq_stream_suffix
        )

        headers: dict[str, str] = {}
        if self._dlq_config.include_headers:
            headers = {
                "dlq.source.topic": source_topic,
                "dlq.source.partition": str(partition),
                "dlq.source.offset": str(offset),
                "dlq.error.message": str(error),
                "dlq.error.type": type(error).__name__,
                "dlq.error.stacktrace": traceback.format_exc(),
                "dlq.timestamp": str(int(time.time() * 1000)),
            }
            if extra_headers:
                headers.update(extra_headers)

        # Wrap value + headers into a JSON envelope
        envelope = {
            "data": (value or b"").decode("utf-8", errors="replace"),
            "headers": headers,
        }
        data = json.dumps(envelope).encode("utf-8")

        try:
            client.put_record(
                StreamName=dlq_stream,
                Data=data,
                PartitionKey=f"{source_topic}-{partition}",
            )
            logger.warning(
                "kinesis_dlq.message_sent",
                stream=dlq_stream,
                source_topic=source_topic,
                partition=partition,
                offset=offset,
                error=str(error),
            )
        except Exception as dlq_exc:
            logger.error(
                "kinesis_dlq.write_failed",
                stream=dlq_stream,
                source_topic=source_topic,
                error=str(dlq_exc),
            )

    def flush(self, timeout: float = 30.0) -> None:
        """No-op — Kinesis PutRecord is synchronous."""
