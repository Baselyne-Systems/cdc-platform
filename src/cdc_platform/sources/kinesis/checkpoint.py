"""DynamoDB checkpoint store for Kinesis shard sequence numbers."""

from __future__ import annotations

import structlog

from cdc_platform.config.models import KinesisConfig

logger = structlog.get_logger()


class DynamoDBCheckpointStore:
    """Stores Kinesis shard checkpoints in a DynamoDB table.

    Table schema:
        - Partition key: ``stream_name`` (S)
        - Sort key: ``shard_id`` (S)
        - Attribute: ``sequence_number`` (S)
    """

    def __init__(self, config: KinesisConfig) -> None:
        self._config = config
        self._table_name = config.checkpoint_table_name
        self._client = None

    def _get_client(self):  # noqa: ANN202
        if self._client is None:
            import boto3

            self._client = boto3.client("dynamodb", region_name=self._config.region)
        return self._client

    def get_checkpoint(self, stream_name: str, shard_id: str) -> str | None:
        """Get the last checkpointed sequence number for a shard."""
        client = self._get_client()
        try:
            resp = client.get_item(
                TableName=self._table_name,
                Key={
                    "stream_name": {"S": stream_name},
                    "shard_id": {"S": shard_id},
                },
            )
            item = resp.get("Item")
            if item and "sequence_number" in item:
                return item["sequence_number"]["S"]
        except Exception:
            logger.exception(
                "checkpoint.get_failed",
                stream=stream_name,
                shard=shard_id,
            )
        return None

    def put_checkpoint(
        self, stream_name: str, shard_id: str, sequence_number: str
    ) -> None:
        """Store a checkpoint for a shard."""
        client = self._get_client()
        try:
            client.put_item(
                TableName=self._table_name,
                Item={
                    "stream_name": {"S": stream_name},
                    "shard_id": {"S": shard_id},
                    "sequence_number": {"S": sequence_number},
                },
            )
        except Exception:
            logger.exception(
                "checkpoint.put_failed",
                stream=stream_name,
                shard=shard_id,
                sequence_number=sequence_number,
            )

    def ensure_table(self) -> None:
        """Create the checkpoint DynamoDB table if it doesn't exist."""
        client = self._get_client()
        try:
            client.describe_table(TableName=self._table_name)
            logger.info("checkpoint.table_exists", table=self._table_name)
        except client.exceptions.ResourceNotFoundException:
            client.create_table(
                TableName=self._table_name,
                KeySchema=[
                    {"AttributeName": "stream_name", "KeyType": "HASH"},
                    {"AttributeName": "shard_id", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "stream_name", "AttributeType": "S"},
                    {"AttributeName": "shard_id", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            logger.info("checkpoint.table_created", table=self._table_name)
            # Wait for table to become active
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=self._table_name)
