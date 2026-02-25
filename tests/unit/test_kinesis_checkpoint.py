"""Unit tests for DynamoDB checkpoint store."""

from __future__ import annotations

from unittest.mock import MagicMock

from cdc_platform.config.models import KinesisConfig
from cdc_platform.sources.kinesis.checkpoint import DynamoDBCheckpointStore


class TestDynamoDBCheckpointStore:
    def test_get_checkpoint_returns_none_when_not_found(self):
        store = DynamoDBCheckpointStore(KinesisConfig())
        mock_client = MagicMock()
        mock_client.get_item.return_value = {}
        store._client = mock_client

        result = store.get_checkpoint("stream-1", "shard-0")
        assert result is None

    def test_get_checkpoint_returns_sequence_number(self):
        store = DynamoDBCheckpointStore(KinesisConfig())
        mock_client = MagicMock()
        mock_client.get_item.return_value = {
            "Item": {"sequence_number": {"S": "12345"}}
        }
        store._client = mock_client

        result = store.get_checkpoint("stream-1", "shard-0")
        assert result == "12345"

    def test_put_checkpoint(self):
        store = DynamoDBCheckpointStore(KinesisConfig())
        mock_client = MagicMock()
        store._client = mock_client

        store.put_checkpoint("stream-1", "shard-0", "67890")
        mock_client.put_item.assert_called_once()
        call_args = mock_client.put_item.call_args
        item = call_args.kwargs["Item"]
        assert item["stream_name"]["S"] == "stream-1"
        assert item["shard_id"]["S"] == "shard-0"
        assert item["sequence_number"]["S"] == "67890"

    def test_ensure_table_exists(self):
        store = DynamoDBCheckpointStore(KinesisConfig())
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {"TableName": "cdc-kinesis-checkpoints"}
        }
        store._client = mock_client

        store.ensure_table()
        mock_client.describe_table.assert_called_once()
        mock_client.create_table.assert_not_called()

    def test_ensure_table_creates(self):
        store = DynamoDBCheckpointStore(KinesisConfig())
        mock_client = MagicMock()

        # Simulate ResourceNotFoundException
        mock_client.exceptions.ResourceNotFoundException = type(
            "ResourceNotFoundException", (Exception,), {}
        )
        mock_client.describe_table.side_effect = (
            mock_client.exceptions.ResourceNotFoundException()
        )
        mock_waiter = MagicMock()
        mock_client.get_waiter.return_value = mock_waiter
        store._client = mock_client

        store.ensure_table()
        mock_client.create_table.assert_called_once()
        mock_waiter.wait.assert_called_once()
