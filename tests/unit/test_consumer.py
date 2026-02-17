"""Unit tests for CDCConsumer.commit_offsets()."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from cdc_platform.streaming.consumer import CDCConsumer


def _make_consumer() -> CDCConsumer:
    """Create a CDCConsumer with mocked internals."""
    with patch("cdc_platform.streaming.consumer.Consumer"), \
         patch("cdc_platform.streaming.consumer.SchemaRegistryClient"), \
         patch("cdc_platform.streaming.consumer.AvroDeserializer"), \
         patch("cdc_platform.streaming.consumer.create_producer"):
        consumer = CDCConsumer(
            topics=["test-topic"],
            kafka_config=MagicMock(
                bootstrap_servers="localhost:9092",
                schema_registry_url="http://localhost:8081",
                group_id="test-group",
                auto_offset_reset="earliest",
            ),
            handler=lambda k, v, m: None,
        )
    return consumer


class TestCommitOffsets:
    def test_commit_offsets_builds_topic_partitions_with_plus_one(self):
        consumer = _make_consumer()
        mock_kafka = MagicMock()
        consumer._consumer = mock_kafka

        consumer.commit_offsets({("topic-a", 0): 10, ("topic-b", 1): 20})

        mock_kafka.commit.assert_called_once()
        call_kwargs = mock_kafka.commit.call_args.kwargs
        assert call_kwargs["asynchronous"] is False

        tps = call_kwargs["offsets"]
        tp_dict = {(tp.topic, tp.partition): tp.offset for tp in tps}
        assert tp_dict[("topic-a", 0)] == 11  # offset + 1
        assert tp_dict[("topic-b", 1)] == 21  # offset + 1

    def test_commit_offsets_empty_is_noop(self):
        consumer = _make_consumer()
        mock_kafka = MagicMock()
        consumer._consumer = mock_kafka

        consumer.commit_offsets({})

        mock_kafka.commit.assert_not_called()

    def test_commit_offsets_synchronous(self):
        consumer = _make_consumer()
        mock_kafka = MagicMock()
        consumer._consumer = mock_kafka

        consumer.commit_offsets({("t", 0): 5})

        call_kwargs = mock_kafka.commit.call_args.kwargs
        assert call_kwargs["asynchronous"] is False
