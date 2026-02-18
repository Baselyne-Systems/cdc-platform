import asyncio
from unittest.mock import MagicMock, patch

from cdc_platform.streaming.consumer import CDCConsumer


def _make_consumer() -> CDCConsumer:
    """Create a CDCConsumer with mocked internals."""
    with (
        patch("cdc_platform.streaming.consumer.Consumer"),
        patch("cdc_platform.streaming.consumer.SchemaRegistryClient"),
        patch("cdc_platform.streaming.consumer.AvroDeserializer"),
        patch("cdc_platform.streaming.consumer.create_producer"),
    ):
        consumer = CDCConsumer(
            topics=["test-topic"],
            kafka_config=MagicMock(
                bootstrap_servers="localhost:9092",
                schema_registry_url="http://localhost:8081",
                group_id="test-group",
                auto_offset_reset="earliest",
            ),
            handler=MagicMock(return_value=asyncio.Future()),
        )
        # Setup the mock to be awaitable
        consumer._handler.return_value.set_result(None)
    return consumer


class TestRebalanceCallbacks:
    async def test_on_assign_callback_wired(self):
        """on_assign callback is stored and invoked on partition assignment."""
        assigned: list[tuple[str, int]] = []

        def on_assign(partitions: list[tuple[str, int]]) -> None:
            assigned.extend(partitions)

        with (
            patch("cdc_platform.streaming.consumer.Consumer"),
            patch("cdc_platform.streaming.consumer.SchemaRegistryClient"),
            patch("cdc_platform.streaming.consumer.AvroDeserializer"),
            patch("cdc_platform.streaming.consumer.create_producer"),
        ):
            consumer = CDCConsumer(
                topics=["test-topic"],
                kafka_config=MagicMock(
                    bootstrap_servers="localhost:9092",
                    schema_registry_url="http://localhost:8081",
                    group_id="test-group",
                    auto_offset_reset="earliest",
                ),
                handler=MagicMock(return_value=asyncio.Future()),
                on_assign=on_assign,
            )

        # Set the loop so call_soon_threadsafe works
        consumer._loop = asyncio.get_running_loop()

        # Simulate Kafka calling the assign handler from a thread
        mock_tp = MagicMock()
        mock_tp.topic = "test-topic"
        mock_tp.partition = 0
        consumer._handle_assign(None, [mock_tp])

        # Yield control so the scheduled callback executes
        await asyncio.sleep(0)

        assert assigned == [("test-topic", 0)]

    async def test_on_revoke_callback_wired(self):
        """on_revoke callback is stored and invoked on partition revocation."""
        revoked: list[tuple[str, int]] = []

        def on_revoke(partitions: list[tuple[str, int]]) -> None:
            revoked.extend(partitions)

        with (
            patch("cdc_platform.streaming.consumer.Consumer"),
            patch("cdc_platform.streaming.consumer.SchemaRegistryClient"),
            patch("cdc_platform.streaming.consumer.AvroDeserializer"),
            patch("cdc_platform.streaming.consumer.create_producer"),
        ):
            consumer = CDCConsumer(
                topics=["test-topic"],
                kafka_config=MagicMock(
                    bootstrap_servers="localhost:9092",
                    schema_registry_url="http://localhost:8081",
                    group_id="test-group",
                    auto_offset_reset="earliest",
                ),
                handler=MagicMock(return_value=asyncio.Future()),
                on_revoke=on_revoke,
            )

        consumer._loop = asyncio.get_running_loop()

        mock_tp = MagicMock()
        mock_tp.topic = "test-topic"
        mock_tp.partition = 2
        consumer._handle_revoke(None, [mock_tp])

        await asyncio.sleep(0)

        assert revoked == [("test-topic", 2)]

    def test_no_callback_is_safe(self):
        """When no callbacks are provided, handle_assign/revoke are noops."""
        consumer = _make_consumer()
        mock_tp = MagicMock()
        mock_tp.topic = "t"
        mock_tp.partition = 0
        # Should not raise
        consumer._handle_assign(None, [mock_tp])
        consumer._handle_revoke(None, [mock_tp])


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
