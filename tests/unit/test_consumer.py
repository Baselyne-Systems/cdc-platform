import asyncio
from unittest.mock import MagicMock, patch

from cdc_platform.streaming.consumer import CDCConsumer


def _make_kafka_config(**overrides: object) -> MagicMock:
    """Create a mock KafkaConfig with defaults + overrides."""
    defaults = {
        "bootstrap_servers": "localhost:9092",
        "schema_registry_url": "http://localhost:8081",
        "group_id": "test-group",
        "auto_offset_reset": "earliest",
        "session_timeout_ms": 45000,
        "max_poll_interval_ms": 300000,
        "fetch_min_bytes": 1,
        "fetch_max_wait_ms": 500,
        "poll_batch_size": 1,
        "deser_pool_size": 1,
        "commit_interval_seconds": 0.0,
    }
    defaults.update(overrides)
    return MagicMock(**defaults)


def _make_consumer(**kafka_overrides: object) -> CDCConsumer:
    """Create a CDCConsumer with mocked internals."""
    with (
        patch("cdc_platform.streaming.consumer.Consumer"),
        patch("cdc_platform.streaming.consumer.SchemaRegistryClient"),
        patch("cdc_platform.streaming.consumer.AvroDeserializer"),
        patch("cdc_platform.streaming.consumer.create_producer"),
    ):
        consumer = CDCConsumer(
            topics=["test-topic"],
            kafka_config=_make_kafka_config(**kafka_overrides),
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
                kafka_config=_make_kafka_config(),
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
                kafka_config=_make_kafka_config(),
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


class TestHandleError:
    def test_commits_offset_after_successful_dlq_send(self):
        consumer = _make_consumer()
        mock_kafka = MagicMock()
        consumer._consumer = mock_kafka
        consumer._dlq = MagicMock()

        msg = MagicMock()
        msg.topic.return_value = "t"
        msg.partition.return_value = 0
        msg.offset.return_value = 42

        consumer._handle_error(msg, ValueError("bad data"))

        consumer._dlq.send.assert_called_once()
        mock_kafka.commit.assert_called_once_with(message=msg)

    def test_does_not_commit_offset_when_dlq_fails(self):
        """If DLQ send fails, offset must NOT be committed (prevents data loss)."""
        consumer = _make_consumer()
        mock_kafka = MagicMock()
        consumer._consumer = mock_kafka
        consumer._dlq = MagicMock()
        consumer._dlq.send.side_effect = RuntimeError("DLQ broker down")

        msg = MagicMock()
        msg.topic.return_value = "t"
        msg.partition.return_value = 0
        msg.offset.return_value = 42

        consumer._handle_error(msg, ValueError("bad data"))

        mock_kafka.commit.assert_not_called()

    def test_commits_offset_when_no_dlq_configured(self):
        """When DLQ is disabled, commit the offset to avoid infinite retry."""
        consumer = _make_consumer()
        mock_kafka = MagicMock()
        consumer._consumer = mock_kafka
        consumer._dlq = None

        msg = MagicMock()
        msg.topic.return_value = "t"
        msg.partition.return_value = 0
        msg.offset.return_value = 42

        consumer._handle_error(msg, ValueError("bad data"))

        mock_kafka.commit.assert_called_once_with(message=msg)


class TestBatchPolling:
    def test_batch_size_1_uses_legacy_path(self):
        """poll_batch_size=1 keeps single-poll behavior."""
        consumer = _make_consumer(poll_batch_size=1)
        assert consumer._poll_batch_size == 1

    def test_batch_size_gt1_stored(self):
        consumer = _make_consumer(poll_batch_size=500)
        assert consumer._poll_batch_size == 500

    def test_deser_executor_created_when_pool_gt1(self):
        consumer = _make_consumer(deser_pool_size=4)
        assert consumer._deser_executor is not None
        consumer._deser_executor.shutdown(wait=False)

    def test_deser_executor_none_when_pool_eq1(self):
        consumer = _make_consumer(deser_pool_size=1)
        assert consumer._deser_executor is None

    def test_deserialize_batch(self):
        consumer = _make_consumer()
        msg1 = MagicMock()
        msg1.topic.return_value = "t"
        msg1.key.return_value = b"k1"
        msg1.value.return_value = b"v1"
        msg2 = MagicMock()
        msg2.topic.return_value = "t"
        msg2.key.return_value = b"k2"
        msg2.value.return_value = b"v2"

        # Mock the deserializers to return passthrough dicts
        consumer._key_deser = MagicMock(side_effect=[{"k": 1}, {"k": 2}])
        consumer._value_deser = MagicMock(side_effect=[{"v": 1}, {"v": 2}])

        results = consumer._deserialize_batch([msg1, msg2])
        assert len(results) == 2
        assert results[0][0] == {"k": 1}
        assert results[1][0] == {"k": 2}


class TestAsyncCommit:
    def test_commit_synchronous_by_default(self):
        consumer = _make_consumer(commit_interval_seconds=0.0)
        mock_kafka = MagicMock()
        consumer._consumer = mock_kafka

        consumer.commit_offsets({("t", 0): 5})

        call_kwargs = mock_kafka.commit.call_args.kwargs
        assert call_kwargs["asynchronous"] is False

    def test_commit_async_when_interval_gt0(self):
        consumer = _make_consumer(commit_interval_seconds=5.0)
        mock_kafka = MagicMock()
        consumer._consumer = mock_kafka

        consumer.commit_offsets({("t", 0): 5})

        call_kwargs = mock_kafka.commit.call_args.kwargs
        assert call_kwargs["asynchronous"] is True
