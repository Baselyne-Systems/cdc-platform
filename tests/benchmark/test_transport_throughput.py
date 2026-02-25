"""Benchmark tests for transport-layer throughput.

Measures throughput for Pub/Sub and Kinesis transport components using
mocked cloud clients, focusing on serialization, hashing, naming, and
publisher overhead.
"""

from __future__ import annotations

import json
import struct
import time
from unittest.mock import MagicMock

import pytest
import structlog

from cdc_platform.sources.pubsub.naming import (
    cdc_topic_from_pubsub,
    pubsub_topic_name,
)
from cdc_platform.sources.pubsub.source import _hash_to_partition
from cdc_platform.sources.wal.decoder import PgOutputDecoder

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# WAL message builders
# ---------------------------------------------------------------------------


def _begin(lsn: int = 100) -> bytes:
    return (
        b"B"
        + struct.pack("!Q", lsn)
        + struct.pack("!q", 1_000_000)
        + struct.pack("!I", 1)
    )


def _commit(lsn: int = 100) -> bytes:
    return (
        b"C"
        + b"\x00"
        + struct.pack("!Q", lsn)
        + struct.pack("!Q", lsn)
        + struct.pack("!q", 0)
    )


def _relation(
    rel_id: int, namespace: str, table: str, columns: list[tuple[str, int]]
) -> bytes:
    data = b"R" + struct.pack("!I", rel_id)
    data += namespace.encode() + b"\x00" + table.encode() + b"\x00" + b"\x00"
    data += struct.pack("!H", len(columns))
    for col_name, type_oid in columns:
        data += b"\x00" + col_name.encode() + b"\x00"
        data += struct.pack("!I", type_oid) + struct.pack("!I", 0)
    return data


def _insert(rel_id: int, values: list[str | None]) -> bytes:
    data = struct.pack("!H", len(values))
    for val in values:
        if val is None:
            data += b"n"
        else:
            encoded = val.encode("utf-8")
            data += b"t" + struct.pack("!I", len(encoded)) + encoded
    return b"I" + struct.pack("!I", rel_id) + b"N" + data


# ---------------------------------------------------------------------------
# benchmarks
# ---------------------------------------------------------------------------


@pytest.mark.benchmark
class TestVirtualPartitionHashThroughput:
    """Benchmark the virtual partition hashing used by Pub/Sub."""

    @pytest.mark.parametrize(
        "key_count",
        [100_000, 500_000, 1_000_000],
        ids=["100K", "500K", "1M"],
    )
    def test_hash_to_partition_throughput(self, key_count: int):
        """Measure hashing throughput for ordering key → partition mapping."""
        keys = [f"public.table_{i % 100}" for i in range(key_count)]

        start = time.perf_counter()
        for key in keys:
            _hash_to_partition(key)
        elapsed = time.perf_counter() - start

        throughput = key_count / elapsed if elapsed > 0 else 0

        logger.info(
            "benchmark.hash_partition",
            keys=key_count,
            elapsed_seconds=round(elapsed, 3),
            hashes_per_sec=round(throughput),
        )

        # MD5 hashing should be very fast
        assert throughput > 500_000

    def test_partition_distribution_uniformity(self):
        """Verify hash distribution across virtual partitions."""
        partitions = [0] * 16
        key_count = 100_000

        for i in range(key_count):
            p = _hash_to_partition(f"schema.table_{i}")
            partitions[p] += 1

        # Each partition should get roughly 100K/16 = 6250 keys
        expected = key_count / 16
        max_deviation = max(abs(c - expected) / expected for c in partitions)

        logger.info(
            "benchmark.hash_distribution",
            min_count=min(partitions),
            max_count=max(partitions),
            expected=round(expected),
            max_deviation_pct=round(max_deviation * 100, 1),
        )

        # Allow up to 20% deviation from uniform
        assert max_deviation < 0.20


@pytest.mark.benchmark
class TestNamingConventionThroughput:
    """Benchmark topic/stream naming operations."""

    def test_pubsub_naming_roundtrip_throughput(self):
        """Measure naming convention roundtrip throughput."""
        count = 100_000
        topics = [f"cdc.schema_{i % 50}.table_{i % 200}" for i in range(count)]

        start = time.perf_counter()
        for topic in topics:
            pubsub_name = pubsub_topic_name("project", topic)
            cdc_topic_from_pubsub(pubsub_name)
        elapsed = time.perf_counter() - start

        throughput = count / elapsed if elapsed > 0 else 0

        logger.info(
            "benchmark.naming_roundtrip",
            count=count,
            elapsed_seconds=round(elapsed, 3),
            ops_per_sec=round(throughput),
        )

        assert throughput > 100_000


@pytest.mark.benchmark
class TestWalPublisherThroughput:
    """Benchmark WAL publisher serialize + publish path."""

    @pytest.mark.asyncio
    async def test_pubsub_publisher_throughput(self):
        """Measure PubSub WAL publisher throughput with mocked client."""
        from cdc_platform.config.models import PubSubConfig
        from cdc_platform.sources.pubsub.publisher import PubSubWalPublisher

        mock_publisher = MagicMock()
        mock_future = MagicMock()
        mock_future.result.return_value = "msg-id"
        mock_publisher.publish.return_value = mock_future

        config = PubSubConfig(project_id="bench-project")
        publisher = PubSubWalPublisher(config)
        publisher._publisher = mock_publisher

        msg_count = 10_000
        value = json.dumps(
            {
                "operation": "insert",
                "schema": "public",
                "table": "customers",
                "after": {"id": "1", "email": "test@example.com"},
            }
        ).encode()
        key = b'{"id": "1"}'

        start = time.perf_counter()
        for _i in range(msg_count):
            await publisher.publish(
                topic="cdc.public.customers",
                key=key,
                value=value,
                ordering_key="public.customers",
            )
        elapsed = time.perf_counter() - start

        throughput = msg_count / elapsed if elapsed > 0 else 0

        logger.info(
            "benchmark.pubsub_publisher",
            messages=msg_count,
            elapsed_seconds=round(elapsed, 3),
            msgs_per_sec=round(throughput),
        )

        assert mock_publisher.publish.call_count == msg_count
        assert throughput > 5_000

    @pytest.mark.asyncio
    async def test_kinesis_publisher_throughput(self):
        """Measure Kinesis WAL publisher throughput with mocked client."""
        from cdc_platform.config.models import KinesisConfig
        from cdc_platform.sources.kinesis.publisher import KinesisWalPublisher

        mock_client = MagicMock()
        mock_client.put_record.return_value = {
            "ShardId": "shardId-000",
            "SequenceNumber": "12345",
        }

        config = KinesisConfig(region="us-east-1")
        publisher = KinesisWalPublisher(config)
        publisher._client = mock_client

        msg_count = 5_000
        value = json.dumps(
            {
                "operation": "insert",
                "schema": "public",
                "table": "orders",
                "after": {"id": "1", "total": "49.99"},
            }
        ).encode()
        key = b'{"id": "1"}'

        start = time.perf_counter()
        for _i in range(msg_count):
            await publisher.publish(
                topic="cdc.public.orders",
                key=key,
                value=value,
                ordering_key="public.orders",
            )
        elapsed = time.perf_counter() - start

        throughput = msg_count / elapsed if elapsed > 0 else 0

        logger.info(
            "benchmark.kinesis_publisher",
            messages=msg_count,
            elapsed_seconds=round(elapsed, 3),
            msgs_per_sec=round(throughput),
        )

        assert throughput > 1_000


@pytest.mark.benchmark
class TestJsonSerializationThroughput:
    """Benchmark JSON serialization as used in WAL reader publish path."""

    @pytest.mark.parametrize(
        "payload_size",
        ["small", "medium", "large"],
    )
    def test_change_serialization_throughput(self, payload_size: str):
        """Measure JSON serialization throughput for different payload sizes."""
        msg_count = 50_000

        if payload_size == "small":
            payload = {"id": "1", "status": "active"}
        elif payload_size == "medium":
            payload = {f"col_{i}": f"value_{i}" for i in range(20)}
        else:
            payload = {f"col_{i}": f"{'x' * 100}" for i in range(50)}

        envelope = {
            "operation": "insert",
            "schema": "public",
            "table": "test",
            "lsn": 12345,
            "timestamp": "2024-01-01T00:00:00",
            "after": payload,
        }

        start = time.perf_counter()
        for _ in range(msg_count):
            json.dumps(envelope, default=str).encode("utf-8")
        elapsed = time.perf_counter() - start

        throughput = msg_count / elapsed if elapsed > 0 else 0
        avg_size = len(json.dumps(envelope, default=str))

        logger.info(
            "benchmark.json_serialization",
            payload_size=payload_size,
            messages=msg_count,
            avg_bytes=avg_size,
            elapsed_seconds=round(elapsed, 3),
            msgs_per_sec=round(throughput),
        )

        assert throughput > 10_000


@pytest.mark.benchmark
class TestEndToEndDecodePlusPipeline:
    """Benchmark full decode → serialize → publish pipeline."""

    @pytest.mark.asyncio
    async def test_decode_serialize_publish_throughput(self):
        """Measure full WAL decode → JSON serialize → mock publish."""
        from unittest.mock import patch

        from cdc_platform.config.models import SourceConfig, WalReaderConfig
        from cdc_platform.sources.wal.reader import WalReader

        # Mock publisher that counts
        publish_count = 0

        class FastMockPublisher:
            async def publish(self, **kwargs: object) -> None:
                nonlocal publish_count
                publish_count += 1

            async def flush(self) -> None:
                pass

            async def close(self) -> None:
                pass

        publisher = FastMockPublisher()

        with patch("cdc_platform.sources.wal.reader.SlotManager"):
            reader = WalReader(
                source_config=SourceConfig(
                    database="testdb", tables=["public.customers"]
                ),
                wal_config=WalReaderConfig(),
                publisher=publisher,
                topic_prefix="cdc",
            )

        # Build WAL messages
        decoder = PgOutputDecoder()
        msg_count = 10_000

        decoder.decode(
            _relation(
                1,
                "public",
                "customers",
                [("id", 23), ("email", 25), ("name", 25)],
            )
        )
        decoder.decode(_begin(lsn=1000))

        all_changes = []
        for i in range(msg_count):
            changes = decoder.decode(
                _insert(1, [str(i), f"user{i}@test.com", f"User {i}"])
            )
            all_changes.extend(changes)
        decoder.decode(_commit(lsn=1000))

        # Measure publish path
        start = time.perf_counter()
        await reader._publish_batch(all_changes)
        elapsed = time.perf_counter() - start

        throughput = publish_count / elapsed if elapsed > 0 else 0

        logger.info(
            "benchmark.e2e_decode_publish",
            messages=msg_count,
            published=publish_count,
            elapsed_seconds=round(elapsed, 3),
            msgs_per_sec=round(throughput),
        )

        assert publish_count == msg_count
        assert throughput > 5_000
