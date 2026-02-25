"""Integration tests for WAL reader components.

Tests the WAL reader, slot manager, and publisher integration using
mocked PostgreSQL connections. Verifies the full pipeline from WAL
decode through publish.
"""

from __future__ import annotations

import struct
from typing import Any
from unittest.mock import patch

import pytest

from cdc_platform.sources.wal.decoder import PgOutputDecoder
from cdc_platform.sources.wal.reader import WalReader

# ---------------------------------------------------------------------------
# helpers — reuse WAL binary builders from decoder integration tests
# ---------------------------------------------------------------------------


def _begin(lsn: int = 100, ts_us: int = 1_000_000, xid: int = 1) -> bytes:
    return (
        b"B"
        + struct.pack("!Q", lsn)
        + struct.pack("!q", ts_us)
        + struct.pack("!I", xid)
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
    rel_id: int,
    namespace: str,
    table: str,
    columns: list[tuple[str, int]],
) -> bytes:
    data = b"R"
    data += struct.pack("!I", rel_id)
    data += namespace.encode() + b"\x00"
    data += table.encode() + b"\x00"
    data += b"\x00"  # replica identity
    data += struct.pack("!H", len(columns))
    for col_name, type_oid in columns:
        data += b"\x00"  # flags
        data += col_name.encode() + b"\x00"
        data += struct.pack("!I", type_oid)
        data += struct.pack("!I", 0)  # type modifier
    return data


def _tuple_data(values: list[str | None]) -> bytes:
    data = struct.pack("!H", len(values))
    for val in values:
        if val is None:
            data += b"n"
        else:
            encoded = val.encode("utf-8")
            data += b"t" + struct.pack("!I", len(encoded)) + encoded
    return data


def _insert(rel_id: int, values: list[str | None]) -> bytes:
    return b"I" + struct.pack("!I", rel_id) + b"N" + _tuple_data(values)


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


class MockWalPublisher:
    """In-memory publisher for testing."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []
        self.flushed = 0
        self.closed = False

    async def publish(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        ordering_key: str | None = None,
    ) -> None:
        self.published.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "ordering_key": ordering_key,
            }
        )

    async def flush(self) -> None:
        self.flushed += 1

    async def close(self) -> None:
        self.closed = True


@pytest.mark.integration
class TestWalReaderPublishIntegration:
    """Verify WAL reader batch publishing with a mock publisher."""

    @pytest.mark.asyncio
    async def test_publish_batch_serializes_changes(self):
        """_publish_batch should serialize WalChange objects to JSON and publish."""
        from cdc_platform.config.models import SourceConfig, WalReaderConfig

        publisher = MockWalPublisher()

        # Build a reader — we won't call start(), just test _publish_batch
        source = SourceConfig(database="testdb", tables=["public.users"])
        wal_config = WalReaderConfig()

        # Decode some changes
        decoder = PgOutputDecoder()
        decoder.decode(_begin(lsn=1000))
        decoder.decode(_relation(1, "public", "users", [("id", 23), ("name", 25)]))
        changes = decoder.decode(_insert(1, ["1", "Alice"]))
        decoder.decode(_commit(lsn=1000))

        # Create reader with mocked connection parts
        with patch("cdc_platform.sources.wal.reader.SlotManager"):
            reader = WalReader(
                source_config=source,
                wal_config=wal_config,
                publisher=publisher,
                topic_prefix="cdc",
            )

        await reader._publish_batch(changes)

        assert len(publisher.published) == 1
        msg = publisher.published[0]
        assert msg["topic"] == "cdc.public.users"
        assert msg["ordering_key"] == "public.users"
        assert b"Alice" in msg["value"]

    @pytest.mark.asyncio
    async def test_publish_batch_multiple_tables(self):
        """Publishing changes from multiple tables should use correct topics."""
        from cdc_platform.config.models import SourceConfig, WalReaderConfig

        publisher = MockWalPublisher()

        decoder = PgOutputDecoder()
        decoder.decode(_begin(lsn=2000))
        decoder.decode(_relation(1, "public", "users", [("id", 23), ("name", 25)]))
        decoder.decode(_relation(2, "public", "orders", [("id", 23), ("total", 1700)]))
        c1 = decoder.decode(_insert(1, ["1", "Alice"]))
        c2 = decoder.decode(_insert(2, ["100", "49.99"]))
        decoder.decode(_commit(lsn=2000))

        source = SourceConfig(
            database="testdb", tables=["public.users", "public.orders"]
        )

        with patch("cdc_platform.sources.wal.reader.SlotManager"):
            reader = WalReader(
                source_config=source,
                wal_config=WalReaderConfig(),
                publisher=publisher,
                topic_prefix="cdc",
            )

        await reader._publish_batch(c1 + c2)

        assert len(publisher.published) == 2
        topics = {m["topic"] for m in publisher.published}
        assert "cdc.public.users" in topics
        assert "cdc.public.orders" in topics

    @pytest.mark.asyncio
    async def test_stop_closes_publisher(self):
        """Stopping the reader should close the publisher."""
        from cdc_platform.config.models import SourceConfig, WalReaderConfig

        publisher = MockWalPublisher()

        with patch("cdc_platform.sources.wal.reader.SlotManager"):
            reader = WalReader(
                source_config=SourceConfig(database="testdb", tables=["public.t"]),
                wal_config=WalReaderConfig(),
                publisher=publisher,
                topic_prefix="cdc",
            )

        await reader.stop()
        assert publisher.closed


@pytest.mark.integration
class TestWalReaderSerializationFormat:
    """Verify the JSON serialization format of WAL changes."""

    @pytest.mark.asyncio
    async def test_serialized_value_has_expected_fields(self):
        """Serialized JSON should include operation, schema, table, lsn, timestamp."""
        import json

        from cdc_platform.config.models import SourceConfig, WalReaderConfig

        publisher = MockWalPublisher()

        decoder = PgOutputDecoder()
        decoder.decode(_begin(lsn=5000))
        decoder.decode(_relation(1, "public", "events", [("id", 23), ("type", 25)]))
        changes = decoder.decode(_insert(1, ["42", "click"]))
        decoder.decode(_commit(lsn=5000))

        with patch("cdc_platform.sources.wal.reader.SlotManager"):
            reader = WalReader(
                source_config=SourceConfig(database="testdb", tables=["public.events"]),
                wal_config=WalReaderConfig(),
                publisher=publisher,
                topic_prefix="cdc",
            )

        await reader._publish_batch(changes)

        payload = json.loads(publisher.published[0]["value"])
        assert payload["operation"] == "insert"
        assert payload["schema"] == "public"
        assert payload["table"] == "events"
        assert payload["lsn"] == 5000
        assert "timestamp" in payload
        assert payload["after"] == {"id": "42", "type": "click"}

    @pytest.mark.asyncio
    async def test_key_is_json_sorted(self):
        """Message key should be sorted JSON of the row data."""
        import json

        from cdc_platform.config.models import SourceConfig, WalReaderConfig

        publisher = MockWalPublisher()

        decoder = PgOutputDecoder()
        decoder.decode(_begin(lsn=6000))
        decoder.decode(_relation(1, "public", "t", [("b", 25), ("a", 25)]))
        changes = decoder.decode(_insert(1, ["val_b", "val_a"]))
        decoder.decode(_commit(lsn=6000))

        with patch("cdc_platform.sources.wal.reader.SlotManager"):
            reader = WalReader(
                source_config=SourceConfig(database="testdb", tables=["public.t"]),
                wal_config=WalReaderConfig(),
                publisher=publisher,
                topic_prefix="cdc",
            )

        await reader._publish_batch(changes)

        # Keys should be sorted
        key_str = publisher.published[0]["key"].decode("utf-8")
        assert list(json.loads(key_str).keys()) == ["a", "b"]
