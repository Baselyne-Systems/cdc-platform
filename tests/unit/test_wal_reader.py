"""Unit tests for WAL reader with mocked PG connection."""

from __future__ import annotations

import json
from datetime import UTC

import pytest

from cdc_platform.config.models import SourceConfig, WalReaderConfig
from cdc_platform.sources.wal.decoder import WalChange
from cdc_platform.sources.wal.reader import WalReader


class MockWalPublisher:
    """Mock WalPublisher for testing."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, bytes, str | None]] = []
        self.flushed = 0
        self.closed = False

    async def publish(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        ordering_key: str | None = None,
    ) -> None:
        self.published.append((topic, key, value, ordering_key))

    async def flush(self) -> None:
        self.flushed += 1

    async def close(self) -> None:
        self.closed = True


class TestWalReader:
    def test_build_key(self):
        publisher = MockWalPublisher()
        reader = WalReader(
            source_config=SourceConfig(database="testdb", tables=["public.t"]),
            wal_config=WalReaderConfig(),
            publisher=publisher,
            topic_prefix="cdc",
        )

        from datetime import datetime

        change = WalChange(
            operation="insert",
            schema="public",
            table="users",
            before=None,
            after={"id": "1", "name": "Alice"},
            lsn=100,
            timestamp=datetime.now(tz=UTC),
        )

        key = reader._build_key(change)
        parsed = json.loads(key)
        assert parsed["id"] == "1"

    def test_serialize_change(self):
        publisher = MockWalPublisher()
        reader = WalReader(
            source_config=SourceConfig(database="testdb", tables=["public.t"]),
            wal_config=WalReaderConfig(),
            publisher=publisher,
            topic_prefix="cdc",
        )

        from datetime import datetime

        change = WalChange(
            operation="insert",
            schema="public",
            table="users",
            before=None,
            after={"id": "1", "name": "Alice"},
            lsn=100,
            timestamp=datetime(2025, 1, 1, tzinfo=UTC),
        )

        data = reader._serialize_change(change)
        parsed = json.loads(data)
        assert parsed["operation"] == "insert"
        assert parsed["schema"] == "public"
        assert parsed["table"] == "users"
        assert parsed["after"]["id"] == "1"
        assert "before" not in parsed

    def test_serialize_change_with_before(self):
        publisher = MockWalPublisher()
        reader = WalReader(
            source_config=SourceConfig(database="testdb", tables=["public.t"]),
            wal_config=WalReaderConfig(),
            publisher=publisher,
            topic_prefix="cdc",
        )

        from datetime import datetime

        change = WalChange(
            operation="update",
            schema="public",
            table="users",
            before={"id": "1", "name": "Old"},
            after={"id": "1", "name": "New"},
            lsn=200,
            timestamp=datetime(2025, 1, 1, tzinfo=UTC),
        )

        data = reader._serialize_change(change)
        parsed = json.loads(data)
        assert parsed["before"]["name"] == "Old"
        assert parsed["after"]["name"] == "New"

    @pytest.mark.asyncio
    async def test_publish_batch(self):
        publisher = MockWalPublisher()
        reader = WalReader(
            source_config=SourceConfig(database="testdb", tables=["public.t"]),
            wal_config=WalReaderConfig(),
            publisher=publisher,
            topic_prefix="cdc",
        )

        from datetime import datetime

        changes = [
            WalChange(
                operation="insert",
                schema="public",
                table="users",
                before=None,
                after={"id": str(i)},
                lsn=100 + i,
                timestamp=datetime(2025, 1, 1, tzinfo=UTC),
            )
            for i in range(3)
        ]

        await reader._publish_batch(changes)

        assert len(publisher.published) == 3
        for topic, _key, _value, ordering_key in publisher.published:
            assert topic == "cdc.public.users"
            assert ordering_key == "public.users"

    @pytest.mark.asyncio
    async def test_stop(self):
        publisher = MockWalPublisher()
        reader = WalReader(
            source_config=SourceConfig(database="testdb", tables=["public.t"]),
            wal_config=WalReaderConfig(),
            publisher=publisher,
            topic_prefix="cdc",
        )

        await reader.stop()
        assert reader._running is False
        assert publisher.closed is True
