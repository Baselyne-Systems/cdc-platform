"""Core WAL reader — async logical replication stream consumer.

Connects to PostgreSQL via the streaming replication protocol, decodes
pgoutput messages, serializes changes to JSON, and publishes them via
a WalPublisher implementation.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import structlog

from cdc_platform.config.models import SourceConfig, WalReaderConfig
from cdc_platform.sources.wal.decoder import PgOutputDecoder, WalChange
from cdc_platform.sources.wal.publisher import WalPublisher
from cdc_platform.sources.wal.slot_manager import SlotManager

logger = structlog.get_logger()


class WalReader:
    """Reads PostgreSQL WAL via logical replication and publishes changes.

    Lifecycle:
        1. Ensure publication + slot exist (via SlotManager)
        2. Open streaming replication connection
        3. Decode pgoutput messages
        4. Serialize to JSON and publish via WalPublisher
        5. Periodically confirm LSN to PostgreSQL
    """

    def __init__(
        self,
        source_config: SourceConfig,
        wal_config: WalReaderConfig,
        publisher: WalPublisher,
        topic_prefix: str,
    ) -> None:
        self._source = source_config
        self._wal_config = wal_config
        self._publisher = publisher
        self._topic_prefix = topic_prefix
        self._running = False
        self._decoder = PgOutputDecoder()

        dsn = (
            f"host={source_config.host} port={source_config.port} "
            f"dbname={source_config.database} user={source_config.username} "
            f"password={source_config.password.get_secret_value()}"
        )
        self._slot_manager = SlotManager(
            dsn=dsn,
            slot_name=wal_config.slot_name,
            publication_name=wal_config.publication_name,
        )
        self._dsn = dsn

    async def start(self) -> None:
        """Start the WAL reader loop."""
        self._running = True

        # Ensure publication + slot
        await self._slot_manager.ensure_publication(self._source.tables)
        await self._slot_manager.ensure_slot()

        logger.info(
            "wal_reader.starting",
            slot=self._wal_config.slot_name,
            publication=self._wal_config.publication_name,
        )

        await self._stream_changes()

    async def _stream_changes(self) -> None:
        """Open a replication connection and stream WAL changes."""
        import psycopg

        conn = await psycopg.AsyncConnection.connect(
            self._dsn,
            autocommit=True,
            connection_class=psycopg.AsyncConnection,
        )

        try:
            # Start replication using the streaming replication protocol
            cursor = conn.cursor()
            slot = self._wal_config.slot_name
            pub = self._wal_config.publication_name
            await cursor.execute(
                f"START_REPLICATION SLOT {slot} LOGICAL 0/0 "
                f"(proto_version '1', publication_names '{pub}')"
            )

            batch: list[WalChange] = []
            last_confirm_time = asyncio.get_event_loop().time()

            async for msg in cursor:
                if not self._running:
                    break

                data = msg.payload if hasattr(msg, "payload") else bytes(msg)
                if isinstance(data, memoryview):
                    data = bytes(data)

                changes = self._decoder.decode(data)
                batch.extend(changes)

                now = asyncio.get_event_loop().time()

                if (
                    len(batch) >= self._wal_config.batch_size
                    or (now - last_confirm_time)
                    >= self._wal_config.batch_timeout_seconds
                ):
                    await self._publish_batch(batch)
                    batch.clear()
                    await self._publisher.flush()

                    # Confirm LSN back to PostgreSQL
                    if hasattr(msg, "cursor") and hasattr(msg.cursor, "send_feedback"):
                        msg.cursor.send_feedback(flush_lsn=msg.data_start)
                    last_confirm_time = now

            # Publish remaining batch
            if batch:
                await self._publish_batch(batch)
                await self._publisher.flush()

        finally:
            await conn.close()
            logger.info("wal_reader.stopped")

    async def _publish_batch(self, changes: list[WalChange]) -> None:
        """Serialize and publish a batch of WAL changes."""
        for change in changes:
            topic = f"{self._topic_prefix}.{change.schema}.{change.table}"
            key = self._build_key(change)
            value = self._serialize_change(change)
            ordering_key = f"{change.schema}.{change.table}"

            await self._publisher.publish(
                topic=topic,
                key=key,
                value=value,
                ordering_key=ordering_key,
            )

    def _build_key(self, change: WalChange) -> bytes:
        """Build a message key from the change's primary key fields."""
        source = change.after or change.before or {}
        return json.dumps(source, default=str, sort_keys=True).encode("utf-8")

    def _serialize_change(self, change: WalChange) -> bytes:
        """Serialize a WalChange to JSON bytes."""
        payload: dict[str, Any] = {
            "operation": change.operation,
            "schema": change.schema,
            "table": change.table,
            "lsn": change.lsn,
            "timestamp": change.timestamp.isoformat(),
        }
        if change.before is not None:
            payload["before"] = change.before
        if change.after is not None:
            payload["after"] = change.after
        return json.dumps(payload, default=str).encode("utf-8")

    async def stop(self) -> None:
        """Signal the WAL reader to stop."""
        self._running = False
        await self._publisher.close()
