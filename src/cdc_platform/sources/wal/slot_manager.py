"""Replication slot and publication lifecycle management."""

from __future__ import annotations

import structlog

logger = structlog.get_logger()


class SlotManager:
    """Manages PostgreSQL replication slots and publications.

    Uses psycopg3 async connections to create/drop slots and publications
    required for logical replication with pgoutput.
    """

    def __init__(
        self,
        dsn: str,
        slot_name: str = "cdc_slot",
        publication_name: str = "cdc_publication",
    ) -> None:
        self._dsn = dsn
        self._slot_name = slot_name
        self._publication_name = publication_name

    async def ensure_publication(self, tables: list[str]) -> None:
        """Create the publication if it doesn't exist.

        Args:
            tables: Schema-qualified table names (e.g. ["public.customers"]).
        """
        import psycopg

        async with await psycopg.AsyncConnection.connect(
            self._dsn, autocommit=True
        ) as conn:
            # Check if publication exists
            row = await (
                await conn.execute(
                    "SELECT 1 FROM pg_publication WHERE pubname = %s",
                    (self._publication_name,),
                )
            ).fetchone()

            if row is None:
                table_list = ", ".join(tables)
                await conn.execute(
                    f"CREATE PUBLICATION {self._publication_name} FOR TABLE {table_list}"  # noqa: S608
                )
                logger.info(
                    "wal.publication_created",
                    name=self._publication_name,
                    tables=tables,
                )
            else:
                logger.info("wal.publication_exists", name=self._publication_name)

    async def ensure_slot(self) -> None:
        """Create the replication slot if it doesn't exist."""
        import psycopg

        async with await psycopg.AsyncConnection.connect(
            self._dsn, autocommit=True
        ) as conn:
            row = await (
                await conn.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                    (self._slot_name,),
                )
            ).fetchone()

            if row is None:
                await conn.execute(
                    "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                    (self._slot_name,),
                )
                logger.info("wal.slot_created", name=self._slot_name)
            else:
                logger.info("wal.slot_exists", name=self._slot_name)

    async def drop_slot(self) -> None:
        """Drop the replication slot."""
        import psycopg

        async with await psycopg.AsyncConnection.connect(
            self._dsn, autocommit=True
        ) as conn:
            await conn.execute(
                "SELECT pg_drop_replication_slot(%s)",
                (self._slot_name,),
            )
            logger.info("wal.slot_dropped", name=self._slot_name)
