"""PostgreSQL destination sink connector."""

from __future__ import annotations

import json
from typing import Any

import structlog
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
)

from cdc_platform.config.models import SinkConfig

logger = structlog.get_logger()


class PostgresSink:
    """Writes CDC events to a PostgreSQL destination table with batched inserts."""

    def __init__(self, config: SinkConfig) -> None:
        from cdc_platform.config.models import PostgresSinkConfig

        self._config = config
        if config.postgres is None:
            msg = "PostgresSink requires a postgres sub-config"
            raise ValueError(msg)
        self._pg_config: PostgresSinkConfig = config.postgres
        self._conn: Any = None
        self._buffer: list[tuple[str, str, str, int, int]] = []
        self._flushed_offsets: dict[tuple[str, int], int] = {}

    @property
    def sink_id(self) -> str:
        return self._config.sink_id

    @property
    def flushed_offsets(self) -> dict[tuple[str, int], int]:
        return self._flushed_offsets

    async def start(self) -> None:
        try:
            import psycopg2
        except ImportError:
            msg = (
                "psycopg2 is required for the PostgreSQL sink. "
                "Install it with: pip install cdc-platform[postgres]"
            )
            raise ImportError(msg) from None

        self._conn = psycopg2.connect(
            host=self._pg_config.host,
            port=self._pg_config.port,
            dbname=self._pg_config.database,
            user=self._pg_config.username,
            password=self._pg_config.password.get_secret_value(),
        )
        self._conn.autocommit = False
        logger.info(
            "postgres_sink.started",
            sink_id=self.sink_id,
            target_table=self._pg_config.target_table,
        )

    async def write(
        self,
        key: dict[str, Any] | None,
        value: dict[str, Any] | None,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        row = (
            json.dumps(key),
            json.dumps(value),
            topic,
            partition,
            offset,
        )
        self._buffer.append(row)
        if len(self._buffer) >= self._pg_config.batch_size:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer or self._conn is None:
            return

        batch = list(self._buffer)
        self._buffer.clear()

        retry_cfg = self._config.retry

        @retry(
            stop=stop_after_attempt(retry_cfg.max_attempts),
            wait=wait_exponential_jitter(
                initial=retry_cfg.initial_wait_seconds,
                max=retry_cfg.max_wait_seconds,
                jitter=retry_cfg.multiplier if retry_cfg.jitter else 0,
            ),
            reraise=True,
        )
        def _insert() -> None:
            try:
                assert self._conn is not None
                cur = self._conn.cursor()
                sql = (
                    f"INSERT INTO {self._pg_config.target_table} "  # noqa: S608
                    "(event_key, event_value, source_topic, source_partition, source_offset) "
                    "VALUES (%s, %s, %s, %s, %s)"
                )
                if self._pg_config.upsert:
                    sql += (
                        " ON CONFLICT (source_topic, source_partition, source_offset) "
                        "DO UPDATE SET event_key=EXCLUDED.event_key, "
                        "event_value=EXCLUDED.event_value"
                    )
                cur.executemany(sql, batch)
                self._conn.commit()
                cur.close()
            except Exception:
                self._conn.rollback()
                raise

        _insert()

        for row in batch:
            key_tp = (row[2], row[3])  # (topic, partition)
            if row[4] > self._flushed_offsets.get(key_tp, -1):
                self._flushed_offsets[key_tp] = row[4]

        logger.debug(
            "postgres_sink.flushed",
            sink_id=self.sink_id,
            rows=len(batch),
        )

    async def stop(self) -> None:
        await self.flush()
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        logger.info("postgres_sink.stopped", sink_id=self.sink_id)

    async def health(self) -> dict[str, Any]:
        connected = False
        if self._conn is not None:
            try:
                cur = self._conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                connected = True
            except Exception:
                connected = False
        return {
            "sink_id": self.sink_id,
            "type": "postgres",
            "status": "running" if connected else "stopped",
            "target_table": self._pg_config.target_table,
            "buffer_size": len(self._buffer),
        }
