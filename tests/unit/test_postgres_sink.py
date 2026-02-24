"""Unit tests for the PostgreSQL sink connector."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from cdc_platform.config.models import (
    PostgresSinkConfig,
    RetryConfig,
    SinkConfig,
    SinkType,
)
from cdc_platform.sinks.postgres import PostgresSink


def _make_sink(batch_size: int = 3, upsert: bool = False) -> PostgresSink:
    cfg = SinkConfig(
        sink_id="test-pg",
        sink_type=SinkType.POSTGRES,
        retry=RetryConfig(
            max_attempts=2, initial_wait_seconds=0.01, max_wait_seconds=0.1
        ),
        postgres=PostgresSinkConfig(
            database="testdb",
            target_table="public.cdc_events",
            batch_size=batch_size,
            upsert=upsert,
        ),
    )
    return PostgresSink(cfg)


@pytest.mark.asyncio
class TestPostgresSink:
    async def test_start_connects(self):
        sink = _make_sink()
        mock_conn = MagicMock()
        with (
            patch("cdc_platform.sinks.postgres.psycopg2", create=True) as mock_pg,
            patch.dict("sys.modules", {"psycopg2": mock_pg}),
        ):
            mock_pg.connect.return_value = mock_conn
            await sink.start()

            mock_pg.connect.assert_called_once_with(
                host="localhost",
                port=5432,
                dbname="testdb",
                user="cdc_user",
                password="cdc_password",
            )
            assert mock_conn.autocommit is False

    async def test_write_buffers_rows(self):
        sink = _make_sink(batch_size=10)
        sink._conn = MagicMock()

        await sink.write(
            key={"id": 1},
            value={"name": "Alice"},
            topic="cdc.public.customers",
            partition=0,
            offset=1,
        )
        await sink.write(
            key={"id": 2},
            value={"name": "Bob"},
            topic="cdc.public.customers",
            partition=0,
            offset=2,
        )

        assert len(sink._buffer) == 2
        # Not flushed yet — batch_size is 10
        sink._conn.cursor.assert_not_called()

    async def test_auto_flush_at_batch_size(self):
        sink = _make_sink(batch_size=2)
        mock_cursor = MagicMock()
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        await sink.write(
            key={"id": 1}, value={"v": 1}, topic="t", partition=0, offset=1
        )
        await sink.write(
            key={"id": 2}, value={"v": 2}, topic="t", partition=0, offset=2
        )

        # Buffer should have auto-flushed
        assert len(sink._buffer) == 0
        mock_cursor.executemany.assert_called_once()
        sink._conn.commit.assert_called_once()

    async def test_flush_inserts_batch(self):
        sink = _make_sink(batch_size=10)
        mock_cursor = MagicMock()
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        await sink.write(
            key={"id": 1}, value={"v": 1}, topic="t", partition=0, offset=1
        )
        await sink.flush()

        args = mock_cursor.executemany.call_args
        sql = args[0][0]
        rows = args[0][1]
        assert "INSERT INTO public.cdc_events" in sql
        assert len(rows) == 1
        assert rows[0] == (json.dumps({"id": 1}), json.dumps({"v": 1}), "t", 0, 1)

    async def test_flush_empty_buffer_is_noop(self):
        sink = _make_sink()
        sink._conn = MagicMock()
        await sink.flush()
        sink._conn.cursor.assert_not_called()

    async def test_flush_rollback_on_error(self):
        sink = _make_sink(batch_size=10)
        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = Exception("db error")
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        await sink.write(key=None, value=None, topic="t", partition=0, offset=1)

        with pytest.raises(Exception, match="db error"):
            await sink.flush()

        sink._conn.rollback.assert_called()

    async def test_stop_flushes_remaining(self):
        sink = _make_sink(batch_size=10)
        mock_cursor = MagicMock()
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        await sink.write(key=None, value=None, topic="t", partition=0, offset=1)
        await sink.stop()

        mock_cursor.executemany.assert_called_once()
        assert sink._conn is None  # Connection closed

    async def test_health_running(self):
        sink = _make_sink()
        mock_cursor = MagicMock()
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        h = await sink.health()
        assert h["status"] == "running"
        assert h["sink_id"] == "test-pg"
        assert h["target_table"] == "public.cdc_events"

    async def test_health_stopped(self):
        sink = _make_sink()
        h = await sink.health()
        assert h["status"] == "stopped"

    async def test_sink_id(self):
        sink = _make_sink()
        assert sink.sink_id == "test-pg"

    async def test_flushed_offsets_tracks_max_per_partition(self):
        sink = _make_sink(batch_size=10)
        mock_cursor = MagicMock()
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        await sink.write(key=None, value=None, topic="t", partition=0, offset=1)
        await sink.write(key=None, value=None, topic="t", partition=0, offset=5)
        await sink.write(key=None, value=None, topic="t", partition=1, offset=3)
        await sink.flush()

        assert sink.flushed_offsets == {("t", 0): 5, ("t", 1): 3}

    async def test_flushed_offsets_accumulates_across_flushes(self):
        sink = _make_sink(batch_size=10)
        mock_cursor = MagicMock()
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        await sink.write(key=None, value=None, topic="t", partition=0, offset=1)
        await sink.flush()
        assert sink.flushed_offsets == {("t", 0): 1}

        await sink.write(key=None, value=None, topic="t", partition=0, offset=10)
        await sink.flush()
        assert sink.flushed_offsets == {("t", 0): 10}

    async def test_flushed_offsets_empty_before_flush(self):
        sink = _make_sink(batch_size=10)
        sink._conn = MagicMock()

        await sink.write(key=None, value=None, topic="t", partition=0, offset=1)
        assert sink.flushed_offsets == {}

    async def test_flushed_offsets_unchanged_on_flush_failure(self):
        sink = _make_sink(batch_size=10)
        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = Exception("db error")
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        await sink.write(key=None, value=None, topic="t", partition=0, offset=1)

        with pytest.raises(Exception, match="db error"):
            await sink.flush()

        assert sink.flushed_offsets == {}

    async def test_upsert_true_generates_on_conflict_sql(self):
        sink = _make_sink(batch_size=10, upsert=True)
        mock_cursor = MagicMock()
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        await sink.write(
            key={"id": 1}, value={"v": 1}, topic="t", partition=0, offset=1
        )
        await sink.flush()

        sql = mock_cursor.executemany.call_args[0][0]
        assert "ON CONFLICT" in sql
        assert "DO UPDATE SET" in sql
        assert "EXCLUDED.event_key" in sql
        assert "EXCLUDED.event_value" in sql

    async def test_upsert_false_no_on_conflict_sql(self):
        sink = _make_sink(batch_size=10, upsert=False)
        mock_cursor = MagicMock()
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        await sink.write(
            key={"id": 1}, value={"v": 1}, topic="t", partition=0, offset=1
        )
        await sink.flush()

        sql = mock_cursor.executemany.call_args[0][0]
        assert "ON CONFLICT" not in sql

    async def test_flush_runs_in_executor(self):
        """flush() must run DB operations in a thread executor, not block the loop."""
        sink = _make_sink(batch_size=10)
        mock_cursor = MagicMock()
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        await sink.write(key=None, value=None, topic="t", partition=0, offset=1)

        loop = asyncio.get_running_loop()
        with patch.object(loop, "run_in_executor", wraps=loop.run_in_executor) as spy:
            await sink.flush()
            spy.assert_called_once()

    async def test_health_runs_in_executor(self):
        """health() must run DB ping in a thread executor."""
        sink = _make_sink()
        mock_cursor = MagicMock()
        sink._conn = MagicMock()
        sink._conn.cursor.return_value = mock_cursor

        loop = asyncio.get_running_loop()
        with patch.object(loop, "run_in_executor", wraps=loop.run_in_executor) as spy:
            h = await sink.health()
            spy.assert_called_once()
            assert h["status"] == "running"
