"""Unit tests for the Iceberg sink connector."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from cdc_platform.config.models import (
    IcebergSinkConfig,
    RetryConfig,
    SinkConfig,
    SinkType,
)
from cdc_platform.sinks.iceberg import IcebergSink


def _make_config(
    write_mode: str = "append",
    batch_size: int = 3,
    auto_create_table: bool = True,
) -> SinkConfig:
    return SinkConfig(
        sink_id="test-iceberg",
        sink_type=SinkType.ICEBERG,
        retry=RetryConfig(max_attempts=2, initial_wait_seconds=0.01, max_wait_seconds=0.1),
        iceberg=IcebergSinkConfig(
            catalog_uri="sqlite:////tmp/test_catalog.db",
            warehouse="file:///tmp/test_warehouse",
            table_name="test_table",
            write_mode=write_mode,
            batch_size=batch_size,
            auto_create_table=auto_create_table,
        ),
    )


def _make_sink(
    write_mode: str = "append",
    batch_size: int = 3,
    auto_create_table: bool = True,
) -> IcebergSink:
    return IcebergSink(_make_config(write_mode, batch_size, auto_create_table))


class TestIcebergSinkConfig:
    def test_defaults(self):
        cfg = IcebergSinkConfig(
            catalog_uri="sqlite:////tmp/cat.db",
            warehouse="file:///tmp/wh",
            table_name="events",
        )
        assert cfg.catalog_name == "default"
        assert cfg.table_namespace == "default"
        assert cfg.write_mode == "append"
        assert cfg.batch_size == 1000
        assert cfg.auto_create_table is True
        assert cfg.partition_by == []
        assert cfg.s3_endpoint is None
        assert cfg.s3_region == "us-east-1"

    def test_s3_secret_is_secret(self):
        cfg = IcebergSinkConfig(
            catalog_uri="sqlite:////tmp/cat.db",
            warehouse="s3://bucket/path",
            table_name="events",
            s3_secret_access_key="supersecret",
        )
        assert cfg.s3_secret_access_key.get_secret_value() == "supersecret"
        assert "supersecret" not in str(cfg)

    def test_invalid_write_mode(self):
        with pytest.raises(ValidationError, match="write_mode"):
            IcebergSinkConfig(
                catalog_uri="sqlite:////tmp/cat.db",
                warehouse="file:///tmp/wh",
                table_name="events",
                write_mode="delete",
            )


class TestSinkConfigIceberg:
    def test_iceberg_requires_iceberg_sub_config(self):
        with pytest.raises(ValidationError, match="iceberg config is required"):
            SinkConfig(sink_id="test", sink_type=SinkType.ICEBERG)

    def test_valid_iceberg_config(self):
        cfg = _make_config()
        assert cfg.sink_id == "test-iceberg"
        assert cfg.iceberg is not None
        assert cfg.iceberg.table_name == "test_table"


@pytest.mark.asyncio
class TestIcebergSink:
    async def test_sink_id(self):
        sink = _make_sink()
        assert sink.sink_id == "test-iceberg"

    async def test_start_loads_catalog_and_table(self):
        sink = _make_sink()
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table

        with patch("cdc_platform.sinks.iceberg.load_catalog", create=True) as mock_load:
            mock_load.return_value = mock_catalog
            with patch.dict("sys.modules", {
                "pyiceberg": MagicMock(),
                "pyiceberg.catalog": MagicMock(load_catalog=mock_load),
            }):
                await sink.start()

        assert sink._catalog is mock_catalog
        assert sink._table is mock_table
        mock_catalog.load_table.assert_called_once_with("default.test_table")

    async def test_start_defers_table_creation_when_auto_create(self):
        sink = _make_sink(auto_create_table=True)
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("NoSuchTableError")

        with patch("cdc_platform.sinks.iceberg.load_catalog", create=True) as mock_load:
            mock_load.return_value = mock_catalog
            with patch.dict("sys.modules", {
                "pyiceberg": MagicMock(),
                "pyiceberg.catalog": MagicMock(load_catalog=mock_load),
            }):
                await sink.start()

        assert sink._table is None

    async def test_start_raises_when_no_auto_create_and_missing_table(self):
        sink = _make_sink(auto_create_table=False)
        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = Exception("NoSuchTableError")

        with (
            patch("cdc_platform.sinks.iceberg.load_catalog", create=True) as mock_load,
            patch.dict("sys.modules", {
                "pyiceberg": MagicMock(),
                "pyiceberg.catalog": MagicMock(load_catalog=mock_load),
            }),
            pytest.raises(RuntimeError, match="auto_create_table is disabled"),
        ):
            mock_load.return_value = mock_catalog
            await sink.start()

    async def test_write_buffers_rows(self):
        sink = _make_sink(batch_size=10)
        sink._catalog = MagicMock()
        sink._table = MagicMock()

        await sink.write(
            key={"id": 1}, value={"name": "Alice"},
            topic="cdc.public.customers", partition=0, offset=1,
        )
        await sink.write(
            key={"id": 2}, value={"name": "Bob"},
            topic="cdc.public.customers", partition=0, offset=2,
        )

        assert len(sink._buffer) == 2
        assert sink._buffer[0]["name"] == "Alice"
        assert sink._buffer[0]["_cdc_topic"] == "cdc.public.customers"
        assert sink._buffer[0]["_cdc_partition"] == 0
        assert sink._buffer[0]["_cdc_offset"] == 1

    async def test_auto_flush_at_batch_size(self):
        sink = _make_sink(batch_size=2)
        sink._catalog = MagicMock()
        mock_table = MagicMock()
        sink._table = mock_table

        mock_pa = MagicMock()
        mock_arrow_table = MagicMock()
        mock_pa.Table.from_pylist.return_value = mock_arrow_table

        with patch.dict("sys.modules", {"pyarrow": mock_pa}):
            await sink.write(key={"id": 1}, value={"v": 1}, topic="t", partition=0, offset=1)
            await sink.write(key={"id": 2}, value={"v": 2}, topic="t", partition=0, offset=2)

        assert len(sink._buffer) == 0
        mock_table.append.assert_called_once_with(mock_arrow_table)

    async def test_flush_append_mode(self):
        sink = _make_sink(write_mode="append", batch_size=10)
        sink._catalog = MagicMock()
        mock_table = MagicMock()
        sink._table = mock_table

        mock_pa = MagicMock()
        mock_arrow_table = MagicMock()
        mock_pa.Table.from_pylist.return_value = mock_arrow_table

        await sink.write(key=None, value={"x": 1}, topic="t", partition=0, offset=1)

        with patch.dict("sys.modules", {"pyarrow": mock_pa}):
            await sink.flush()

        mock_table.append.assert_called_once_with(mock_arrow_table)
        mock_table.upsert.assert_not_called()

    async def test_flush_upsert_mode(self):
        sink = _make_sink(write_mode="upsert", batch_size=10)
        sink._catalog = MagicMock()
        mock_table = MagicMock()
        sink._table = mock_table

        mock_pa = MagicMock()
        mock_arrow_table = MagicMock()
        mock_pa.Table.from_pylist.return_value = mock_arrow_table

        await sink.write(key=None, value={"x": 1}, topic="t", partition=0, offset=1)

        with patch.dict("sys.modules", {"pyarrow": mock_pa}):
            await sink.flush()

        mock_table.upsert.assert_called_once_with(mock_arrow_table)
        mock_table.append.assert_not_called()

    async def test_flush_empty_buffer_is_noop(self):
        sink = _make_sink()
        sink._catalog = MagicMock()
        sink._table = MagicMock()
        await sink.flush()
        sink._table.append.assert_not_called()

    async def test_auto_create_table_on_first_flush(self):
        sink = _make_sink(batch_size=10)
        sink._catalog = MagicMock()
        sink._table = None  # Deferred creation

        mock_created_table = MagicMock()
        sink._catalog.create_table.return_value = mock_created_table

        mock_pa = MagicMock()
        mock_arrow_table = MagicMock()
        mock_arrow_table.schema = MagicMock()
        mock_pa.Table.from_pylist.return_value = mock_arrow_table

        mock_partition_spec = MagicMock()

        await sink.write(key=None, value={"x": 1}, topic="t", partition=0, offset=1)

        with (
            patch.dict("sys.modules", {"pyarrow": mock_pa}),
            patch.object(
                IcebergSink, "_build_partition_spec", return_value=mock_partition_spec
            ),
        ):
            await sink.flush()

        sink._catalog.create_table.assert_called_once_with(
            "default.test_table",
            schema=mock_arrow_table.schema,
            partition_spec=mock_partition_spec,
        )
        mock_created_table.append.assert_called_once_with(mock_arrow_table)

    async def test_stop_flushes_remaining(self):
        sink = _make_sink(batch_size=10)
        sink._catalog = MagicMock()
        mock_table = MagicMock()
        sink._table = mock_table

        mock_pa = MagicMock()
        mock_arrow_table = MagicMock()
        mock_pa.Table.from_pylist.return_value = mock_arrow_table

        await sink.write(key=None, value={"v": 1}, topic="t", partition=0, offset=1)

        with patch.dict("sys.modules", {"pyarrow": mock_pa}):
            await sink.stop()

        mock_table.append.assert_called_once()
        assert sink._catalog is None
        assert sink._table is None

    async def test_health_running(self):
        sink = _make_sink()
        sink._catalog = MagicMock()
        mock_snapshot = MagicMock()
        mock_snapshot.snapshot_id = 12345
        mock_snapshot.timestamp_ms = 1700000000000
        mock_table = MagicMock()
        mock_table.current_snapshot.return_value = mock_snapshot
        sink._table = mock_table

        h = await sink.health()
        assert h["status"] == "running"
        assert h["sink_id"] == "test-iceberg"
        assert h["table"] == "default.test_table"
        assert h["snapshot_id"] == 12345

    async def test_health_stopped(self):
        sink = _make_sink()
        h = await sink.health()
        assert h["status"] == "stopped"
        assert h["sink_id"] == "test-iceberg"

    async def test_write_null_value(self):
        sink = _make_sink(batch_size=10)
        sink._catalog = MagicMock()
        sink._table = MagicMock()

        await sink.write(key={"id": 1}, value=None, topic="t", partition=0, offset=1)

        assert len(sink._buffer) == 1
        row = sink._buffer[0]
        assert row["_cdc_topic"] == "t"
        assert row["_cdc_partition"] == 0
        assert row["_cdc_offset"] == 1

    async def test_lazy_import_error(self):
        sink = _make_sink()
        with (
            patch.dict("sys.modules", {"pyiceberg": None, "pyiceberg.catalog": None}),
            pytest.raises(ImportError, match="cdc-platform\\[iceberg\\]"),
        ):
            await sink.start()

    async def test_flushed_offsets_empty_before_flush(self):
        sink = _make_sink(batch_size=10)
        sink._catalog = MagicMock()
        sink._table = MagicMock()

        await sink.write(key=None, value={"x": 1}, topic="t", partition=0, offset=1)
        assert sink.flushed_offsets == {}

    async def test_flushed_offsets_tracks_max_per_partition(self):
        sink = _make_sink(batch_size=10)
        sink._catalog = MagicMock()
        mock_table = MagicMock()
        sink._table = mock_table

        mock_pa = MagicMock()
        mock_pa.Table.from_pylist.return_value = MagicMock()

        await sink.write(key=None, value={"x": 1}, topic="t", partition=0, offset=1)
        await sink.write(key=None, value={"x": 2}, topic="t", partition=0, offset=5)
        await sink.write(key=None, value={"x": 3}, topic="t", partition=1, offset=3)

        with patch.dict("sys.modules", {"pyarrow": mock_pa}):
            await sink.flush()

        assert sink.flushed_offsets == {("t", 0): 5, ("t", 1): 3}

    async def test_flushed_offsets_not_updated_on_write_failure(self):
        sink = _make_sink(batch_size=10)
        sink._catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.append.side_effect = Exception("write failed")
        sink._table = mock_table

        mock_pa = MagicMock()
        mock_pa.Table.from_pylist.return_value = MagicMock()

        await sink.write(key=None, value={"x": 1}, topic="t", partition=0, offset=1)

        with patch.dict("sys.modules", {"pyarrow": mock_pa}), \
             pytest.raises(Exception, match="write failed"):
            await sink.flush()

        assert sink.flushed_offsets == {}
