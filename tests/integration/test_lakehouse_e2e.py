"""End-to-end tests for lakehouse maintenance and time travel.

Uses a real SQLite-backed Iceberg catalog with a local filesystem warehouse.
No Docker services required — these tests exercise pyiceberg directly.
"""

from __future__ import annotations

import shutil
import tempfile

import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog

from cdc_platform.config.models import TableMaintenanceConfig
from cdc_platform.lakehouse.maintenance import TableMaintenanceMonitor
from cdc_platform.lakehouse.time_travel import IcebergTimeTravel


@pytest.fixture()
def iceberg_env():
    """Create a temporary Iceberg catalog + warehouse, yield (catalog, table_name), cleanup."""
    tmpdir = tempfile.mkdtemp(prefix="iceberg_e2e_")
    catalog = load_catalog(
        "test",
        **{
            "type": "sql",
            "uri": f"sqlite:///{tmpdir}/catalog.db",
            "warehouse": f"file://{tmpdir}/warehouse",
        },
    )
    catalog.create_namespace("e2e")
    yield catalog, tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


def _create_unpartitioned_table(catalog, name="e2e.events"):
    """Create an unpartitioned Iceberg table and return it."""
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("_cdc_topic", pa.string()),
            ("_cdc_partition", pa.int64()),
            ("_cdc_offset", pa.int64()),
        ]
    )
    return catalog.create_table(name, schema=schema)


def _append_batch(table, start_id: int, count: int) -> None:
    """Append a batch of rows — each call creates a new snapshot + data file."""
    rows = pa.table(
        {
            "id": pa.array(range(start_id, start_id + count), type=pa.int64()),
            "name": [f"user_{i}" for i in range(start_id, start_id + count)],
            "_cdc_topic": ["cdc.public.events"] * count,
            "_cdc_partition": pa.array([0] * count, type=pa.int64()),
            "_cdc_offset": pa.array(range(start_id, start_id + count), type=pa.int64()),
        }
    )
    table.append(rows)


# ---------------------------------------------------------------------------
# Time Travel
# ---------------------------------------------------------------------------


class TestTimeTravelE2E:
    def test_list_snapshots_reflects_appends(self, iceberg_env):
        catalog, _ = iceberg_env
        table = _create_unpartitioned_table(catalog)

        _append_batch(table, 0, 5)
        _append_batch(table, 5, 5)
        _append_batch(table, 10, 5)

        tt = IcebergTimeTravel(table, "e2e.events")
        snapshots = tt.list_snapshots()
        assert len(snapshots) == 3
        # Each snapshot should have a unique ID
        ids = {s["snapshot_id"] for s in snapshots}
        assert len(ids) == 3

    def test_list_history(self, iceberg_env):
        catalog, _ = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.history_test")

        _append_batch(table, 0, 3)
        _append_batch(table, 3, 3)

        tt = IcebergTimeTravel(table, "e2e.history_test")
        history = tt.list_history()
        assert len(history) >= 2

    def test_scan_at_snapshot_returns_point_in_time_data(self, iceberg_env):
        catalog, _ = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.time_travel_scan")

        # First append: 5 rows
        _append_batch(table, 0, 5)
        tt = IcebergTimeTravel(table, "e2e.time_travel_scan")
        snap1_id = tt.list_snapshots()[-1]["snapshot_id"]

        # Second append: 5 more rows (10 total)
        _append_batch(table, 5, 5)
        snap2_id = tt.list_snapshots()[-1]["snapshot_id"]

        # Scan at snapshot 1 should see only 5 rows
        result1 = tt.scan_at_snapshot(snap1_id)
        assert result1.num_rows == 5

        # Scan at snapshot 2 should see 10 rows
        result2 = tt.scan_at_snapshot(snap2_id)
        assert result2.num_rows == 10

    def test_scan_at_snapshot_with_limit(self, iceberg_env):
        catalog, _ = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.limit_test")

        _append_batch(table, 0, 50)

        tt = IcebergTimeTravel(table, "e2e.limit_test")
        snap_id = tt.list_snapshots()[-1]["snapshot_id"]

        result = tt.scan_at_snapshot(snap_id, limit=10)
        assert result.num_rows == 10

    def test_rollback_restores_previous_state(self, iceberg_env):
        catalog, _ = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.rollback_test")

        # Append 5 rows, record snapshot
        _append_batch(table, 0, 5)
        tt = IcebergTimeTravel(table, "e2e.rollback_test")
        good_snapshot_id = tt.list_snapshots()[-1]["snapshot_id"]

        # Append 5 more "bad" rows
        _append_batch(table, 100, 5)
        assert table.scan().to_arrow().num_rows == 10

        # Rollback to the good snapshot
        tt.rollback_to_snapshot(good_snapshot_id)

        # Table should now show only the original 5 rows
        result = table.scan().to_arrow()
        assert result.num_rows == 5
        ids = result.column("id").to_pylist()
        assert sorted(ids) == [0, 1, 2, 3, 4]

    def test_rollback_invalid_snapshot_raises(self, iceberg_env):
        catalog, _ = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.bad_rollback")

        _append_batch(table, 0, 3)

        tt = IcebergTimeTravel(table, "e2e.bad_rollback")
        with pytest.raises(ValueError, match="not found"):
            tt.rollback_to_snapshot(999999999)


# ---------------------------------------------------------------------------
# Table Maintenance — Compaction (unpartitioned)
# ---------------------------------------------------------------------------


class TestCompactionE2E:
    def test_compact_reduces_file_count(self, iceberg_env):
        """Write many small batches, then compact — file count should drop."""
        catalog, _ = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.compact_test")

        # Write 12 small batches → 12 data files
        for i in range(12):
            _append_batch(table, i * 5, 5)

        snapshot_before = table.current_snapshot()
        manifests_before = snapshot_before.manifests(table.io)
        files_before = sum(
            m.added_files_count + m.existing_files_count for m in manifests_before
        )
        assert files_before >= 12

        # Run compaction
        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="e2e.compact_test",
            config=TableMaintenanceConfig(
                enabled=True,
                compaction_file_threshold=5,
                compaction_max_rows_per_batch=500_000,
            ),
        )
        monitor._do_compact()

        # After compaction: all data rewritten into fewer files
        snapshot_after = table.current_snapshot()
        manifests_after = snapshot_after.manifests(table.io)
        files_after = sum(
            m.added_files_count + m.existing_files_count for m in manifests_after
        )
        assert files_after < files_before

        # Data integrity: row count should be preserved
        result = table.scan().to_arrow()
        assert result.num_rows == 60  # 12 batches × 5 rows

    def test_compact_skips_when_below_threshold(self, iceberg_env):
        """With few files, compaction should be a no-op."""
        catalog, _ = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.compact_skip")

        # Only 3 batches → 3 files, below threshold of 10
        for i in range(3):
            _append_batch(table, i * 5, 5)

        snapshot_before_id = table.current_snapshot().snapshot_id

        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="e2e.compact_skip",
            config=TableMaintenanceConfig(
                enabled=True,
                compaction_file_threshold=10,
            ),
        )
        monitor._do_compact()

        # No new snapshot should have been created
        assert table.current_snapshot().snapshot_id == snapshot_before_id

    def test_compact_skips_table_too_large(self, iceberg_env):
        """When row count exceeds max, compaction should skip."""
        catalog, _ = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.compact_large")

        # Write 12 batches of 10 rows = 120 rows
        for i in range(12):
            _append_batch(table, i * 10, 10)

        snapshot_before_id = table.current_snapshot().snapshot_id

        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="e2e.compact_large",
            config=TableMaintenanceConfig(
                enabled=True,
                compaction_file_threshold=5,
                compaction_max_rows_per_batch=50,  # Very low limit
            ),
        )
        monitor._do_compact()

        # Should have skipped — no new snapshot
        assert table.current_snapshot().snapshot_id == snapshot_before_id


# ---------------------------------------------------------------------------
# Table Maintenance — Snapshot Expiry
# ---------------------------------------------------------------------------


class TestSnapshotExpiryE2E:
    def test_expire_handles_missing_api_gracefully(self, iceberg_env):
        """On pyiceberg 0.11, expire_snapshots_older_than doesn't exist — should not raise."""
        catalog, _ = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.expire_test")

        _append_batch(table, 0, 5)
        _append_batch(table, 5, 5)

        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="e2e.expire_test",
            config=TableMaintenanceConfig(
                enabled=True,
                expire_snapshots_older_than_seconds=0.0,  # Expire everything
            ),
        )

        # Should not raise — gracefully handles missing API
        monitor._do_expire_snapshots()


# ---------------------------------------------------------------------------
# Table Maintenance — Monitor Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestMaintenanceLifecycleE2E:
    async def test_monitor_start_runs_and_stops_cleanly(self, iceberg_env):
        """Start the monitor with short intervals, let it tick, then stop."""
        catalog, _ = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.lifecycle_test")

        _append_batch(table, 0, 5)

        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="e2e.lifecycle_test",
            config=TableMaintenanceConfig(
                enabled=True,
                expire_snapshots_interval_seconds=0.1,
                compaction_interval_seconds=0.1,
            ),
        )

        await monitor.start()
        assert monitor._expire_task is not None
        assert monitor._compact_task is not None

        # Let at least one tick fire
        import asyncio

        await asyncio.sleep(0.3)

        await monitor.stop()
        assert monitor._expire_task.cancelled()
        assert monitor._compact_task.cancelled()


# ---------------------------------------------------------------------------
# CLI (lakehouse sub-app)
# ---------------------------------------------------------------------------


class TestLakehouseCLIE2E:
    def test_snapshots_command_with_real_table(self, iceberg_env):
        """Run `cdc lakehouse snapshots` against a real Iceberg table."""
        catalog, tmpdir = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.cli_snapshots")

        _append_batch(table, 0, 5)
        _append_batch(table, 5, 5)

        from unittest.mock import patch

        from typer.testing import CliRunner

        from cdc_platform.cli import app

        runner = CliRunner()

        with patch(
            "cdc_platform.cli._load_iceberg_table",
            return_value=(table, "e2e.cli_snapshots"),
        ):
            result = runner.invoke(app, ["lakehouse", "snapshots", "dummy.yaml"])

        assert result.exit_code == 0
        assert "Snapshot ID" in result.output

    def test_query_command_with_real_table(self, iceberg_env):
        """Run `cdc lakehouse query` against a real Iceberg table."""
        catalog, tmpdir = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.cli_query")

        _append_batch(table, 0, 10)

        tt = IcebergTimeTravel(table, "e2e.cli_query")
        snap_id = tt.list_snapshots()[-1]["snapshot_id"]

        from unittest.mock import patch

        from typer.testing import CliRunner

        from cdc_platform.cli import app

        runner = CliRunner()

        with patch(
            "cdc_platform.cli._load_iceberg_table",
            return_value=(table, "e2e.cli_query"),
        ):
            result = runner.invoke(
                app,
                [
                    "lakehouse",
                    "query",
                    "dummy.yaml",
                    "--snapshot-id",
                    str(snap_id),
                    "--limit",
                    "5",
                ],
            )

        assert result.exit_code == 0
        assert "5 rows" in result.output

    def test_rollback_command_with_real_table(self, iceberg_env):
        """Run `cdc lakehouse rollback` against a real Iceberg table."""
        catalog, tmpdir = iceberg_env
        table = _create_unpartitioned_table(catalog, "e2e.cli_rollback")

        _append_batch(table, 0, 5)
        tt = IcebergTimeTravel(table, "e2e.cli_rollback")
        good_snap = tt.list_snapshots()[-1]["snapshot_id"]

        _append_batch(table, 100, 5)
        assert table.scan().to_arrow().num_rows == 10

        from unittest.mock import patch

        from typer.testing import CliRunner

        from cdc_platform.cli import app

        runner = CliRunner()

        with patch(
            "cdc_platform.cli._load_iceberg_table",
            return_value=(table, "e2e.cli_rollback"),
        ):
            result = runner.invoke(
                app,
                [
                    "lakehouse",
                    "rollback",
                    "dummy.yaml",
                    "--snapshot-id",
                    str(good_snap),
                    "--yes",
                ],
            )

        assert result.exit_code == 0
        assert "Rolled back" in result.output
        assert table.scan().to_arrow().num_rows == 5
