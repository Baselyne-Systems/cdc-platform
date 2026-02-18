"""Unit tests for lakehouse maintenance and time travel."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from cdc_platform.config.models import TableMaintenanceConfig
from cdc_platform.lakehouse.maintenance import TableMaintenanceMonitor
from cdc_platform.lakehouse.time_travel import IcebergTimeTravel

# ---------------------------------------------------------------------------
# Table Maintenance
# ---------------------------------------------------------------------------


class TestExpireSnapshots:
    def test_expire_snapshots_calls_api(self):
        table = MagicMock()
        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="default.events",
            config=TableMaintenanceConfig(enabled=True),
        )
        monitor._do_expire_snapshots()

        table.manage_snapshots.assert_called_once()
        chain = table.manage_snapshots()
        chain.expire_snapshots_older_than.assert_called_once()
        chain.expire_snapshots_older_than().commit.assert_called_once()

    def test_expire_handles_missing_api(self):
        table = MagicMock()
        table.manage_snapshots.side_effect = AttributeError("no manage_snapshots")
        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="default.events",
            config=TableMaintenanceConfig(enabled=True),
        )
        # Should not raise
        monitor._do_expire_snapshots()


class TestCompaction:
    def test_compact_skips_below_file_threshold(self):
        table = MagicMock()
        snapshot = MagicMock()
        table.current_snapshot.return_value = snapshot
        table.spec.return_value = MagicMock(fields=[])

        # Unpartitioned: total files below threshold
        manifest = MagicMock()
        manifest.added_files_count = 3
        manifest.existing_files_count = 2
        snapshot.manifests.return_value = [manifest]

        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="default.events",
            config=TableMaintenanceConfig(enabled=True, compaction_file_threshold=10),
        )
        monitor._do_compact()

        table.overwrite.assert_not_called()

    def test_compact_rewrites_partition_with_many_files(self):
        table = MagicMock()
        snapshot = MagicMock()
        table.current_snapshot.return_value = snapshot

        # Partitioned table
        spec_field = MagicMock()
        spec_field.name = "date"
        table.spec.return_value = MagicMock(fields=[spec_field])

        # Build manifest entries with enough files
        entries = []
        for _ in range(15):
            entry = MagicMock()
            entry.data_file.partition = "2024-01-01"
            entries.append(entry)

        manifest = MagicMock()
        manifest.fetch_manifest_entry.return_value = entries
        snapshot.manifests.return_value = [manifest]

        # Mock the scan/filter/to_arrow chain
        arrow_result = MagicMock()
        arrow_result.num_rows = 1000
        table.scan.return_value.filter.return_value.to_arrow.return_value = arrow_result

        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="default.events",
            config=TableMaintenanceConfig(enabled=True, compaction_file_threshold=10),
        )

        with patch("cdc_platform.lakehouse.maintenance.EqualTo"):
            monitor._do_compact()

        table.overwrite.assert_called_once()

    def test_compact_skips_large_partition(self):
        table = MagicMock()
        snapshot = MagicMock()
        table.current_snapshot.return_value = snapshot

        spec_field = MagicMock()
        spec_field.name = "date"
        table.spec.return_value = MagicMock(fields=[spec_field])

        entries = []
        for _ in range(15):
            entry = MagicMock()
            entry.data_file.partition = "2024-01-01"
            entries.append(entry)

        manifest = MagicMock()
        manifest.fetch_manifest_entry.return_value = entries
        snapshot.manifests.return_value = [manifest]

        # Rows exceed max
        arrow_result = MagicMock()
        arrow_result.num_rows = 1_000_000
        table.scan.return_value.filter.return_value.to_arrow.return_value = arrow_result

        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="default.events",
            config=TableMaintenanceConfig(
                enabled=True,
                compaction_file_threshold=10,
                compaction_max_rows_per_batch=500_000,
            ),
        )

        with patch("cdc_platform.lakehouse.maintenance.EqualTo"):
            monitor._do_compact()

        table.overwrite.assert_not_called()

    def test_compact_unpartitioned_table_too_large(self):
        table = MagicMock()
        snapshot = MagicMock()
        table.current_snapshot.return_value = snapshot
        table.spec.return_value = MagicMock(fields=[])

        manifest = MagicMock()
        manifest.added_files_count = 8
        manifest.existing_files_count = 5
        manifest.added_rows_count = 600_000
        manifest.existing_rows_count = 400_000
        snapshot.manifests.return_value = [manifest]

        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="default.events",
            config=TableMaintenanceConfig(
                enabled=True,
                compaction_file_threshold=10,
                compaction_max_rows_per_batch=500_000,
            ),
        )
        monitor._do_compact()

        table.overwrite.assert_not_called()


@pytest.mark.asyncio
class TestMaintenanceLifecycle:
    async def test_monitor_start_stop_lifecycle(self):
        table = MagicMock()
        monitor = TableMaintenanceMonitor(
            table=table,
            table_name="default.events",
            config=TableMaintenanceConfig(
                enabled=True,
                expire_snapshots_interval_seconds=3600,
                compaction_interval_seconds=7200,
            ),
        )
        await monitor.start()
        assert monitor._expire_task is not None
        assert monitor._compact_task is not None
        assert not monitor._expire_task.done()
        assert not monitor._compact_task.done()

        await monitor.stop()
        assert monitor._expire_task.cancelled()
        assert monitor._compact_task.cancelled()


# ---------------------------------------------------------------------------
# Time Travel
# ---------------------------------------------------------------------------


class TestIcebergTimeTravel:
    def test_list_snapshots(self):
        table = MagicMock()
        expected = [{"snapshot_id": 1}, {"snapshot_id": 2}]
        table.inspect.snapshots.return_value.to_pylist.return_value = expected

        tt = IcebergTimeTravel(table, "default.events")
        assert tt.list_snapshots() == expected

    def test_list_history(self):
        table = MagicMock()
        expected = [{"made_current_at": "2024-01-01"}]
        table.inspect.history.return_value.to_pylist.return_value = expected

        tt = IcebergTimeTravel(table, "default.events")
        assert tt.list_history() == expected

    def test_scan_at_snapshot_with_limit(self):
        table = MagicMock()
        arrow_result = MagicMock()
        table.scan.return_value.limit.return_value.to_arrow.return_value = arrow_result

        tt = IcebergTimeTravel(table, "default.events")
        result = tt.scan_at_snapshot(snapshot_id=123, limit=10)

        table.scan.assert_called_once_with(snapshot_id=123)
        table.scan.return_value.limit.assert_called_once_with(10)
        assert result is arrow_result

    def test_scan_at_snapshot_without_limit(self):
        table = MagicMock()
        arrow_result = MagicMock()
        table.scan.return_value.to_arrow.return_value = arrow_result

        tt = IcebergTimeTravel(table, "default.events")
        result = tt.scan_at_snapshot(snapshot_id=123)

        table.scan.assert_called_once_with(snapshot_id=123)
        table.scan.return_value.limit.assert_not_called()
        assert result is arrow_result

    def test_rollback_invalid_snapshot_raises(self):
        table = MagicMock()
        table.inspect.snapshots.return_value.to_pylist.return_value = [
            {"snapshot_id": 1},
            {"snapshot_id": 2},
        ]

        tt = IcebergTimeTravel(table, "default.events")
        with pytest.raises(ValueError, match="Snapshot 999 not found"):
            tt.rollback_to_snapshot(999)

    def test_rollback_calls_set_current_snapshot(self):
        table = MagicMock()
        table.inspect.snapshots.return_value.to_pylist.return_value = [
            {"snapshot_id": 100},
            {"snapshot_id": 200},
        ]

        tt = IcebergTimeTravel(table, "default.events")
        tt.rollback_to_snapshot(100)

        chain = table.manage_snapshots()
        chain.set_current_snapshot.assert_called_once_with(100)
        chain.set_current_snapshot().commit.assert_called_once()

    def test_rollback_missing_api_raises_clear_error(self):
        table = MagicMock()
        table.inspect.snapshots.return_value.to_pylist.return_value = [
            {"snapshot_id": 100},
        ]
        table.manage_snapshots.return_value.set_current_snapshot.side_effect = (
            AttributeError("no set_current_snapshot")
        )

        tt = IcebergTimeTravel(table, "default.events")
        with pytest.raises(RuntimeError, match="Upgrade to pyiceberg"):
            tt.rollback_to_snapshot(100)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


class TestLakehouseCLI:
    def test_snapshots_command(self):
        from typer.testing import CliRunner

        from cdc_platform.cli import app

        runner = CliRunner()

        mock_table = MagicMock()
        mock_table.inspect.snapshots.return_value.to_pylist.return_value = [
            {
                "snapshot_id": 1,
                "committed_at": "2024-01-01T00:00:00",
                "operation": "append",
                "summary": {},
            }
        ]

        with patch(
            "cdc_platform.cli._load_iceberg_table",
            return_value=(mock_table, "default.events"),
        ):
            result = runner.invoke(app, ["lakehouse", "snapshots", "dummy.yaml"])

        assert result.exit_code == 0
        assert "Snapshot ID" in result.output

    def test_query_command(self):
        from typer.testing import CliRunner

        from cdc_platform.cli import app

        runner = CliRunner()

        mock_table = MagicMock()
        mock_arrow = MagicMock()
        mock_arrow.num_rows = 5
        mock_df = MagicMock()
        mock_df.to_string.return_value = "col1  col2\n1     2"
        mock_arrow.to_pandas.return_value = mock_df
        mock_table.scan.return_value.limit.return_value.to_arrow.return_value = (
            mock_arrow
        )

        with patch(
            "cdc_platform.cli._load_iceberg_table",
            return_value=(mock_table, "default.events"),
        ):
            result = runner.invoke(
                app, ["lakehouse", "query", "dummy.yaml", "--snapshot-id", "123"]
            )

        assert result.exit_code == 0
        assert "5 rows" in result.output

    def test_rollback_with_confirm(self):
        from typer.testing import CliRunner

        from cdc_platform.cli import app

        runner = CliRunner()

        mock_table = MagicMock()
        mock_table.inspect.snapshots.return_value.to_pylist.return_value = [
            {"snapshot_id": 100},
        ]

        with patch(
            "cdc_platform.cli._load_iceberg_table",
            return_value=(mock_table, "default.events"),
        ):
            result = runner.invoke(
                app,
                [
                    "lakehouse",
                    "rollback",
                    "dummy.yaml",
                    "--snapshot-id",
                    "100",
                    "--yes",
                ],
            )

        assert result.exit_code == 0
        assert "Rolled back" in result.output

    def test_rollback_fails_gracefully(self):
        from typer.testing import CliRunner

        from cdc_platform.cli import app

        runner = CliRunner()

        mock_table = MagicMock()
        mock_table.inspect.snapshots.return_value.to_pylist.return_value = [
            {"snapshot_id": 100},
        ]
        mock_table.manage_snapshots.return_value.set_current_snapshot.side_effect = (
            AttributeError("no set_current_snapshot")
        )

        with patch(
            "cdc_platform.cli._load_iceberg_table",
            return_value=(mock_table, "default.events"),
        ):
            result = runner.invoke(
                app,
                [
                    "lakehouse",
                    "rollback",
                    "dummy.yaml",
                    "--snapshot-id",
                    "100",
                    "--yes",
                ],
            )

        assert result.exit_code == 1
        assert "Rollback failed" in result.output
