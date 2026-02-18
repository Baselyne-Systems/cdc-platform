"""Time travel and rollback operations for Iceberg tables."""

from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger()


class IcebergTimeTravel:
    """Provides snapshot listing, time-travel queries, and rollback for an Iceberg table."""

    def __init__(self, table: Any, table_name: str) -> None:
        self._table = table
        self._table_name = table_name

    def list_snapshots(self) -> list[dict[str, Any]]:
        return self._table.inspect.snapshots().to_pylist()  # type: ignore[no-any-return]

    def list_history(self) -> list[dict[str, Any]]:
        return self._table.inspect.history().to_pylist()  # type: ignore[no-any-return]

    def scan_at_snapshot(self, snapshot_id: int, limit: int | None = None) -> Any:
        """Return an Arrow table from a point-in-time scan."""
        scan = self._table.scan(snapshot_id=snapshot_id)
        result = scan.to_arrow()
        if limit is not None:
            result = result.slice(0, limit)
        return result

    def rollback_to_snapshot(self, snapshot_id: int) -> None:
        """Roll the table back to a previous snapshot."""
        # Validate snapshot exists
        snapshots = self.list_snapshots()
        valid_ids = {s["snapshot_id"] for s in snapshots}
        if snapshot_id not in valid_ids:
            msg = (
                f"Snapshot {snapshot_id} not found in table '{self._table_name}'. "
                f"Valid snapshot IDs: {sorted(valid_ids)}"
            )
            raise ValueError(msg)

        try:
            self._table.manage_snapshots().set_current_snapshot(snapshot_id).commit()
        except AttributeError:
            msg = (
                "set_current_snapshot is not available in your pyiceberg version. "
                "Upgrade to pyiceberg >=0.9 for rollback support."
            )
            raise RuntimeError(msg) from None

        logger.info(
            "time_travel.rollback_complete",
            table=self._table_name,
            snapshot_id=snapshot_id,
        )
