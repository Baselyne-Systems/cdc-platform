"""Background table maintenance — snapshot expiry and compaction for Iceberg tables."""

from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from typing import Any

import structlog

from cdc_platform.config.models import TableMaintenanceConfig

try:
    from pyiceberg.expressions import EqualTo
except ImportError:  # pragma: no cover
    EqualTo = None  # type: ignore[assignment,misc]

logger = structlog.get_logger()


class TableMaintenanceMonitor:
    """Async background service for Iceberg table maintenance.

    Runs two independent poll loops:
    - Snapshot expiry: removes old snapshots to bound metadata growth
    - Compaction: rewrites small files into larger ones, partition-at-a-time
    """

    def __init__(
        self,
        table: Any,
        table_name: str,
        config: TableMaintenanceConfig,
        write_lock: asyncio.Lock | None = None,
    ) -> None:
        self._table = table
        self._table_name = table_name
        self._config = config
        self._write_lock = write_lock
        self._expire_task: asyncio.Task[None] | None = None
        self._compact_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._expire_task = asyncio.create_task(
            self._expire_loop(), name=f"expire-{self._table_name}"
        )
        self._compact_task = asyncio.create_task(
            self._compact_loop(), name=f"compact-{self._table_name}"
        )
        logger.info(
            "table_maintenance.started",
            table=self._table_name,
        )

    async def stop(self) -> None:
        for task in (self._expire_task, self._compact_task):
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        logger.info("table_maintenance.stopped", table=self._table_name)

    async def _acquire_lock(self) -> Any:
        """Acquire the write lock if one was provided."""
        if self._write_lock is not None:
            await self._write_lock.acquire()

    def _release_lock(self) -> None:
        """Release the write lock if one was provided."""
        if self._write_lock is not None and self._write_lock.locked():
            self._write_lock.release()

    async def _expire_loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.expire_snapshots_interval_seconds)
            try:
                await self._acquire_lock()
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None, self._do_expire_snapshots
                    )
                finally:
                    self._release_lock()
            except Exception:
                logger.exception(
                    "table_maintenance.expire_error", table=self._table_name
                )

    async def _compact_loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.compaction_interval_seconds)
            try:
                await self._acquire_lock()
                try:
                    await asyncio.get_event_loop().run_in_executor(
                        None, self._do_compact
                    )
                finally:
                    self._release_lock()
            except Exception:
                logger.exception(
                    "table_maintenance.compact_error", table=self._table_name
                )

    def _do_expire_snapshots(self) -> None:
        cutoff_ms = int(
            (time.time() - self._config.expire_snapshots_older_than_seconds) * 1000
        )
        try:
            self._table.manage_snapshots().expire_snapshots_older_than(
                cutoff_ms
            ).commit()
            logger.info(
                "table_maintenance.snapshots_expired",
                table=self._table_name,
                cutoff_ms=cutoff_ms,
            )
        except AttributeError:
            logger.warning(
                "table_maintenance.expire_not_supported",
                table=self._table_name,
                detail="expire_snapshots_older_than API not available; upgrade pyiceberg",
            )

    def _do_compact(self) -> None:
        snapshot = self._table.current_snapshot()
        if snapshot is None:
            return

        partition_spec = self._table.spec()
        if not partition_spec.fields:
            self._compact_unpartitioned()
            return

        self._compact_partitioned(snapshot)

    def _compact_unpartitioned(self) -> None:
        """Compact an unpartitioned table if it has many small files but fits in memory."""
        try:
            manifests = self._table.current_snapshot().manifests(self._table.io)
        except Exception:
            logger.warning(
                "table_maintenance.compact_manifest_error",
                table=self._table_name,
            )
            return

        total_files = sum(
            m.added_files_count + m.existing_files_count for m in manifests
        )
        if total_files < self._config.compaction_file_threshold:
            return

        # Estimate row count from manifest metadata — no data I/O
        total_rows = sum(m.added_rows_count + m.existing_rows_count for m in manifests)

        if total_rows > self._config.compaction_max_rows_per_batch:
            logger.warning(
                "table_maintenance.compact_skipped_too_large",
                table=self._table_name,
                total_rows=total_rows,
                max_rows=self._config.compaction_max_rows_per_batch,
                detail="Table too large for in-process compaction; use Spark/Trino",
            )
            return

        # Only materialize after confirming the table fits in memory
        arrow_table = self._table.scan().to_arrow()
        self._table.overwrite(arrow_table)
        logger.info(
            "table_maintenance.compacted_unpartitioned",
            table=self._table_name,
            files_before=total_files,
            rows=total_rows,
        )

    def _compact_partitioned(self, snapshot: Any) -> None:
        """Compact partitions that exceed the file threshold."""
        try:
            manifests = snapshot.manifests(self._table.io)
        except Exception:
            logger.warning(
                "table_maintenance.compact_manifest_error",
                table=self._table_name,
            )
            return

        # Build partition -> file count map from manifest entries
        partition_file_counts: dict[str, int] = {}
        for manifest in manifests:
            try:
                entries = manifest.fetch_manifest_entry(self._table.io)
                for entry in entries:
                    partition_key = str(entry.data_file.partition)
                    partition_file_counts[partition_key] = (
                        partition_file_counts.get(partition_key, 0) + 1
                    )
            except Exception:
                logger.debug(
                    "table_maintenance.manifest_entry_read_error",
                    table=self._table_name,
                )
                continue

        partition_spec = self._table.spec()
        partition_field_name: str = partition_spec.fields[0].name

        for partition_key, file_count in partition_file_counts.items():
            if file_count < self._config.compaction_file_threshold:
                continue

            try:
                partition_filter = EqualTo(
                    term=partition_field_name,  # type: ignore[arg-type]
                    literal=partition_key,
                )  # type: ignore[call-arg]
                arrow_table = self._table.scan().filter(partition_filter).to_arrow()
                total_rows = arrow_table.num_rows

                if total_rows > self._config.compaction_max_rows_per_batch:
                    logger.warning(
                        "table_maintenance.partition_too_large",
                        table=self._table_name,
                        partition=partition_key,
                        rows=total_rows,
                        max_rows=self._config.compaction_max_rows_per_batch,
                    )
                    continue

                self._table.overwrite(arrow_table, overwrite_filter=partition_filter)
                logger.info(
                    "table_maintenance.partition_compacted",
                    table=self._table_name,
                    partition=partition_key,
                    files_before=file_count,
                    rows=total_rows,
                )
            except Exception:
                logger.exception(
                    "table_maintenance.partition_compact_error",
                    table=self._table_name,
                    partition=partition_key,
                )
