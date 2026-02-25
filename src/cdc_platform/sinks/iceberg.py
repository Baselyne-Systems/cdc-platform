"""Apache Iceberg lakehouse sink connector."""

import asyncio
import contextlib
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import structlog

from cdc_platform.config.models import SinkConfig

logger = structlog.get_logger()


class IcebergSink:
    """Writes CDC events to an Apache Iceberg table with batched appends/upserts."""

    def __init__(self, config: SinkConfig) -> None:
        from cdc_platform.config.models import IcebergSinkConfig

        self._config = config
        if config.iceberg is None:
            msg = "IcebergSink requires an iceberg sub-config"
            raise ValueError(msg)
        self._ice_config: IcebergSinkConfig = config.iceberg
        self._catalog: Any = None
        self._table: Any = None
        self._buffer: list[dict[str, Any]] = []
        self._flushed_offsets: dict[tuple[str, int], int] = {}
        self._maintenance_monitor: Any = None
        self._write_lock: asyncio.Lock = asyncio.Lock()
        self._write_executor: ThreadPoolExecutor | None = None
        self._flush_task: asyncio.Task[None] | None = None

    @property
    def sink_id(self) -> str:
        return self._config.sink_id

    @property
    def flushed_offsets(self) -> dict[tuple[str, int], int]:
        return self._flushed_offsets

    async def start(self) -> None:
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError:
            msg = (
                "pyiceberg is required for the Iceberg sink. "
                "Install it with: pip install cdc-platform[iceberg]"
            )
            raise ImportError(msg) from None

        cfg = self._ice_config
        catalog_props: dict[str, str] = {
            "uri": cfg.catalog_uri,
            "warehouse": cfg.warehouse,
        }
        if cfg.s3_endpoint is not None:
            catalog_props["s3.endpoint"] = cfg.s3_endpoint
        if cfg.s3_access_key_id is not None:
            catalog_props["s3.access-key-id"] = cfg.s3_access_key_id
        if cfg.s3_secret_access_key is not None:
            catalog_props["s3.secret-access-key"] = (
                cfg.s3_secret_access_key.get_secret_value()
            )
        catalog_props["s3.region"] = cfg.s3_region

        self._catalog = load_catalog(cfg.catalog_name, **catalog_props)

        full_name = f"{cfg.table_namespace}.{cfg.table_name}"
        try:
            self._table = self._catalog.load_table(full_name)
            logger.info(
                "iceberg_sink.table_loaded",
                sink_id=self.sink_id,
                table=full_name,
            )
        except Exception:
            if not cfg.auto_create_table:
                msg = (
                    f"Iceberg table '{full_name}' not found and "
                    f"auto_create_table is disabled"
                )
                raise RuntimeError(msg) from None
            self._table = None
            logger.info(
                "iceberg_sink.table_deferred",
                sink_id=self.sink_id,
                table=full_name,
            )

        if self._ice_config.maintenance.enabled and self._table is not None:
            from cdc_platform.lakehouse.maintenance import TableMaintenanceMonitor

            self._maintenance_monitor = TableMaintenanceMonitor(
                table=self._table,
                table_name=full_name,
                config=self._ice_config.maintenance,
                write_lock=self._write_lock,
            )
            await self._maintenance_monitor.start()

        # High-throughput: thread pool for blocking Iceberg writes
        if self._ice_config.write_executor_threads > 0:
            self._write_executor = ThreadPoolExecutor(
                max_workers=self._ice_config.write_executor_threads
            )

        # High-throughput: periodic flush for partial batches
        if self._ice_config.flush_interval_seconds > 0:
            self._flush_task = asyncio.create_task(self._periodic_flush_loop())

        logger.info("iceberg_sink.started", sink_id=self.sink_id)

    async def write(
        self,
        key: dict[str, Any] | None,
        value: dict[str, Any] | None,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        row: dict[str, Any] = {}
        if value is not None:
            row.update(value)
        row["_cdc_topic"] = topic
        row["_cdc_partition"] = partition
        row["_cdc_offset"] = offset
        self._buffer.append(row)

        if len(self._buffer) >= self._ice_config.batch_size:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return

        try:
            import pyarrow as pa  # type: ignore[import-untyped]
        except ImportError:
            msg = (
                "pyarrow is required for the Iceberg sink. "
                "Install it with: pip install cdc-platform[iceberg]"
            )
            raise ImportError(msg) from None

        batch = list(self._buffer)
        self._buffer.clear()

        arrow_table = pa.Table.from_pylist(batch)

        cfg = self._ice_config
        full_name = f"{cfg.table_namespace}.{cfg.table_name}"

        if self._table is None:
            async with self._write_lock:
                # Double-check after acquiring lock (another flush may have created it)
                if self._table is None:
                    partition_spec = self._build_partition_spec(cfg.partition_by)
                    with contextlib.suppress(Exception):
                        self._catalog.create_namespace(cfg.table_namespace)
                    try:
                        self._table = self._catalog.create_table(
                            full_name,
                            schema=arrow_table.schema,
                            partition_spec=partition_spec,
                        )
                        logger.info(
                            "iceberg_sink.table_created",
                            sink_id=self.sink_id,
                            table=full_name,
                        )
                    except Exception:
                        # Table may already exist from a concurrent creation
                        self._table = self._catalog.load_table(full_name)
                        logger.info(
                            "iceberg_sink.table_loaded_after_race",
                            sink_id=self.sink_id,
                            table=full_name,
                        )

        t0 = time.monotonic()
        async with self._write_lock:
            if self._write_executor is not None:
                loop = asyncio.get_running_loop()
                if cfg.write_mode == "upsert":
                    await loop.run_in_executor(
                        self._write_executor, self._table.upsert, arrow_table
                    )
                else:
                    await loop.run_in_executor(
                        self._write_executor, self._table.append, arrow_table
                    )
            else:
                if cfg.write_mode == "upsert":
                    self._table.upsert(arrow_table)
                else:
                    self._table.append(arrow_table)
        elapsed_ms = (time.monotonic() - t0) * 1000

        for row in batch:
            key_tp = (row["_cdc_topic"], row["_cdc_partition"])
            if row["_cdc_offset"] > self._flushed_offsets.get(key_tp, -1):
                self._flushed_offsets[key_tp] = row["_cdc_offset"]

        logger.info(
            "iceberg_sink.flushed",
            sink_id=self.sink_id,
            rows=len(batch),
            latency_ms=round(elapsed_ms, 2),
        )

    async def _periodic_flush_loop(self) -> None:
        """Flush partial batches on a timer to prevent data staleness."""
        interval = self._ice_config.flush_interval_seconds
        while True:
            await asyncio.sleep(interval)
            try:
                await self.flush()
            except Exception:
                logger.exception(
                    "iceberg_sink.periodic_flush_error", sink_id=self.sink_id
                )

    async def stop(self) -> None:
        if self._flush_task is not None:
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task
            self._flush_task = None
        if self._maintenance_monitor is not None:
            await self._maintenance_monitor.stop()
            self._maintenance_monitor = None
        await self.flush()
        if self._write_executor is not None:
            self._write_executor.shutdown(wait=False)
            self._write_executor = None
        self._catalog = None
        self._table = None
        logger.info("iceberg_sink.stopped", sink_id=self.sink_id)

    async def health(self) -> dict[str, Any]:
        cfg = self._ice_config
        full_name = f"{cfg.table_namespace}.{cfg.table_name}"
        status = "stopped"
        snapshot: dict[str, Any] = {}

        if self._catalog is not None:
            try:
                if self._table is not None:
                    current = self._table.current_snapshot()
                    if current is not None:
                        snapshot = {
                            "snapshot_id": current.snapshot_id,
                            "timestamp_ms": current.timestamp_ms,
                        }
                status = "running"
            except Exception:
                status = "degraded"

        return {
            "sink_id": self.sink_id,
            "type": "iceberg",
            "status": status,
            "table": full_name,
            "buffer_size": len(self._buffer),
            **snapshot,
        }

    @staticmethod
    def _build_partition_spec(partition_by: list[str]) -> Any:
        from pyiceberg.partitioning import PartitionSpec

        if not partition_by:
            return PartitionSpec()
        return PartitionSpec(*partition_by)  # type: ignore[arg-type]
