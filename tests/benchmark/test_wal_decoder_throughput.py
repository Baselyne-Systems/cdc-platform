"""Benchmark tests for WAL decoder throughput.

Measures decode throughput for pgoutput binary messages under various
workloads: bulk inserts, mixed operations, wide tables, and large values.
"""

from __future__ import annotations

import struct
import time

import pytest
import structlog

from cdc_platform.sources.wal.decoder import PgOutputDecoder

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# WAL message builders (same as integration tests)
# ---------------------------------------------------------------------------


def _begin(lsn: int = 100, ts_us: int = 1_000_000, xid: int = 1) -> bytes:
    return (
        b"B"
        + struct.pack("!Q", lsn)
        + struct.pack("!q", ts_us)
        + struct.pack("!I", xid)
    )


def _commit(lsn: int = 100) -> bytes:
    return (
        b"C"
        + b"\x00"
        + struct.pack("!Q", lsn)
        + struct.pack("!Q", lsn)
        + struct.pack("!q", 0)
    )


def _relation(
    rel_id: int,
    namespace: str,
    table: str,
    columns: list[tuple[str, int]],
) -> bytes:
    data = b"R"
    data += struct.pack("!I", rel_id)
    data += namespace.encode() + b"\x00"
    data += table.encode() + b"\x00"
    data += b"\x00"
    data += struct.pack("!H", len(columns))
    for col_name, type_oid in columns:
        data += b"\x00"
        data += col_name.encode() + b"\x00"
        data += struct.pack("!I", type_oid)
        data += struct.pack("!I", 0)
    return data


def _tuple_data(values: list[str | None]) -> bytes:
    data = struct.pack("!H", len(values))
    for val in values:
        if val is None:
            data += b"n"
        else:
            encoded = val.encode("utf-8")
            data += b"t" + struct.pack("!I", len(encoded)) + encoded
    return data


def _insert(rel_id: int, values: list[str | None]) -> bytes:
    return b"I" + struct.pack("!I", rel_id) + b"N" + _tuple_data(values)


def _update_no_old(rel_id: int, new_values: list[str | None]) -> bytes:
    return b"U" + struct.pack("!I", rel_id) + b"N" + _tuple_data(new_values)


def _delete(rel_id: int, values: list[str | None]) -> bytes:
    return b"D" + struct.pack("!I", rel_id) + b"K" + _tuple_data(values)


# ---------------------------------------------------------------------------
# benchmarks
# ---------------------------------------------------------------------------


@pytest.mark.benchmark
class TestWalDecoderThroughput:
    """Measure WAL decoder decode throughput."""

    @pytest.mark.parametrize(
        "msg_count",
        [10_000, 50_000, 100_000],
        ids=["10K", "50K", "100K"],
    )
    def test_insert_decode_throughput(self, msg_count: int):
        """Measure inserts/sec for pure INSERT decoding."""
        decoder = PgOutputDecoder()

        # Setup relation
        decoder.decode(
            _relation(
                1,
                "public",
                "customers",
                [("id", 23), ("email", 25), ("name", 25)],
            )
        )

        # Pre-build messages
        messages = [
            _insert(1, [str(i), f"user{i}@example.com", f"User {i}"])
            for i in range(msg_count)
        ]

        # Time the decoding
        decoder.decode(_begin(lsn=1000))
        start = time.perf_counter()
        total_changes = 0
        for msg in messages:
            changes = decoder.decode(msg)
            total_changes += len(changes)
        elapsed = time.perf_counter() - start
        decoder.decode(_commit(lsn=1000))

        throughput = total_changes / elapsed if elapsed > 0 else 0

        logger.info(
            "benchmark.wal_decoder.insert_throughput",
            messages=msg_count,
            changes=total_changes,
            elapsed_seconds=round(elapsed, 3),
            msgs_per_sec=round(throughput),
        )

        assert total_changes == msg_count
        # Expect at least 50K msgs/sec on any reasonable hardware
        assert throughput > 50_000, f"Throughput {throughput:.0f}/s below 50K/s"

    def test_mixed_operation_throughput(self):
        """Measure throughput for mixed INSERT/UPDATE/DELETE workload."""
        decoder = PgOutputDecoder()
        msg_count = 30_000

        decoder.decode(
            _relation(
                1,
                "public",
                "orders",
                [("id", 23), ("status", 25), ("total", 1700)],
            )
        )

        # Build mixed messages: 60% insert, 25% update, 15% delete
        messages: list[bytes] = []
        for i in range(msg_count):
            if i % 100 < 60:
                messages.append(_insert(1, [str(i), "pending", "99.99"]))
            elif i % 100 < 85:
                messages.append(_update_no_old(1, [str(i), "shipped", "99.99"]))
            else:
                messages.append(_delete(1, [str(i), "cancelled", "0.00"]))

        decoder.decode(_begin(lsn=2000))
        start = time.perf_counter()
        total_changes = 0
        for msg in messages:
            changes = decoder.decode(msg)
            total_changes += len(changes)
        elapsed = time.perf_counter() - start
        decoder.decode(_commit(lsn=2000))

        throughput = total_changes / elapsed if elapsed > 0 else 0

        logger.info(
            "benchmark.wal_decoder.mixed_throughput",
            messages=msg_count,
            elapsed_seconds=round(elapsed, 3),
            msgs_per_sec=round(throughput),
        )

        assert total_changes == msg_count
        assert throughput > 30_000

    @pytest.mark.parametrize(
        "col_count",
        [10, 50, 100],
        ids=["10col", "50col", "100col"],
    )
    def test_wide_table_throughput(self, col_count: int):
        """Measure throughput impact of increasing column count."""
        decoder = PgOutputDecoder()
        msg_count = 10_000

        columns = [(f"col_{i}", 25) for i in range(col_count)]
        decoder.decode(_relation(1, "wide", "table", columns))

        values = [f"value_{i}" for i in range(col_count)]
        messages = [_insert(1, values) for _ in range(msg_count)]

        decoder.decode(_begin())
        start = time.perf_counter()
        total_changes = 0
        for msg in messages:
            changes = decoder.decode(msg)
            total_changes += len(changes)
        elapsed = time.perf_counter() - start
        decoder.decode(_commit())

        throughput = total_changes / elapsed if elapsed > 0 else 0

        logger.info(
            "benchmark.wal_decoder.wide_table",
            columns=col_count,
            messages=msg_count,
            elapsed_seconds=round(elapsed, 3),
            msgs_per_sec=round(throughput),
        )

        assert total_changes == msg_count
        # Wide tables are slower but should still be reasonable
        assert throughput > 5_000

    def test_multi_table_interleaved_throughput(self):
        """Measure throughput when rapidly switching between tables."""
        decoder = PgOutputDecoder()
        msg_count = 30_000
        num_tables = 10

        # Register multiple relations
        for t in range(num_tables):
            decoder.decode(
                _relation(
                    t + 1,
                    "public",
                    f"table_{t}",
                    [("id", 23), ("data", 25)],
                )
            )

        # Build interleaved messages
        messages = [
            _insert(
                (i % num_tables) + 1,
                [str(i), f"data_{i}"],
            )
            for i in range(msg_count)
        ]

        decoder.decode(_begin())
        start = time.perf_counter()
        total_changes = 0
        for msg in messages:
            changes = decoder.decode(msg)
            total_changes += len(changes)
        elapsed = time.perf_counter() - start
        decoder.decode(_commit())

        throughput = total_changes / elapsed if elapsed > 0 else 0

        logger.info(
            "benchmark.wal_decoder.multi_table",
            tables=num_tables,
            messages=msg_count,
            elapsed_seconds=round(elapsed, 3),
            msgs_per_sec=round(throughput),
        )

        assert total_changes == msg_count
        assert throughput > 30_000

    def test_large_value_throughput(self):
        """Measure throughput impact of large text values (simulating TOAST)."""
        decoder = PgOutputDecoder()
        msg_count = 1_000

        decoder.decode(_relation(1, "public", "docs", [("id", 23), ("body", 25)]))

        large_text = "x" * 10_000  # 10KB per value
        messages = [_insert(1, [str(i), large_text]) for i in range(msg_count)]

        decoder.decode(_begin())
        start = time.perf_counter()
        total_changes = 0
        for msg in messages:
            changes = decoder.decode(msg)
            total_changes += len(changes)
        elapsed = time.perf_counter() - start
        decoder.decode(_commit())

        throughput = total_changes / elapsed if elapsed > 0 else 0
        data_rate_mbps = (
            (msg_count * 10_000) / (elapsed * 1024 * 1024) if elapsed > 0 else 0
        )

        logger.info(
            "benchmark.wal_decoder.large_values",
            messages=msg_count,
            value_size_bytes=10_000,
            elapsed_seconds=round(elapsed, 3),
            msgs_per_sec=round(throughput),
            data_rate_mbps=round(data_rate_mbps, 1),
        )

        assert total_changes == msg_count
        assert throughput > 500

    def test_null_heavy_throughput(self):
        """Measure throughput with mostly-NULL rows (sparse data)."""
        decoder = PgOutputDecoder()
        msg_count = 50_000

        columns = [(f"col_{i}", 25) for i in range(20)]
        decoder.decode(_relation(1, "public", "sparse", columns))

        # Only 3 out of 20 columns have values
        values: list[str | None] = [None] * 20
        values[0] = "id_value"
        values[5] = "some_data"
        values[19] = "last_col"

        messages = [_insert(1, values) for _ in range(msg_count)]

        decoder.decode(_begin())
        start = time.perf_counter()
        total_changes = 0
        for msg in messages:
            changes = decoder.decode(msg)
            total_changes += len(changes)
        elapsed = time.perf_counter() - start
        decoder.decode(_commit())

        throughput = total_changes / elapsed if elapsed > 0 else 0

        logger.info(
            "benchmark.wal_decoder.null_heavy",
            messages=msg_count,
            elapsed_seconds=round(elapsed, 3),
            msgs_per_sec=round(throughput),
        )

        assert total_changes == msg_count
        assert throughput > 50_000
