from typing import Any

import pytest
import structlog

from .helpers import (
    CountingSink,
    InstrumentedSink,
    consume_with_sink,
    generate_bulk_customers,
)

logger = structlog.get_logger()


@pytest.mark.benchmark
class TestThroughput:
    """End-to-end throughput tests: PG INSERT -> Debezium -> Kafka -> Consumer -> Sink."""

    @pytest.mark.parametrize("row_count", [1_000, 10_000], ids=["1K", "10K"])
    async def test_e2e_throughput(
        self,
        row_count: int,
        pg_dsn: str,
        platform_config: Any,
        benchmark_report: Any,
        _register_connector: Any,
    ) -> None:
        """Measure end-to-end throughput (Postgres -> Debezium -> Kafka -> Consumer -> Sink)."""
        logger.info("benchmark.throughput.start", row_count=row_count)

        # 1. Generate bulk data in Postgres
        generate_bulk_customers(pg_dsn, row_count)

        # 2. Setup sink and consumer
        # generate_bulk_customers TRUNCATEs the table first, so seed rows are gone.
        # Debezium captures the new inserts only.
        expected_count = row_count

        real_sink = CountingSink(expected=expected_count)
        sink = InstrumentedSink(
            real_sink, expected_count=expected_count, track_e2e_latency=False
        )

        # 3. Consume
        logger.info(
            "benchmark.throughput.consuming",
            topic="cdc.public.customers",
            expected=expected_count,
        )

        # Scale timeout with row count: base 60s for Debezium capture latency,
        # plus ~3s per 1K rows for larger batches
        timeout = max(60.0, row_count * 0.003 * 3)

        result = await consume_with_sink(
            topic="cdc.public.customers",
            sink=sink,
            expected_count=expected_count,
            kafka_config=platform_config.kafka,
            timeout=timeout,
        )

        # 4. Report
        result.name = f"e2e-throughput-{row_count}"
        benchmark_report.add_result(result)

        logger.info(
            "benchmark.throughput.complete",
            messages=result.messages,
            duration=result.duration_seconds,
            throughput=result.throughput_msg_per_sec,
        )
