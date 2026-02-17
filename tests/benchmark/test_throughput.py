import uuid
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

    @pytest.fixture
    def bench_topic(self, kafka_admin: Any, platform_config: Any) -> str:
        topic_name = f"bench.throughput.{uuid.uuid4().hex[:8]}"
        return topic_name

    @pytest.mark.parametrize("row_count", [100, 500], ids=["100", "500"])
    async def test_e2e_throughput(
        self,
        row_count: int,
        bench_topic: str,
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
        # Expect row_count + maybe existing rows?
        # The environment is fresh docker compose, but let's assume valid state.
        # We need to account for existing rows if any, but init.sql adds 2 rows.
        expected_count = row_count + 2

        real_sink = CountingSink(expected=expected_count)
        sink = InstrumentedSink(real_sink, expected_count=expected_count)

        # 3. Consume
        logger.info(
            "benchmark.throughput.consuming",
            topic="cdc.public.customers",
            expected=expected_count,
        )

        # Timeout needs to be sufficient for 100K records processing
        # 100K records at 1K/sec -> 100s. Give it 300s.
        timeout = 30.0
        if row_count >= 100_000:
            timeout = 300.0

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
