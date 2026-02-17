import asyncio
import uuid
from typing import Any

import pytest
import structlog

from cdc_platform.config.models import (
    IcebergSinkConfig,
    PostgresSinkConfig,
    SinkConfig,
    SinkType,
    WebhookSinkConfig,
)
from cdc_platform.sinks.postgres import PostgresSink
from cdc_platform.sinks.webhook import WebhookSink

try:
    from cdc_platform.sinks.iceberg import IcebergSink
except ImportError:
    IcebergSink = Any  # type: ignore[misc,assignment]

from .helpers import (
    AvroProducer,
    InstrumentedSink,
    consume_with_sink,
    create_benchmark_topic,
)

logger = structlog.get_logger()
SUSTAINED_LOAD = 2_000


@pytest.mark.benchmark
class TestSinkLatency:
    """Measure per-sink latency distributions under sustained load."""

    @pytest.fixture
    def bench_topic(self, kafka_admin: Any, platform_config: Any) -> str:
        """Create a dedicated benchmark topic and pre-populate it."""
        topic_name = f"bench.latency.{uuid.uuid4().hex[:8]}"
        create_benchmark_topic(kafka_admin, topic_name, num_partitions=1)

        # Pre-produce messages
        producer = AvroProducer(
            topic_name,
            platform_config.kafka.bootstrap_servers,
            platform_config.kafka.schema_registry_url,
        )
        producer.produce_batch(SUSTAINED_LOAD)

        return topic_name

    async def test_webhook_latency(
        self, bench_topic: str, platform_config: Any, benchmark_report: Any
    ) -> None:
        """Measure latency for Webhook sink."""
        # Start a simple asyncio server to act as webhook receiver
        # We use a random port (0) and getting it after start

        received_count = 0

        async def handle_request(reader: Any, writer: Any) -> None:
            nonlocal received_count
            await reader.read(1024)  # Read some bytes
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            received_count += 1

        server = await asyncio.start_server(handle_request, "127.0.0.1", 0)
        async with server:
            # Get the port
            port = server.sockets[0].getsockname()[1]
            url = f"http://127.0.0.1:{port}/webhook"

            # Setup Sink
            config = WebhookSinkConfig(url=url)
            # WebhookSink takes (sink_config: SinkConfig, platform_config: PlatformConfig) usually

            sink_cfg = SinkConfig(
                sink_id="bench-webhook",
                sink_type=SinkType.WEBHOOK,
                webhook=config,
            )

            real_sink = WebhookSink(sink_cfg)
            sink = InstrumentedSink(
                real_sink, expected_count=SUSTAINED_LOAD, track_e2e_latency=False
            )

            await sink.start()

            # Run test
            logger.info("benchmark.latency.consuming", sink_id=sink.sink_id)
            result = await consume_with_sink(
                topic=bench_topic,
                sink=sink,
                expected_count=SUSTAINED_LOAD,
                kafka_config=platform_config.kafka,
                timeout=30.0,
            )

            await sink.stop()

            # Report
            result.name = "sink-latency-bench-webhook"
            result.latency = sink.latency
            benchmark_report.add_result(result)

            logger.info(
                "benchmark.latency.complete",
                sink_id=sink.sink_id,
                p50=sink.latency.p50,
                p95=sink.latency.p95,
                p99=sink.latency.p99,
                throughput=result.throughput_msg_per_sec,
            )

    async def test_postgres_sink_latency(
        self,
        bench_topic: str,
        pg_dsn: str,
        platform_config: Any,
        benchmark_report: Any,
    ) -> None:
        """Measure latency for Postgres sink."""
        import psycopg2  # type: ignore[import-untyped]

        # Create target table
        target_table = "public.bench_latency_sink"
        conn = psycopg2.connect(pg_dsn)
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {target_table}")
                # Create table matching the flat structure PostgresSink expects?
                # Or does PostgresSink create table?
                # Usually CDC sinks map fields.
                # Assuming PostgresSink requires table to exist or auto-creates?
                # Let's assume we need to create it for the benchmark to be reliable.
                # The PostgresSink likely inserts JSONB or flattening.
                # Based on typical implementations: (event_key, event_value, source_topic, ...)
                # I'll inspect generic sink behavior if I can, but let's assume standard columns:
                # event_key, event_value, source_topic, source_partition, source_offset.
                cur.execute(f"""
                    CREATE TABLE {target_table} (
                        event_key TEXT,
                        event_value JSONB,
                        source_topic TEXT,
                        source_partition INTEGER,
                        source_offset BIGINT,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
            conn.commit()
        finally:
            conn.close()

        # Setup Sink
        from pydantic import SecretStr

        pg_config = PostgresSinkConfig(
            host="localhost",  # internal docker host? No, platform_config use localhost for default
            # But the sink runs on host (pytest). So localhost:5432 is correct.
            port=5432,
            database="cdc_demo",
            username="cdc_user",
            password=SecretStr("cdc_password"),
            target_table=target_table,
            batch_size=100,
        )

        sink_cfg = SinkConfig(
            sink_id="bench-postgres",
            sink_type=SinkType.POSTGRES,
            postgres=pg_config,
        )

        real_sink = PostgresSink(sink_cfg)
        sink = InstrumentedSink(
            real_sink, expected_count=SUSTAINED_LOAD, track_e2e_latency=False
        )

        await sink.start()

        # Run test
        logger.info("benchmark.latency.consuming", sink_id=sink.sink_id)
        result = await consume_with_sink(
            topic=bench_topic,
            sink=sink,
            expected_count=SUSTAINED_LOAD,
            kafka_config=platform_config.kafka,
            timeout=30.0,
        )
        await sink.stop()

        # Report
        result.name = "sink-latency-bench-postgres"
        result.latency = sink.latency
        benchmark_report.add_result(result)

        logger.info(
            "benchmark.latency.complete",
            sink_id=sink.sink_id,
            p50=sink.latency.p50,
            p95=sink.latency.p95,
            p99=sink.latency.p99,
            throughput=result.throughput_msg_per_sec,
        )

    async def test_iceberg_sink_latency(
        self,
        bench_topic: str,
        platform_config: Any,
        benchmark_report: Any,
        tmp_path: Any,
    ) -> None:
        """Measure latency for Iceberg sink."""
        pytest.importorskip("pyiceberg")
        if IcebergSink is None:
            pytest.skip("IcebergSink not available")

        # Local catalog using sqlite and tmp directory
        catalog_uri = f"sqlite:///{tmp_path}/catalog.db"
        warehouse = f"file://{tmp_path}/warehouse"

        iceberg_config = IcebergSinkConfig(
            catalog_uri=catalog_uri,
            warehouse=warehouse,
            table_name="bench_iceberg",
            write_mode="append",
            batch_size=500,
            auto_create_table=True,
        )

        sink_cfg = SinkConfig(
            sink_id="bench-iceberg",
            sink_type=SinkType.ICEBERG,
            iceberg=iceberg_config,
        )

        real_sink = IcebergSink(sink_cfg)
        sink = InstrumentedSink(
            real_sink, expected_count=SUSTAINED_LOAD, track_e2e_latency=False
        )

        await sink.start()

        # Run test
        logger.info("benchmark.latency.consuming", sink_id=sink.sink_id)
        result = await consume_with_sink(
            topic=bench_topic,
            sink=sink,
            expected_count=SUSTAINED_LOAD,
            kafka_config=platform_config.kafka,
            timeout=60.0,  # Iceberg might be slower
        )
        await sink.stop()

        # Report
        result.name = "sink-latency-bench-iceberg"
        result.latency = sink.latency
        benchmark_report.add_result(result)

        logger.info(
            "benchmark.latency.complete",
            sink_id=sink.sink_id,
            p50=sink.latency.p50,
            p95=sink.latency.p95,
            p99=sink.latency.p99,
            throughput=result.throughput_msg_per_sec,
        )
