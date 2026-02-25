"""Load tests for high-throughput pipeline features.

Exercises batch polling, parallel deserialization, periodic offset commits,
and Iceberg write executor offload at 50K-100K+ message scale.
Compares default (single-poll) vs high-throughput config to verify speedup.
"""

import asyncio
import contextlib
import time
import uuid
from typing import Any

import pytest
import structlog

from cdc_platform.config.models import (
    IcebergSinkConfig,
    KafkaConfig,
    SinkConfig,
    SinkType,
)
from cdc_platform.streaming.consumer import CDCConsumer

from .helpers import (
    AvroProducer,
    BenchmarkResult,
    CountingSink,
    InstrumentedSink,
    consume_with_sink,
    create_benchmark_topic,
)

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _default_kafka_config() -> KafkaConfig:
    """Single-poll baseline config (current behavior)."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
        poll_batch_size=1,
        deser_pool_size=1,
        commit_interval_seconds=0.0,
    )


def _high_throughput_kafka_config(
    *,
    poll_batch_size: int = 500,
    deser_pool_size: int = 4,
    commit_interval_seconds: float = 5.0,
) -> KafkaConfig:
    """High-throughput config with batch poll, parallel deser, periodic commits."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
        poll_batch_size=poll_batch_size,
        deser_pool_size=deser_pool_size,
        commit_interval_seconds=commit_interval_seconds,
    )


def _produce_messages(topic: str, count: int, platform_config: Any) -> float:
    """Pre-produce Avro messages to a topic. Returns elapsed seconds."""
    producer = AvroProducer(
        topic,
        platform_config.kafka.bootstrap_servers,
        platform_config.kafka.schema_registry_url,
    )
    return producer.produce_batch(count)


# ---------------------------------------------------------------------------
# Phase 2: Batch Polling + Parallel Deserialization
# ---------------------------------------------------------------------------

BATCH_POLL_LOAD = 50_000


@pytest.mark.benchmark
class TestBatchPolling:
    """Compare single-poll vs batch-poll throughput at scale."""

    @pytest.fixture
    def batch_topic(self, kafka_admin: Any, platform_config: Any) -> str:
        topic = f"bench.batch.{uuid.uuid4().hex[:8]}"
        create_benchmark_topic(kafka_admin, topic, num_partitions=4)
        _produce_messages(topic, BATCH_POLL_LOAD, platform_config)
        return topic

    @pytest.mark.parametrize(
        ("label", "batch_size", "deser_pool"),
        [
            ("single-poll", 1, 1),
            ("batch-100", 100, 1),
            ("batch-500", 500, 1),
            ("batch-500-deser4", 500, 4),
        ],
        ids=["single", "batch100", "batch500", "batch500+deser4"],
    )
    async def test_batch_poll_throughput(
        self,
        batch_topic: str,
        label: str,
        batch_size: int,
        deser_pool: int,
        benchmark_report: Any,
    ) -> None:
        """Measure throughput for different poll_batch_size / deser_pool configs."""
        kafka_cfg = KafkaConfig(
            bootstrap_servers="localhost:9092",
            schema_registry_url="http://localhost:8081",
            poll_batch_size=batch_size,
            deser_pool_size=deser_pool,
            commit_interval_seconds=0.0,
        )

        real_sink = CountingSink(expected=BATCH_POLL_LOAD)
        sink = InstrumentedSink(
            real_sink,
            expected_count=BATCH_POLL_LOAD,
            track_e2e_latency=False,
        )

        result = await consume_with_sink(
            topic=batch_topic,
            sink=sink,
            expected_count=BATCH_POLL_LOAD,
            kafka_config=kafka_cfg,
            timeout=120.0,
        )

        result.name = f"batch-poll-{label}-{BATCH_POLL_LOAD // 1000}K"
        benchmark_report.add_result(result)

        logger.info(
            "benchmark.batch_poll.complete",
            label=label,
            messages=result.messages,
            throughput=result.throughput_msg_per_sec,
            duration=result.duration_seconds,
        )


# ---------------------------------------------------------------------------
# Phase 3: Periodic Offset Commits
# ---------------------------------------------------------------------------

COMMIT_LOAD = 50_000


@pytest.mark.benchmark
class TestPeriodicCommits:
    """Compare per-event vs periodic offset commit throughput."""

    @pytest.fixture
    def commit_topic(self, kafka_admin: Any, platform_config: Any) -> str:
        topic = f"bench.commit.{uuid.uuid4().hex[:8]}"
        create_benchmark_topic(kafka_admin, topic, num_partitions=4)
        _produce_messages(topic, COMMIT_LOAD, platform_config)
        return topic

    @pytest.mark.parametrize(
        ("label", "commit_interval"),
        [
            ("per-event", 0.0),
            ("interval-1s", 1.0),
            ("interval-5s", 5.0),
        ],
        ids=["per-event", "1s", "5s"],
    )
    async def test_commit_strategy_throughput(
        self,
        commit_topic: str,
        label: str,
        commit_interval: float,
        benchmark_report: Any,
    ) -> None:
        """Measure throughput impact of commit strategy."""
        kafka_cfg = KafkaConfig(
            bootstrap_servers="localhost:9092",
            schema_registry_url="http://localhost:8081",
            poll_batch_size=500,
            deser_pool_size=4,
            commit_interval_seconds=commit_interval,
        )

        real_sink = CountingSink(expected=COMMIT_LOAD)
        sink = InstrumentedSink(
            real_sink,
            expected_count=COMMIT_LOAD,
            track_e2e_latency=False,
        )

        result = await consume_with_sink(
            topic=commit_topic,
            sink=sink,
            expected_count=COMMIT_LOAD,
            kafka_config=kafka_cfg,
            timeout=120.0,
        )

        result.name = f"commit-{label}-{COMMIT_LOAD // 1000}K"
        benchmark_report.add_result(result)

        logger.info(
            "benchmark.commit.complete",
            label=label,
            messages=result.messages,
            throughput=result.throughput_msg_per_sec,
        )


# ---------------------------------------------------------------------------
# Phase 6: Iceberg Sink with Write Executor
# ---------------------------------------------------------------------------

ICEBERG_LOAD = 20_000


@pytest.mark.benchmark
class TestIcebergHighThroughput:
    """Iceberg sink throughput with write_executor_threads and flush_interval."""

    @pytest.fixture
    def iceberg_topic(self, kafka_admin: Any, platform_config: Any) -> str:
        topic = f"bench.iceberg.ht.{uuid.uuid4().hex[:8]}"
        create_benchmark_topic(kafka_admin, topic, num_partitions=4)
        _produce_messages(topic, ICEBERG_LOAD, platform_config)
        return topic

    @pytest.mark.parametrize(
        ("label", "write_threads", "flush_interval", "batch_size"),
        [
            ("default", 0, 0.0, 1000),
            ("executor-2", 2, 0.0, 1000),
            ("executor-2-flush5s", 2, 5.0, 1000),
            ("executor-4-batch5k", 4, 5.0, 5000),
        ],
        ids=["default", "exec2", "exec2+flush", "exec4+batch5k"],
    )
    async def test_iceberg_throughput(
        self,
        iceberg_topic: str,
        label: str,
        write_threads: int,
        flush_interval: float,
        batch_size: int,
        platform_config: Any,
        benchmark_report: Any,
        tmp_path: Any,
    ) -> None:
        """Measure Iceberg sink throughput under different executor configs."""
        pytest.importorskip("pyiceberg")

        from cdc_platform.sinks.iceberg import IcebergSink

        catalog_uri = f"sqlite:///{tmp_path}/catalog_{label}.db"
        warehouse = f"file://{tmp_path}/warehouse_{label}"

        iceberg_cfg = IcebergSinkConfig(
            catalog_uri=catalog_uri,
            warehouse=warehouse,
            table_name=f"bench_{label.replace('-', '_')}",
            write_mode="append",
            batch_size=batch_size,
            auto_create_table=True,
            write_executor_threads=write_threads,
            flush_interval_seconds=flush_interval,
        )

        sink_cfg = SinkConfig(
            sink_id=f"bench-iceberg-{label}",
            sink_type=SinkType.ICEBERG,
            iceberg=iceberg_cfg,
        )

        real_sink = IcebergSink(sink_cfg)
        sink = InstrumentedSink(
            real_sink,
            expected_count=ICEBERG_LOAD,
            track_e2e_latency=False,
        )

        await sink.start()

        kafka_cfg = _high_throughput_kafka_config()
        result = await consume_with_sink(
            topic=iceberg_topic,
            sink=sink,
            expected_count=ICEBERG_LOAD,
            kafka_config=kafka_cfg,
            timeout=180.0,
        )

        await sink.stop()

        result.name = f"iceberg-{label}-{ICEBERG_LOAD // 1000}K"
        result.latency = sink.latency
        benchmark_report.add_result(result)

        logger.info(
            "benchmark.iceberg.complete",
            label=label,
            messages=result.messages,
            throughput=result.throughput_msg_per_sec,
            p50=sink.latency.p50,
            p99=sink.latency.p99,
        )


# ---------------------------------------------------------------------------
# Combined: Default vs Full High-Throughput at 100K Scale
# ---------------------------------------------------------------------------

FULL_LOAD = 100_000


@pytest.mark.benchmark
class TestDefaultVsHighThroughput:
    """Head-to-head comparison of default vs high-throughput config at 100K."""

    @pytest.fixture
    def ht_topic(self, kafka_admin: Any, platform_config: Any) -> str:
        topic = f"bench.ht.full.{uuid.uuid4().hex[:8]}"
        create_benchmark_topic(kafka_admin, topic, num_partitions=8)
        _produce_messages(topic, FULL_LOAD, platform_config)
        return topic

    @pytest.mark.parametrize(
        ("label", "kafka_cfg"),
        [
            ("default", _default_kafka_config()),
            ("high-throughput", _high_throughput_kafka_config()),
        ],
        ids=["default", "high-throughput"],
    )
    async def test_full_pipeline_throughput(
        self,
        ht_topic: str,
        label: str,
        kafka_cfg: KafkaConfig,
        benchmark_report: Any,
    ) -> None:
        """100K messages: default vs high-throughput config, 8 partitions."""
        real_sink = CountingSink(expected=FULL_LOAD)
        sink = InstrumentedSink(
            real_sink,
            expected_count=FULL_LOAD,
            track_e2e_latency=False,
        )

        result = await consume_with_sink(
            topic=ht_topic,
            sink=sink,
            expected_count=FULL_LOAD,
            kafka_config=kafka_cfg,
            timeout=180.0,
        )

        result.name = f"full-{label}-{FULL_LOAD // 1000}K-8P"
        benchmark_report.add_result(result)

        logger.info(
            "benchmark.full.complete",
            label=label,
            messages=result.messages,
            throughput=result.throughput_msg_per_sec,
            duration=result.duration_seconds,
        )


# ---------------------------------------------------------------------------
# Partition Scaling at High-Throughput Config
# ---------------------------------------------------------------------------

SCALE_LOAD = 50_000


@pytest.mark.benchmark
class TestPartitionScalingHighThroughput:
    """Partition scaling with high-throughput config (vs existing test which uses default)."""

    @pytest.mark.parametrize(
        "num_partitions",
        [1, 4, 8, 16],
        ids=["1P", "4P", "8P", "16P"],
    )
    async def test_partition_scaling_ht(
        self,
        num_partitions: int,
        kafka_admin: Any,
        platform_config: Any,
        benchmark_report: Any,
    ) -> None:
        """Throughput scaling with partitions under high-throughput config."""
        topic = f"bench.scale.ht.{num_partitions}p.{uuid.uuid4().hex[:8]}"
        create_benchmark_topic(kafka_admin, topic, num_partitions=num_partitions)
        _produce_messages(topic, SCALE_LOAD, platform_config)

        kafka_cfg = _high_throughput_kafka_config()

        real_sink = CountingSink(expected=SCALE_LOAD)
        sink = InstrumentedSink(
            real_sink,
            expected_count=SCALE_LOAD,
            track_e2e_latency=False,
        )

        result = await consume_with_sink(
            topic=topic,
            sink=sink,
            expected_count=SCALE_LOAD,
            kafka_config=kafka_cfg,
            timeout=120.0,
        )

        result.name = f"ht-partition-{num_partitions}P-{SCALE_LOAD // 1000}K"
        benchmark_report.add_result(result)

        logger.info(
            "benchmark.partition_scaling.complete",
            num_partitions=num_partitions,
            messages=result.messages,
            throughput=result.throughput_msg_per_sec,
        )


# ---------------------------------------------------------------------------
# Sustained Load with Backpressure (High-Throughput)
# ---------------------------------------------------------------------------

SUSTAINED_LOAD = 50_000


@pytest.mark.benchmark
class TestSustainedBackpressureHighThroughput:
    """Verify backpressure and memory behavior under high-throughput config at scale."""

    @pytest.fixture
    def bp_topic(self, kafka_admin: Any, platform_config: Any) -> str:
        topic = f"bench.bp.ht.{uuid.uuid4().hex[:8]}"
        create_benchmark_topic(kafka_admin, topic, num_partitions=4)
        _produce_messages(topic, SUSTAINED_LOAD, platform_config)
        return topic

    async def test_backpressure_with_slow_sink(
        self,
        bp_topic: str,
        benchmark_report: Any,
    ) -> None:
        """Batch poll + slow sink: verify queue stays bounded at 50K messages."""
        max_buffered = 100
        slow_delay = 0.001  # 1ms per message
        target = 5_000  # Process subset to keep test time reasonable

        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_buffered)
        max_seen_depth = 0
        processed_count = 0

        async def enqueue_handler(
            key: dict[str, Any] | None,
            value: dict[str, Any] | None,
            msg: Any,
        ) -> None:
            await queue.put((key, value, msg))

        async def worker() -> None:
            nonlocal processed_count, max_seen_depth
            while processed_count < target:
                try:
                    await asyncio.wait_for(queue.get(), timeout=10.0)
                except TimeoutError:
                    break
                depth = queue.qsize()
                max_seen_depth = max(max_seen_depth, depth)
                await asyncio.sleep(slow_delay)
                queue.task_done()
                processed_count += 1

        kafka_cfg = _high_throughput_kafka_config()
        kafka_cfg = kafka_cfg.model_copy(
            update={
                "group_id": f"bench-bp-ht-{uuid.uuid4().hex[:8]}",
                "max_poll_interval_ms": 600_000,  # Allow slow sink to block handler
            }
        )

        consumer = CDCConsumer(
            topics=[bp_topic],
            kafka_config=kafka_cfg,
            handler=enqueue_handler,
        )

        start = time.perf_counter()
        consume_task = asyncio.create_task(consumer.consume())
        worker_task = asyncio.create_task(worker())

        await worker_task
        consumer.stop()
        consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consume_task
        # Drain remaining queue items so put() waiters unblock
        while not queue.empty():
            try:
                queue.get_nowait()
                queue.task_done()
            except asyncio.QueueEmpty:
                break

        elapsed = time.perf_counter() - start

        # Queue must never exceed max_buffered (+ small tolerance)
        assert max_seen_depth <= max_buffered + 1, (
            f"Queue exceeded max: {max_seen_depth} > {max_buffered}"
        )

        benchmark_report.add_result(
            BenchmarkResult(
                name=f"bp-ht-slow-sink-{target // 1000}K",
                messages=processed_count,
                duration_seconds=elapsed,
                throughput_msg_per_sec=processed_count / elapsed if elapsed > 0 else 0,
                extra={"max_queue_depth": max_seen_depth},
            )
        )

        logger.info(
            "benchmark.backpressure_ht.complete",
            messages=processed_count,
            max_depth=max_seen_depth,
            throughput=processed_count / elapsed if elapsed > 0 else 0,
        )
