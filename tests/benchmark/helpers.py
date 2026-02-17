"""Helper classes and functions for benchmark tests."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import psycopg2  # type: ignore[import-untyped]
import structlog
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from rich.console import Console
from rich.table import Table

from cdc_platform.config.models import KafkaConfig
from cdc_platform.sinks.base import SinkConnector
from cdc_platform.streaming.consumer import CDCConsumer

logger = structlog.get_logger()


def generate_bulk_customers(dsn: str, count: int) -> float:
    """Insert bulk rows into customers table via psycopg2."""
    logger.info("benchmark.pg_insert.start", count=count)
    start = time.perf_counter()

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            # Clear existing data to avoid UniqueViolation on repeated runs
            cur.execute("TRUNCATE TABLE public.customers CASCADE")
            conn.commit()

            batch_size = 500
            # Generate deterministic data
            data = [
                (
                    i,
                    f"user{i}@example.com",
                    f"User {i}",
                    "2024-01-01 00:00:00",
                    "2024-01-01 00:00:00",
                )
                for i in range(1, count + 1)
            ]

            # Using executemany with batching is faster than individual inserts,
            # but for true bulk COPY is best. Here we use executemany to simulate
            # slightly more realistic app traffic than COPY.
            for i in range(0, count, batch_size):
                batch = data[i : i + batch_size]
                args_str = ",".join(
                    cur.mogrify("(%s, %s, %s, %s, %s)", x).decode("utf-8")
                    for x in batch
                )
                cur.execute(
                    "INSERT INTO public.customers (id, email, full_name, created_at, updated_at) "
                    f"VALUES {args_str}"
                )
                conn.commit()
                if (i + batch_size) % 10000 == 0:
                    logger.info(
                        "benchmark.pg_insert.batch",
                        batch_num=(i // batch_size) + 1,
                        rows_so_far=i + batch_size,
                    )
    finally:
        conn.close()

    elapsed = time.perf_counter() - start
    logger.info(
        "benchmark.pg_insert.complete",
        count=count,
        elapsed_seconds=elapsed,
        rows_per_sec=count / elapsed if elapsed > 0 else 0,
    )
    return elapsed


class AvroProducer:
    """Produces Avro messages directly to Kafka for benchmarking."""

    def __init__(
        self, topic: str, bootstrap_servers: str, schema_registry_url: str
    ) -> None:
        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "linger.ms": 5,
                "batch.num.messages": 1000,
                "enable.idempotence": False,
            }
        )
        registry = SchemaRegistryClient({"url": schema_registry_url})

        # Simplified CDC envelope matching Debezium's output for customers
        value_schema_str = """
        {
            "type": "record",
            "name": "Envelope",
            "namespace": "cdc.public.customers",
            "fields": [
                {"name": "op", "type": "string"},
                {"name": "ts_ms", "type": ["null", "long"], "default": null},
                {"name": "before", "type": ["null", {
                    "type": "record",
                    "name": "Value",
                    "fields": [
                        {"name": "id", "type": "int"},
                        {"name": "email", "type": "string"},
                        {"name": "full_name", "type": "string"},
                        {"name": "created_at", "type": "string"},
                        {"name": "updated_at", "type": "string"}
                    ]
                }], "default": null},
                {"name": "after", "type": ["null", "Value"], "default": null},
                {"name": "source", "type": ["null", {
                    "type": "record",
                    "name": "Source",
                    "fields": [
                        {"name": "version", "type": "string"},
                        {"name": "connector", "type": "string"},
                        {"name": "name", "type": "string"},
                        {"name": "ts_ms", "type": "long"},
                        {"name": "snapshot", "type": ["string", "boolean"]},
                        {"name": "db", "type": "string"},
                        {"name": "sequence", "type": ["null", "string"], "default": null},
                        {"name": "schema", "type": "string"},
                        {"name": "table", "type": "string"},
                        {"name": "txId", "type": ["null", "long"], "default": null},
                        {"name": "lsn", "type": ["null", "long"], "default": null},
                        {"name": "xmin", "type": ["null", "long"], "default": null}
                    ]
                }], "default": null}
            ]
        }
        """

        key_schema_str = """
        {
            "type": "record",
            "name": "Key",
            "namespace": "cdc.public.customers",
            "fields": [
                {"name": "id", "type": "int"}
            ]
        }
        """

        self.value_serializer = AvroSerializer(registry, value_schema_str)
        self.key_serializer = AvroSerializer(registry, key_schema_str)
        logger.info("benchmark.avro_producer.init", topic=topic)

    def produce_batch(self, count: int) -> float:
        """Produce count messages to the topic."""
        logger.info("benchmark.produce.start", topic=self.topic, count=count)
        start = time.perf_counter()

        ctx_val = SerializationContext(self.topic, MessageField.VALUE)
        ctx_key = SerializationContext(self.topic, MessageField.KEY)

        print(f"DEBUG: producing {count} messages to {self.topic}")
        for i in range(count):
            now_ms = int(time.time() * 1000)
            key = {"id": i}
            # Simulate a CREATE event
            after = {
                "id": i,
                "email": f"bench{i}@example.com",
                "full_name": f"Benchmark User {i}",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            }
            source = {
                "version": "2.5.0.Final",
                "connector": "postgresql",
                "name": "cdc_demo",
                "ts_ms": now_ms,
                "snapshot": "false",
                "db": "cdc_demo",
                "schema": "public",
                "table": "customers",
            }
            # Provide non-null values for ALL optional fields for the first message
            # to avoid pa.null() inference which Iceberg v2 doesn't support.
            before = None
            if i == 0:
                before = after
                source.update(
                    {
                        "sequence": "sample",
                        "txId": 1,
                        "lsn": 1,
                        "xmin": 1,
                    }
                )

            value = {
                "op": "c",
                "ts_ms": now_ms,
                "before": before,
                "after": after,
                "source": source,
            }

            self.producer.produce(
                topic=self.topic,
                key=self.key_serializer(key, ctx_key),
                value=self.value_serializer(value, ctx_val),
            )

            if (i + 1) % 1000 == 0:
                self.producer.poll(0)
                elapsed_so_far = time.perf_counter() - start
                logger.info(
                    "benchmark.produce.progress",
                    produced=i + 1,
                    total=count,
                    elapsed=elapsed_so_far,
                )

        print("DEBUG: flushing producer...")
        remaining = self.producer.flush(timeout=10)
        if remaining > 0:
            print(
                f"WARNING: Producer flush timed out with {remaining} messages remaining"
            )
            logger.warning("benchmark.produce.flush_timeout", remaining=remaining)
        else:
            print("DEBUG: producer flushed successfully")

        elapsed = time.perf_counter() - start

        logger.info(
            "benchmark.produce.complete",
            count=count,
            elapsed_seconds=elapsed,
            msgs_per_sec=count / elapsed if elapsed > 0 else 0,
        )
        return elapsed


def create_benchmark_topic(
    admin: AdminClient, topic: str, num_partitions: int = 1
) -> None:
    """Create a topic, deleting it first if it exists."""
    # Check if exists
    cluster_metadata = admin.list_topics(topic)
    if topic in cluster_metadata.topics:
        logger.info("benchmark.topic.delete", topic=topic)
        fs = admin.delete_topics([topic])
        for f in fs.values():
            try:
                f.result(timeout=10)
            except Exception as e:
                logger.warning("benchmark.topic.delete_failed", error=str(e))

        # Wait for actual deletion from metadata
        import time

        for _ in range(20):
            cluster_metadata = admin.list_topics(topic)
            if topic not in cluster_metadata.topics:
                break
            time.sleep(0.5)
        else:
            logger.warning("benchmark.topic.delete_timeout", topic=topic)

    logger.info("benchmark.topic.create", topic=topic, num_partitions=num_partitions)
    print(f"DEBUG: creating topic {topic}")
    new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
    fs = admin.create_topics([new_topic])
    for f in fs.values():
        try:
            f.result(timeout=10)  # Wait for creation with timeout
        except Exception as e:
            # If it still exists (race condition), just log and proceed if possible,
            # but usually we want a fresh topic. For benchmarks, reusing might be bad.
            # However, prompt says "Topic ... already exists"
            if "TOPIC_ALREADY_EXISTS" in str(e):
                logger.warning("benchmark.topic.already_exists", topic=topic)
            else:
                print(f"ERROR: failed to create topic {topic}: {e}")
                raise

    print(f"DEBUG: topic {topic} created")
    logger.info("benchmark.topic.ready", topic=topic)


@dataclass
class LatencyTracker:
    """Tracks latency samples."""

    samples: list[float] = field(default_factory=list)

    def add(self, latency_ms: float) -> None:
        self.samples.append(latency_ms)

    @property
    def count(self) -> int:
        return len(self.samples)

    def percentile(self, p: float) -> float:
        if not self.samples:
            return 0.0
        # Sort on demand (could be optimized but sufficient for benchmark reporting)
        sorted_samples = sorted(self.samples)
        k = int(len(sorted_samples) * (p / 100.0))
        return sorted_samples[min(k, len(sorted_samples) - 1)]

    @property
    def p50(self) -> float:
        return self.percentile(50)

    @property
    def p95(self) -> float:
        return self.percentile(95)

    @property
    def p99(self) -> float:
        return self.percentile(99)

    @property
    def mean(self) -> float:
        if not self.samples:
            return 0.0
        return sum(self.samples) / len(self.samples)

    @property
    def max(self) -> float:
        return max(self.samples) if self.samples else 0.0


class InstrumentedSink:
    """Decorator for SinkConnector that measures write latency."""

    def __init__(self, inner: SinkConnector, expected_count: int | None = None) -> None:
        self.inner = inner
        self.latency = LatencyTracker()  # Sink write latency
        self.e2e_latency = LatencyTracker()  # Producer -> Sink latency
        self.write_count = 0
        self.expected_count = expected_count
        self.done_event = asyncio.Event()
        self.start_time: float | None = None
        self.first_write_time: float | None = None
        self.last_write_time: float | None = None

    @property
    def sink_id(self) -> str:
        return self.inner.sink_id

    @property
    def flushed_offsets(self) -> dict[tuple[str, int], int]:
        return self.inner.flushed_offsets

    async def start(self) -> None:
        logger.info("benchmark.sink.started", sink_id=self.sink_id)
        self.start_time = time.perf_counter()
        await self.inner.start()

    async def write(
        self,
        key: dict[str, Any] | None,
        value: dict[str, Any] | None,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        now_ms = time.time() * 1000
        now_perf = time.perf_counter()

        if self.first_write_time is None:
            self.first_write_time = now_perf
        self.last_write_time = now_perf

        start = time.perf_counter()
        await self.inner.write(key, value, topic, partition, offset)
        write_elapsed_ms = (time.perf_counter() - start) * 1000

        self.latency.add(write_elapsed_ms)
        self.write_count += 1

        # Track E2E latency if value has ts_ms
        if value and "ts_ms" in value:
            # Prefer top-level ts_ms, fall back to source.ts_ms
            prod_ts = value.get("ts_ms") or value.get("source", {}).get("ts_ms")
            if prod_ts:
                self.e2e_latency.add(now_ms - prod_ts)

        if self.write_count % 500 == 0:
            logger.info(
                "benchmark.sink.write",
                sink_id=self.sink_id,
                writes_so_far=self.write_count,
                last_latency_ms=write_elapsed_ms,
            )

        if (
            self.expected_count
            and self.write_count >= self.expected_count
            and not self.done_event.is_set()
        ):
            self.done_event.set()

    async def flush(self) -> None:
        await self.inner.flush()

    async def stop(self) -> None:
        await self.inner.stop()
        if self.start_time:
            # We just want to ensure it's calculated or used if needed, or remove if truly unused.
            # The linter said total_time is unused.
            # But we might want to log it? The original code calculated it but didn't use it in log?
            # Ah, the original code logs latency_p50/p99 but not total duration of sink.
            # Let's just remove the assignment.
            pass

        logger.info(
            "benchmark.sink.stopped",
            sink_id=self.sink_id,
            total_writes=self.write_count,
            latency_p50=self.latency.p50,
            latency_p99=self.latency.p99,
        )

    async def health(self) -> dict[str, Any]:
        return await self.inner.health()


# ... (skipping to consume_with_sink) ...

# We can't reach consume_with_sink cleanly with one replace call if they are far apart
# So I'll do two replace calls.


class SlowSink:
    """Synthetic sink that sleeps for delay_seconds on each write."""

    def __init__(self, delay_seconds: float, sink_id: str = "slow-sink") -> None:
        self._delay = delay_seconds
        self._sink_id = sink_id
        self._offsets: dict[tuple[str, int], int] = {}
        self.write_count = 0

    @property
    def sink_id(self) -> str:
        return self._sink_id

    @property
    def flushed_offsets(self) -> dict[tuple[str, int], int]:
        return self._offsets

    async def start(self) -> None:
        pass

    async def write(
        self,
        key: dict[str, Any] | None,
        value: dict[str, Any] | None,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        await asyncio.sleep(self._delay)
        self._offsets[(topic, partition)] = offset
        self.write_count += 1

        if self.write_count % 50 == 0:
            logger.info(
                "benchmark.slow_sink.write",
                write_count=self.write_count,
                delay_ms=self._delay * 1000,
            )

    async def flush(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def health(self) -> dict[str, Any]:
        return {"status": "ok"}


class CountingSink:
    """Sink that counts messages and signals when expected count reached."""

    def __init__(self, expected: int, sink_id: str = "counting-sink") -> None:
        self.expected = expected
        self.count = 0
        self.done_event = asyncio.Event()
        self._sink_id = sink_id
        self._offsets: dict[tuple[str, int], int] = {}

    @property
    def sink_id(self) -> str:
        return self._sink_id

    @property
    def flushed_offsets(self) -> dict[tuple[str, int], int]:
        return self._offsets

    async def start(self) -> None:
        pass

    async def write(
        self,
        key: dict[str, Any] | None,
        value: dict[str, Any] | None,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        self.count += 1
        self._offsets[(topic, partition)] = offset

        if self.count % 1000 == 0:
            logger.info(
                "benchmark.counting_sink.progress",
                count=self.count,
                expected=self.expected,
                pct_complete=(self.count / self.expected) * 100,
            )

        if self.count >= self.expected and not self.done_event.is_set():
            logger.info("benchmark.counting_sink.done", count=self.count)
            self.done_event.set()

    async def flush(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def health(self) -> dict[str, Any]:
        return {"status": "ok"}


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    name: str
    messages: int
    duration_seconds: float
    throughput_msg_per_sec: float
    conn_rebal_duration: float | None = None
    active_duration: float | None = None
    teardown_duration: float | None = None
    latency: LatencyTracker | None = None
    e2e_latency: LatencyTracker | None = None
    extra: dict[str, Any] = field(default_factory=dict)


class BenchmarkReport:
    """Collects and reports benchmark results."""

    def __init__(self, test_name: str | None = None) -> None:
        self.results: list[BenchmarkResult] = []
        if test_name:
            self.current_test = test_name

    def add_result(self, result: BenchmarkResult) -> None:
        self.results.append(result)

        # Log structured event
        log_kwargs = {
            "name": result.name,
            "messages": result.messages,
            "duration": result.duration_seconds,
            "throughput": result.throughput_msg_per_sec,
        }
        if result.latency:
            log_kwargs.update(
                {
                    "p50": result.latency.p50,
                    "p95": result.latency.p95,
                    "p99": result.latency.p99,
                }
            )
        logger.info("benchmark.result", **log_kwargs)

    def print_summary(self) -> None:
        """Print a summary table of results."""
        if not self.results:
            return

        console = Console()

        # Throughput Table
        table = Table(title="CONSOLIDATED BENCHMARK RESULTS")
        table.add_column("Test", style="cyan")
        table.add_column("Msgs", justify="right")
        table.add_column("Conn/Rebal", justify="right")
        table.add_column("Active", justify="right")
        table.add_column("Teardown", justify="right")
        table.add_column("Duration", justify="right")
        table.add_column("Throughput", justify="right", style="green")

        for r in self.results:
            table.add_row(
                r.name,
                f"{r.messages:,}",
                f"{r.conn_rebal_duration:.2f}s"
                if r.conn_rebal_duration is not None
                else "-",
                f"{r.active_duration:.2f}s" if r.active_duration is not None else "-",
                f"{r.teardown_duration:.2f}s"
                if r.teardown_duration is not None
                else "-",
                f"{r.duration_seconds:.2f}s",
                f"{r.throughput_msg_per_sec:,.0f}/s",
            )

        console.print(table)
        console.print("[dim]Metric Descriptions:[/dim]")
        console.print(
            "[dim]- Conn/Rebal: Kafka connection and rebalance overhead[/dim]"
        )
        console.print("[dim]- Active: Time from first to last message written[/dim]")
        console.print("[dim]- Teardown: Final offset flush and consumer shutdown[/dim]")
        console.print()

        # Latency Table (if any latency results)
        latency_results = [r for r in self.results if r.latency]
        if latency_results:
            lat_table = Table(title="LATENCY DISTRIBUTION (ms)")
            lat_table.add_column("Test", style="cyan")
            lat_table.add_column("Metric", style="magenta")
            lat_table.add_column("p50", justify="right", style="green")
            lat_table.add_column("p95", justify="right", style="yellow")
            lat_table.add_column("p99", justify="right", style="red")

            for r in latency_results:
                if r.latency:
                    lat_table.add_row(
                        r.name,
                        "Sink Write",
                        f"{r.latency.p50:.2f}",
                        f"{r.latency.p95:.2f}",
                        f"{r.latency.p99:.2f}",
                    )
                if r.e2e_latency:
                    lat_table.add_row(
                        "",
                        "End-to-End",
                        f"{r.e2e_latency.p50:.2f}",
                        f"{r.e2e_latency.p95:.2f}",
                        f"{r.e2e_latency.p99:.2f}",
                    )
            console.print(lat_table)
            console.print()


async def consume_with_sink(
    topic: str,
    sink: SinkConnector,
    expected_count: int,
    kafka_config: KafkaConfig,
    timeout: float = 30.0,
) -> BenchmarkResult:
    """Consume from topic into sink until expected count or timeout."""
    logger.info(
        "benchmark.consume.start",
        topic=topic,
        expected_count=expected_count,
        timeout=timeout,
    )

    start = time.perf_counter()

    # Ensure unique group ID for this run to avoid offset interference
    import uuid

    # Create a copy with modified group_id
    # KafkaConfig is a Pydantic model
    config_copy = kafka_config.model_copy()
    config_copy.group_id = f"{kafka_config.group_id}-{uuid.uuid4().hex[:8]}"

    # Wrapper to adapt Message -> sink.write signature
    async def sink_handler(
        key: dict[str, Any] | None, value: dict[str, Any] | None, msg: Any
    ) -> None:
        await sink.write(
            key=key,
            value=value,
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )

    consumer = CDCConsumer(
        topics=[topic],
        kafka_config=config_copy,
        handler=sink_handler,
    )

    # If sink is CountingSink and already finished (rare race), checks done
    if isinstance(sink, CountingSink) and sink.done_event.is_set():
        pass
    else:
        consume_task = asyncio.create_task(consumer.consume())

        try:
            if isinstance(sink, CountingSink):
                await asyncio.wait_for(sink.done_event.wait(), timeout=timeout)
            elif isinstance(sink, InstrumentedSink) and sink.done_event:
                # If InstrumentedSink has a done_event, use it (we added this)
                await asyncio.wait_for(sink.done_event.wait(), timeout=timeout)
            else:
                # Generic wait
                await asyncio.sleep(timeout)
        except TimeoutError:
            logger.warning("benchmark.consume.timeout", expected=expected_count)
        finally:
            consumer.stop()
            await consume_task
            await sink.flush()

    elapsed = time.perf_counter() - start

    # Determine count
    count = 0
    latency = None
    e2e = None
    conn_rebal_dur = None
    active_dur = None
    teardown_dur = None
    if isinstance(sink, CountingSink):
        count = sink.count
    elif isinstance(sink, InstrumentedSink):
        count = sink.write_count
        latency = sink.latency
        e2e = sink.e2e_latency
        if sink.first_write_time and sink.last_write_time:
            conn_rebal_dur = sink.first_write_time - start
            active_dur = sink.last_write_time - sink.first_write_time
            teardown_dur = elapsed - (sink.last_write_time - start)

    return BenchmarkResult(
        name="consume_throughput",
        messages=count,
        duration_seconds=elapsed,
        throughput_msg_per_sec=count / elapsed if elapsed > 0 else 0,
        conn_rebal_duration=conn_rebal_dur,
        active_duration=active_dur,
        teardown_duration=teardown_dur,
        latency=latency,
        e2e_latency=e2e,
    )
