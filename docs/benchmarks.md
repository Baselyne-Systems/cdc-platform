# Benchmark / Scale Test Suite

## 1. Overview

The CDC Benchmark Suite measures the performance characteristics of the platform under various conditions. Unlike unit or integration tests which focus on correctness, these tests focus on:

- **Throughput**: Maximum messages per second the pipeline can handle.
- **Latency**: End-to-end time and per-sink write latency distributions (p50, p95, p99).
- **Backpressure**: System behavior when sinks are slower than the source.
- **Scaling**: Throughput scaling across multiple partitions and consumers.

## 2. Architecture

The benchmark suite lives in `tests/benchmark/` and is independent of the integration test suite. It manages its own Docker lifecycle.

### Key Components

- **Direct Kafka Production**: Most Kafka tests (`test_sink_latency`, `test_backpressure`, `test_multi_partition`) bypass Debezium and produce Avro messages directly to Kafka using `AvroProducer`. This gives precise control over input rate, volume, and data shape.
- **E2E Throughput**: The `test_throughput` module exercises the full path: PostgreSQL INSERTs -> Debezium capture -> Kafka -> Consumer -> Sink.
- **WAL Decoder Benchmarks**: The `test_wal_decoder_throughput` module measures pgoutput binary decoding throughput for the direct WAL reader used by Pub/Sub and Kinesis transports.
- **Transport Benchmarks**: The `test_transport_throughput` module measures virtual partition hashing, naming conventions, WAL publisher throughput (Pub/Sub + Kinesis), JSON serialization, and end-to-end decode-to-publish pipelines with mocked cloud clients.
- **Instrumented Sinks**: Sinks are wrapped in an `InstrumentedSink` decorator that tracks write latency without modifying the core sink code.
- **Process Isolation**: To prevent segmentation faults caused by C-extensions (fastavro/confluent-kafka) during high-volume runs, tests are executed in separate Python processes via the Makefile.

## 3. Prerequisites

- Docker and Docker Compose
- No services bound to ports 5432, 9092, 8081, 8083, or 8080.
- `uv` or `pip` to install dependencies.

For Iceberg benchmarks, `pyiceberg` is required (installed via `uv sync --extra iceberg`).

## 4. Running Benchmarks

Run the full suite:

```bash
make bench
```

This command automatically:
1. Starts the Docker environment.
2. Runs each benchmark module (`backpressure`, `latency`, `partitions`, `throughput`) in a fresh process.
3. Shuts down the Docker environment.

### Filtering Tests

To run specific tests manually, use `pytest` but be aware of possible teardown crashes if running multiple files:

```bash
# Run only latency tests
uv run pytest tests/benchmark/test_sink_latency.py -v -s
```

## 5. Test Descriptions

### `test_throughput.py` (End-to-End)

Measures true end-to-end throughput from database transaction to sink write.

- **Flow**: `INSERT INTO customers` -> Debezium -> Kafka -> `CDCConsumer` -> `CountingSink`.
- **Scenarios**: 100 and 500 rows.
- **Metrics**: Total duration and messages/second.

### `test_sink_latency.py`

Measures write latency distributions for each sink type, isolated from Debezium overhead.

- **Flow**: Pre-produced Kafka messages -> `CDCConsumer` -> `InstrumentedSink(TargetSink)`.
- **Sinks Tested**:
    - **Webhook**: Local asyncio HTTP server.
    - **PostgreSQL**: Real local PostgreSQL instance.
    - **Iceberg**: Local filesystem warehouse with SQLite catalog.
- **Metrics**: p50, p95, p99 latency in milliseconds.

### `test_backpressure.py`

Verifies that the system bounds memory usage when sinks are slow.

- **Scenario**: 
    - SlowSink delays writes by 50ms.
    - Consumer reads from Kafka faster than sink can write.
- **Assertions**:
    - Internal queue depth must not exceed `max_buffered_messages`.
    - Memory usage growth must be bounded (< 50MB).

### `test_multi_partition.py`

Measures how throughput scales with partition count.

- **Scenarios**: 1, 4, and 8 partitions.
- **Flow**: Parallel consumption with one async worker per partition.
- **Metrics**: Aggregate throughput (messages/second).

### `test_wal_decoder_throughput.py` (WAL Decoder)

Measures pgoutput binary decoding throughput for the direct WAL reader.

- **Scenarios**: Pure INSERT (10K/50K/100K), mixed INSERT/UPDATE/DELETE, wide tables (10/50/100 columns), multi-table interleaved, large text values (10KB), null-heavy sparse rows.
- **Metrics**: Messages decoded per second, data rate for large values.
- **Thresholds**: INSERT decoding > 50K/s, wide tables > 5K/s, large values > 500/s.

### `test_transport_throughput.py` (Cloud Transports)

Measures throughput for Pub/Sub and Kinesis transport components using mocked cloud clients.

- **Virtual Partition Hashing**: Hash-to-partition throughput (100K/500K/1M keys), distribution uniformity across 16 partitions.
- **Naming Conventions**: Topic/stream naming roundtrip throughput.
- **WAL Publishers**: PubSubWalPublisher and KinesisWalPublisher publish throughput with mocked clients.
- **JSON Serialization**: Serialization throughput for small/medium/large CDC payloads.
- **E2E Pipeline**: Full WAL decode -> JSON serialize -> mock publish throughput.

## 6. Helper Reference

### `helpers.py`

- **`AvroProducer`**: Produces CDC-compliant Avro messages directly to Kafka.
- **`InstrumentedSink`**: Wraps any `SinkConnector` to measure `write()` latency.
- **`SlowSink`**: Synthetic sink that sleeps in `write()`.
- **`CountingSink`**: Counts messages and signals completion.
- **`LatencyTracker`**: Computes p50/p95/p99 statistics.
- **`BenchmarkReport`**: Collects and prints formatted results.

## 7. Logging

Benchmarks use `structlog` to emit structured events. Use `make bench` (which uses `-s`) to see them in real-time.

Key events:
- `benchmark.produce.progress`
- `benchmark.sink.write`
- `benchmark.backpressure.queue_sample`
- `benchmark.result`

## 8. Interpreting Results

At the end of each module run, a consolidated table is printed:

```text
CONSOLIDATED BENCHMARK RESULTS
Test                                  Msgs   Duration     Throughput
----------------------------------- -------- ---------- --------------
backpressure-delay-0.01s               200      10.40s          19/s
```

## 9. Troubleshooting

- **Port Conflicts**: Ensure `docker compose down` is run if a previous run crashed.
- **Segfaults**: If you see `Segmentation fault (Error 139)`, ensure you are using `make bench`. This is a known issue with the C-extensions when running multiple tests in one process.
- **UniqueViolation**: The `generate_bulk_customers` helper now truncates the table before insertion to prevent primary key conflicts.

## 10. Implementation Notes

To ensure stability on environments like macOS/Docker:
- Process isolation is used in the `Makefile`.
- `os._exit()` is called in `pytest_sessionfinish` to bypass problematic C-extension cleanup.
- `asyncio_default_fixture_loop_scope = "session"` is set in `pyproject.toml` to handle session-scoped async fixtures.
