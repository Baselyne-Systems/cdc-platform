# Scaling Guide

This guide covers tuning CDC Platform for high-throughput workloads.

## Architecture Overview

```
                         Batch Poll (N msgs/poll)
                                │
                         Thread Pool Deser
                                │
              ┌─────────────────┼─────────────────┐
              ▼                 ▼                 ▼
         Queue(p0)         Queue(p1)         Queue(pN)
              │                 │                 │
         Worker(p0)        Worker(p1)        Worker(pN)
              │                 │                 │
         Iceberg Sink      Iceberg Sink      Iceberg Sink
         (batched writes   (batched writes   (batched writes
          in thread pool)   in thread pool)   in thread pool)
              │                 │                 │
              └────── Periodic Offset Commit ─────┘
```

**Default behavior (backward-compatible):** single-message polling, synchronous deserialization, per-event offset commits, and blocking sink writes. All new config fields default to values that preserve this behavior.

**High-throughput mode:** batch polling, parallel deserialization in a thread pool, interval-based async commits, and offloaded Iceberg writes. Enabled by setting config values.

## Tuning Knobs

### Kafka Consumer

| Field | Default | Recommended (high-throughput) | Effect |
|-------|---------|-------------------------------|--------|
| `kafka.poll_batch_size` | `1` | `500` | Messages fetched per poll call. Higher = less poll overhead. |
| `kafka.deser_pool_size` | `1` | `4` | Thread pool size for parallel Avro deserialization. `1` = no pool. |
| `kafka.commit_interval_seconds` | `0.0` | `5.0` | `0.0` = sync commit per event. `>0` = periodic async commit at this interval. |
| `kafka.fetch_min_bytes` | `1` | `1024` | Broker waits until this many bytes are available before responding. Reduces round trips. |
| `kafka.topic_num_partitions` | `1` | `64` | More partitions = more parallelism across consumers. |

### Iceberg Sink

| Field | Default | Recommended (high-throughput) | Effect |
|-------|---------|-------------------------------|--------|
| `iceberg.batch_size` | `1000` | `5000–10000` | Rows buffered before flush. Larger = fewer, bigger Parquet files. |
| `iceberg.write_executor_threads` | `0` | `2–4` | `0` = writes on event loop (blocking). `>0` = writes in thread pool. |
| `iceberg.flush_interval_seconds` | `0.0` | `10.0` | `0.0` = flush only at batch_size. `>0` = periodic flush for partial batches. |

### DLQ

| Field | Default | Recommended (high-throughput) | Effect |
|-------|---------|-------------------------------|--------|
| `dlq.flush_interval_seconds` | `0.0` | `5.0` | `0.0` = sync flush per DLQ message. `>0` = non-blocking produce with periodic flush. |

### Pipeline Tuning

| Field | Default | Recommended (high-throughput) | Effect |
|-------|---------|-------------------------------|--------|
| `max_buffered_messages` | `1000` | `10000` | Per-partition queue depth. Higher = more buffering before backpressure. |
| `lag_monitor_interval_seconds` | `15.0` | `30.0` | Reduces overhead from lag monitoring consumer. |

## Scaling Formula

```
throughput ≈ num_partitions × per_partition_rate
```

Each partition is processed by one async worker within a single consumer process. To scale beyond a single process:

- **Horizontal scaling:** Run multiple pipeline replicas (K8s Deployments). Kafka's consumer group protocol distributes partitions across replicas automatically.
- **Vertical scaling:** Increase `deser_pool_size` and `write_executor_threads` to use more CPU cores within a single process.

## Capacity Planning

**Memory:**
```
memory ≈ max_buffered_messages × num_partitions × avg_event_bytes
```

Example: 10,000 buffer × 64 partitions × 1 KB avg = ~640 MB buffered.

**CPU:** Avro deserialization is CPU-bound. With `deser_pool_size=4`, up to 4 messages are deserialized concurrently in threads. Iceberg writes with `write_executor_threads > 0` also consume CPU for Arrow conversion and Parquet encoding.

## At-Least-Once Tradeoff

When `commit_interval_seconds > 0`, offsets are committed periodically instead of per-event. On crash, events since the last commit are redelivered. The commit interval directly controls the redelivery window:

- `commit_interval_seconds=0.0` — per-event commit, minimal redelivery (current behavior)
- `commit_interval_seconds=5.0` — up to 5 seconds of events may be redelivered on crash

Idempotent sinks (Iceberg upsert, PostgreSQL upsert) handle duplicates transparently. For non-idempotent sinks (webhooks), keep the interval low or at `0.0`.

## High-Throughput Profile

A pre-built profile is available at `src/cdc_platform/config/defaults/platform-high-throughput.yaml`:

```yaml
kafka:
  topic_num_partitions: 64
  topic_replication_factor: 3
  fetch_min_bytes: 1024
  poll_batch_size: 500
  deser_pool_size: 4
  commit_interval_seconds: 5.0

dlq:
  flush_interval_seconds: 5.0

max_buffered_messages: 10000
lag_monitor_interval_seconds: 30.0
```

Use it as a starting point:

```bash
cdc run pipeline.yaml --platform-config src/cdc_platform/config/defaults/platform-high-throughput.yaml
```

## Iceberg-Specific Tuning

### Batch Size

Larger `batch_size` produces fewer, larger Parquet files — better for query performance and reduces Iceberg metadata overhead. However, larger batches increase memory usage and flush latency.

| batch_size | Parquet files/hour (1K eps) | Notes |
|------------|----------------------------|-------|
| 1,000 | 3,600 | Many small files, needs frequent compaction |
| 5,000 | 720 | Good balance |
| 10,000 | 360 | Fewer files, higher memory |

### Write Executor

Setting `write_executor_threads > 0` offloads blocking PyIceberg I/O (Parquet encoding, S3 upload) to a thread pool. This prevents the asyncio event loop from stalling during writes.

### Flush Interval

When `flush_interval_seconds > 0`, a background task periodically flushes partial batches. This prevents data staleness when event rates are low or bursty — without it, events sit in the buffer until `batch_size` is reached.

### Maintenance

Enable table maintenance for high-throughput workloads:

```yaml
iceberg:
  maintenance:
    enabled: true
    compaction_interval_seconds: 3600
    compaction_file_threshold: 10
    expire_snapshots_interval_seconds: 3600
    expire_snapshots_older_than_seconds: 86400
```
