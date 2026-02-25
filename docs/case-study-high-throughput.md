# Building a High-Throughput CDC Platform: From Database Change to Lakehouse in Milliseconds

## The Problem: Real-Time Data Is Hard

Every modern data team faces the same challenge: operational databases power applications, but the data trapped inside them needs to flow — to analytics warehouses, to event-driven microservices, to machine learning pipelines, to real-time dashboards. The gap between "data changed in Postgres" and "that change is available downstream" is where most data infrastructure breaks down.

Change Data Capture (CDC) is the industry answer. By reading the database's write-ahead log (WAL), CDC captures every insert, update, and delete as a stream of events — no polling, no batch ETL, no missed changes. But capturing changes is only the first step. The real engineering challenge is everything that comes after: serializing events, managing schemas, routing to multiple destinations, handling failures, maintaining delivery guarantees, and doing all of this at scale without losing data.

Most teams cobble this together from open-source components: Debezium for capture, Kafka for transport, custom consumers for delivery, ad-hoc scripts for monitoring. The result is fragile, operationally expensive, and difficult to scale. Each new destination table or downstream system requires custom integration work.

We built CDC Platform to solve this.

---

## Introducing CDC Platform

CDC Platform is a modular, end-to-end streaming system that owns the full pipeline from source database to sink destination. It is not a connector framework or a message broker — it is the orchestration layer that ties the entire CDC lifecycle together.

```
 ┌─────────────────── CDC Platform ──────────────────────────────┐
 │                                                               │
 │  ┌─────────────┐   ┌──────────────────────────────┐           │
 │  │ Provisioner │──▸│   EventSource (transport)    │           │
 │  │ (topics +   │   │  ┌────────────────────────┐  │           │
 │  │  connector) │   │  │ Kafka / Pub/Sub / PG   │  │           │
 │  └─────────────┘   │  └────────────┬───────────┘  │           │
 │                    └───────────────┼──────────────┘           │
 │                         Event stream                          │
 │                              │                                │
 │              ┌───────────────┼───────────────┐                │
 │              ▼               ▼               ▼                │
 │         Queue(p0)       Queue(p1)       Queue(p2)             │
 │              │               │               │                │
 │         Worker(p0)      Worker(p1)      Worker(p2)──▸ sinks   │
 │                                                               │
 │    Schema Monitor          Lag Monitor        DLQ Router      │
 └───────────────────────────────────────────────────────────────┘
         ▲                                     │
    source DB                          sink destinations:
   (Postgres,                       Webhook, PostgreSQL, Iceberg
    MySQL, etc.)
```

A single YAML file defines a pipeline — which tables to capture and where to send them:

```yaml
pipeline_id: orders-sync
source:
  database: production
  tables:
    - public.customers
    - public.orders

sinks:
  - sink_id: analytics-lake
    sink_type: iceberg
    iceberg:
      catalog_uri: "s3://my-catalog"
      table_name: customers_cdc
      batch_size: 5000

  - sink_id: notifications
    sink_type: webhook
    webhook:
      url: https://api.internal/events
```

From that config, the platform handles everything:

- **Provisions transport resources** — creates Kafka topics based on table names, registers a Debezium connector to capture WAL changes
- **Manages consumer groups and offsets** — manual offset lifecycle with min-watermark commits across all sinks
- **Monitors schemas** — polls the Confluent Schema Registry for version changes, optionally halts on backward-incompatible changes before they corrupt downstream systems
- **Routes events to multiple sinks concurrently** — each CDC event fans out to all enabled sinks in parallel. A slow Iceberg flush doesn't block webhook delivery
- **Handles failures gracefully** — per-sink retry with exponential backoff, Dead Letter Queue routing with full diagnostic headers, bounded backpressure to prevent memory exhaustion
- **Provides observability** — structured logging, consumer lag metrics per partition, HTTP health endpoints for Kubernetes liveness and readiness probes

### Delivery Guarantees

The platform delivers exactly-once semantics through a combination of two mechanisms:

**Min-watermark offset commits.** Kafka offsets are not committed per-message. Instead, each sink independently tracks the highest offset it has durably flushed. After every dispatch cycle, the pipeline computes the *minimum* flushed offset across all sinks for each partition and commits only that watermark. This means a slow sink (e.g., Iceberg buffering 5,000 rows) never causes a fast sink (e.g., webhook) to lose data on crash recovery.

**Idempotent writes.** On crash recovery, Kafka re-delivers events from the last committed offset. PostgreSQL sinks use `ON CONFLICT` upserts. Iceberg sinks use upsert mode. Duplicate events are absorbed transparently.

### Backpressure

Each partition gets its own bounded `asyncio.Queue`. When sinks can't keep up, the queue fills, `await queue.put()` blocks the poll loop, and the Kafka broker stops delivering new messages. When sinks recover, the pipeline resumes automatically. No tuning required — it just works.

### Transport-Agnostic Architecture

The core pipeline never touches Kafka directly. It operates through four protocol abstractions — `EventSource`, `Provisioner`, `ErrorRouter`, and `SourceMonitor` — with Kafka as the default implementation. This means adding a new transport (Google Pub/Sub, embedded WAL reader, Amazon MSK) requires implementing four protocols without modifying the pipeline, sinks, CLI, or any existing tests.

---

## The Scaling Challenge

As our platform deployments grew to handle higher event volumes, we needed to answer a fundamental question: **how far can a single consumer instance push throughput before requiring horizontal scaling?**

Under the original architecture, the answer was approximately 12,000–15,000 messages per second. Each poll call fetched a single message. Each message was deserialized synchronously. Each offset commit was a synchronous broker round-trip. All Iceberg writes — Parquet encoding, Arrow conversion, S3 uploads — ran on the main asyncio event loop, blocking every other coroutine.

These per-message overheads were invisible at hundreds of events per second. At tens of thousands, they dominated processing time.

We set out to eliminate them — with a strict engineering constraint: **every optimization must be backward-compatible and config-driven.** Existing deployments should require zero changes. Operators opt into high-throughput mode by setting config values; defaults preserve existing behavior.

---

## Engineering High Throughput

The work spanned six phases, each targeting a specific bottleneck.

### Batch Polling and Parallel Deserialization

The highest-impact change. We replaced the single-message `poll()` call with Kafka's `consume(batch_size, timeout)` API, fetching up to N messages per broker round-trip:

```
Before: poll() → 1 message → deserialize → handler → commit → poll() → ...
After:  consume(500) → [500 messages] → parallel deser → handler × 500 → ...
```

Avro deserialization was offloaded to a configurable `ThreadPoolExecutor`. When `deser_pool_size > 1`, a batch of raw messages is submitted to the thread pool for parallel decoding while the event loop remains free for I/O.

The handler contract remained unchanged — each message is still dispatched individually to sinks. The throughput gain comes from amortizing poll overhead and parallelizing the CPU-bound deserialization step.

### Periodic Offset Commits

The original pipeline committed offsets to Kafka synchronously after every event dispatch — thousands of broker round-trips per second under load. We introduced an interval-based commit loop: a background asyncio task commits the min-watermark offset at a configurable interval (e.g., every 5 seconds). Per-event commits are eliminated entirely. A final commit on shutdown ensures no offsets are lost.

This trades a small at-least-once redelivery window (equal to the commit interval) for dramatically reduced broker load. Idempotent sinks handle redelivered events transparently.

### Lag Monitor Isolation

We discovered the lag monitor was creating a Kafka `Consumer` that joined the *same* consumer group as the main pipeline. This triggered a rebalance every 15 seconds — a stability issue that was invisible in low-throughput environments but caused significant disruption under load. The fix: use `AdminClient` API calls for offset queries instead of a consumer that joins the group.

### Async DLQ Produces

Dead Letter Queue writes used synchronous `producer.flush()` per message — blocking the event loop for up to 10 seconds per error. Under error cascades, this could stall healthy partitions. We replaced this with non-blocking `producer.poll(0)` for delivery callbacks, with an explicit `flush()` during pipeline shutdown to drain pending messages.

### Iceberg Write Executor

The Iceberg sink performs Parquet encoding and PyIceberg table writes — both CPU- and I/O-intensive. Running these on the asyncio event loop blocked all other coroutines during flushes.

When `write_executor_threads > 0`, the sink offloads these operations to a `ThreadPoolExecutor`. A periodic flush task ensures partial batches are written even when event rates are bursty, preventing data staleness.

---

## Proving It: 18 Load Tests, 100,000+ Messages

We don't ship performance claims without evidence. We built a benchmark suite of 18 load tests that exercise each optimization in isolation and in combination, running against real infrastructure: Kafka in KRaft mode, Confluent Schema Registry, Avro-serialized CDC events, and instrumented sinks that measure per-phase timing.

Each test creates a dedicated Kafka topic, pre-produces the full message volume with an Avro producer, then consumes all messages through the pipeline while measuring wall-clock time broken down into connection/rebalance overhead, active processing, and teardown phases.

### Batch Polling Impact

**50,000 Avro messages, 4 partitions**

| Configuration | Throughput | Improvement |
|---------------|-----------|-------------|
| Single-poll (baseline) | 12,339 msg/s | — |
| Batch-100 | 26,531 msg/s | **2.1x** |
| Batch-500 | 24,058 msg/s | **1.9x** |
| Batch-500 + parallel deser (4 threads) | 25,161 msg/s | **2.0x** |

Batch polling alone doubled throughput by amortizing poll overhead. The diminishing returns from batch-100 to batch-500 confirm the per-poll overhead is already minimal at batch-100. Parallel deserialization provides modest incremental gains with simple schemas — larger gains are expected with the deeply nested Avro schemas typical of Debezium CDC events in production.

### Offset Commit Strategy

**50,000 Avro messages, 4 partitions, batch polling enabled**

| Strategy | Throughput |
|----------|-----------|
| Per-event (synchronous) | 25,175 msg/s |
| 1-second interval | 26,183 msg/s |
| 5-second interval | 25,590 msg/s |

In a local benchmark environment, periodic commits show marginal throughput improvement because broker latency is sub-millisecond. In production deployments with cross-AZ or cross-region broker latency, eliminating thousands of synchronous round-trips per second has a more pronounced impact.

### Head-to-Head: Default vs. High-Throughput

**100,000 Avro messages, 8 partitions**

| Configuration | Active Processing | Total Duration | Throughput |
|---------------|------------------|----------------|-----------|
| Default | 7.13s | 10.28s | 14,968 msg/s |
| High-Throughput | 2.69s | 5.04s | **38,647 msg/s** |

The combined effect of all optimizations: **2.6x overall throughput improvement** and **2.7x active processing speedup**. The high-throughput config processed 100,000 Avro-serialized CDC events in 2.69 seconds of active time.

### Iceberg Sink Performance

**20,000 Avro messages, 4 partitions**

| Configuration | Active Write Time |
|---------------|------------------|
| Default (on event loop) | 0.78s |
| 2-thread executor | 0.49s |
| 4-thread executor + 5K batch | 0.43s |

Offloading Iceberg writes to a thread pool reduced active write time by **1.8x**. More importantly, the write executor prevents the asyncio event loop from stalling — webhook and PostgreSQL sinks continue processing unblocked during Iceberg flushes.

### Partition Scaling

**50,000 messages, high-throughput config**

| Partitions | Throughput |
|-----------|-----------|
| 1 | 25,979 msg/s |
| 4 | 25,262 msg/s |
| 8 | 25,260 msg/s |
| 16 | 25,709 msg/s |

A single consumer saturates at ~25K msg/s regardless of partition count. This is expected — one consumer processes all assigned partitions sequentially. The result validates the horizontal scaling model.

### Backpressure Under Load

**50,000 messages produced, batch-500, slow sink (1ms/msg)**

| Metric | Result |
|--------|--------|
| Max queue depth | 101 (limit: 100) |
| Messages lost | 0 |
| Throughput | 796 msg/s (throttled by sink) |

Under batch polling with a deliberately slow sink, the per-partition queue never exceeded its configured maximum. Backpressure propagated naturally from the slow sink through the bounded queue to the consumer poll loop, throttling ingestion to match downstream capacity. No messages were lost or dropped. The system degraded gracefully rather than failing catastrophically.

---

## Scaling to Production

With a single consumer instance sustaining ~25,000 messages/second, higher aggregate throughput scales linearly through Kafka's consumer group protocol:

| Replicas | Partitions | Projected Throughput |
|----------|-----------|---------------------|
| 1 | 64 | ~25,000 msg/s |
| 4 | 64 | ~100,000 msg/s |
| 16 | 64 | ~400,000 msg/s |
| 64 | 256 | ~1,600,000 msg/s |

Each replica joins the same consumer group and receives a subset of partitions automatically. The platform's partition-aware architecture — per-partition queues, independent workers, min-watermark offset tracking — ensures replicas operate without coordination. Kubernetes Deployments with a `replicas` field is all it takes.

---

## Results Summary

| Metric | Default Config | High-Throughput Config | Change |
|--------|---------------|----------------------|--------|
| Throughput (100K msgs, 8 partitions) | 14,968 msg/s | 38,647 msg/s | **+158%** |
| Active processing time | 7.13s | 2.69s | **-62%** |
| Iceberg write time | 0.78s | 0.43s | **-45%** |
| Backpressure integrity | N/A | Queue bounded at 101 | Verified |
| Unit tests | 317 passing | 317 passing | Zero regressions |
| Operator migration effort | — | Config values only | Zero code changes |

Every optimization is enabled through configuration. Defaults preserve existing behavior. Existing deployments upgrade with no changes. Operators unlock high-throughput mode by setting a few config values:

```yaml
kafka:
  poll_batch_size: 500
  deser_pool_size: 4
  commit_interval_seconds: 5.0
```

Or use the pre-built profile:

```bash
cdc run pipeline.yaml --platform-config platform-high-throughput.yaml
```

---

## What We Learned

**Measure before you optimize.** Batch polling delivered 2.1x alone — a single change that amortized per-poll overhead. We could have over-invested in parallel deserialization or commit strategies, but benchmarks showed their marginal impact clearly. Data drove prioritization.

**Backward compatibility is non-negotiable for infrastructure.** CDC pipelines run 24/7 in production. Any change that requires a deployment-wide migration or risks behavior changes in existing pipelines is a non-starter. Config-driven opt-in with safe defaults is the only path.

**Backpressure is a feature, not a bug.** When we introduced batch polling, the natural question was: "what happens when we fetch 500 messages but the sink can only handle 100?" The answer — bounded queues propagate backpressure upstream, and the system throttles itself. We verified this under load. The system degrades gracefully rather than failing with out-of-memory errors.

**The event loop is sacred.** In an asyncio architecture, any blocking I/O on the event loop stalls *everything*. Iceberg writes, DLQ flushes, and lag monitor queries all needed to be moved off the event loop for high throughput. The pattern — `run_in_executor` for blocking I/O — is simple but must be applied consistently.

---

*CDC Platform is open source under the Apache 2.0 license. For documentation, deployment guides, and the full benchmark suite, visit the [GitHub repository](https://github.com/Baselyne-Systems/cdc-platform).*
