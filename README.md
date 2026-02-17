# CDC Platform

[![CI](https://github.com/Baselyne-Systems/cdc-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Baselyne-Systems/cdc-platform/actions/workflows/ci.yml)

A modular, extensible CDC platform for streaming changes from operational databases into multiple sink destinations with exactly-once delivery semantics.

## Overview

CDC Platform owns the full pipeline from source database to sink destination. It provisions Kafka topics, deploys Debezium connectors, manages consumer groups and offset lifecycle, monitors schemas via the Confluent Schema Registry, and routes events to configurable sinks — webhooks, PostgreSQL replicas, and Apache Iceberg lakehouse tables. Events are serialized with Avro and delivered with exactly-once guarantees through min-watermark offset commits and idempotent writes.

```
 ┌─────────────────── CDC Platform ──────────────────────┐
 │                                                       │
 │  ┌───────────┐   ┌──────────────────────────────┐     │
 │  │  Debezium │──▸│ Kafka (topics, DLQ, schemas) │     │
 │  │ connector │   └──────────────┬───────────────┘     │
 │  └───────────┘                  │                     │
 │                      consumer poll loop               │
 │                          │                            │
 │              ┌───────────┼───────────┐                │
 │              ▼           ▼           ▼                │
 │         Queue(p0)   Queue(p1)   Queue(p2)             │
 │              │           │           │                │
 │         Worker(p0)  Worker(p1)  Worker(p2)──▸ sinks   │
 │                                                       │
 │         schema monitor     lag monitor                │
 └───────────────────────────────────────────────────────┘
         ▲                                     │
    source DB                          sink destinations:
   (Postgres)                       Webhook, Postgres, Iceberg
```

The source database and sink destinations are the only components outside the platform boundary. The platform provisions Kafka topics, deploys Debezium connectors, manages consumer groups and offsets, monitors schemas, and routes events to sinks.

## Features

- **Multi-sink fan-out** — each CDC event is dispatched concurrently to all enabled sinks
- **Backpressure** — bounded per-partition queues prevent unbounded memory growth when sinks are slow
- **Parallel consumption** — per-partition async workers process independently within a single asyncio process
- **Exactly-once delivery** — min-watermark offset commits ensure no data loss; idempotent upserts handle replay
- **Schema evolution monitoring** — polls Schema Registry for version changes, optionally halts on incompatible changes
- **Dead Letter Queue** — per-sink failures are routed to a DLQ topic with full diagnostic headers
- **Avro serialization** — schema evolution managed by Confluent Schema Registry
- **Retry with backoff** — configurable exponential backoff with jitter on all sink writes
- **Template-based config** — base templates with per-pipeline overrides, validated by Pydantic
- **CLI tooling** — validate configs, deploy connectors, check health, debug-consume, and run pipelines
- **Observability** — structured logging (structlog), health probes, consumer lag metrics per partition

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Docker and Docker Compose

### 1. Start the infrastructure

```bash
make up
```

This starts PostgreSQL (with logical replication), Kafka (KRaft mode), Schema Registry, Debezium Connect, and Kafka UI.

| Service         | Port  | URL                          |
|-----------------|-------|------------------------------|
| PostgreSQL      | 5432  | `localhost:5432`             |
| Kafka           | 9092  | `localhost:9092`             |
| Schema Registry | 8081  | `http://localhost:8081`      |
| Kafka Connect   | 8083  | `http://localhost:8083`      |
| Kafka UI        | 8080  | `http://localhost:8080`      |

### 2. Install the platform

```bash
uv sync                          # core dependencies
uv sync --extra postgres         # + PostgreSQL sink
uv sync --extra iceberg          # + Iceberg sink
uv sync --extra dev              # + dev/test tools
```

Or with pip:

```bash
pip install -e ".[postgres,iceberg,dev]"
```

### 3. Check platform health

```bash
cdc health
```

### 4. Validate and run a pipeline

```bash
cdc validate examples/demo-config.yaml
cdc run examples/demo-config.yaml
```

## Configuration

Configuration is split into two files that enforce a strict boundary between **platform infrastructure** and **pipeline definitions**.

### Pipeline config (`pipeline.yaml`)

This is the only file users need to write. It defines _what_ to capture and _where_ to send it:

```yaml
pipeline_id: demo
topic_prefix: cdc

source:
  database: cdc_demo
  password: cdc_password
  tables:
    - public.customers
    - public.orders

sinks:
  - sink_id: webhook-notifications
    sink_type: webhook
    enabled: true
    webhook:
      url: http://localhost:8000/cdc-events
      method: POST
      headers:
        X-Source: cdc-platform
      timeout_seconds: 10

  - sink_id: analytics-db
    sink_type: postgres
    enabled: false
    postgres:
      host: localhost
      port: 5433
      database: analytics
      target_table: public.cdc_events
      batch_size: 50
      upsert: true

  - sink_id: iceberg-lake
    sink_type: iceberg
    enabled: false
    iceberg:
      catalog_uri: "sqlite:////tmp/cdc_catalog.db"
      warehouse: "file:///tmp/cdc_warehouse"
      table_name: customers_cdc
      write_mode: append
      batch_size: 500
      auto_create_table: true
```

Pipeline configs are validated strictly — including `kafka`, `connector`, or `dlq` keys in a pipeline YAML will raise a validation error. These belong in the platform config.

### Platform config (`platform.yaml`)

Configures the platform's managed infrastructure: Kafka, Debezium Connect, Schema Registry, DLQ behavior, and pipeline tuning. All fields have sensible defaults, so this file is **optional for local development**.

```yaml
kafka:
  bootstrap_servers: kafka-prod:9092
  schema_registry_url: http://registry-prod:8081

connector:
  connect_url: http://connect-prod:8083
```

Only specify fields that differ from the defaults. See `examples/platform.yaml` for a production override example.

### Environment variables

Secrets and environment-specific values use `${VAR}` or `${VAR:-default}` syntax in YAML files:

```yaml
# In pipeline.yaml
source:
  database: ${CDC_SOURCE_DATABASE:-cdc_demo}
  password: ${CDC_SOURCE_PASSWORD:-cdc_password}

# In platform.yaml
kafka:
  bootstrap_servers: ${CDC_KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
```

### Source configuration (external)

The source database that CDC captures changes from. The platform connects to it via Debezium.

| Field              | Default         | Description                              |
|--------------------|-----------------|------------------------------------------|
| `source_type`      | `postgres`      | Source database type (`postgres`, `mysql`) |
| `host`             | `localhost`     | Database host                            |
| `port`             | `5432`          | Database port                            |
| `database`         | *(required)*    | Database name                            |
| `username`         | `cdc_user`      | Database user                            |
| `password`         | `cdc_password`  | Database password                        |
| `tables`           | `[]`            | Schema-qualified tables (e.g. `public.customers`) |
| `slot_name`        | `cdc_slot`      | PostgreSQL replication slot              |
| `snapshot_mode`    | `initial`       | Debezium snapshot mode                   |

### Platform internals

These configure the platform's managed infrastructure. They live in `platform.yaml` (not in pipeline configs). The defaults are production-ready for most deployments.

**Kafka** (`kafka.*`) — The platform provisions topics, manages consumer groups, commits offsets, and monitors schemas through Kafka and the Schema Registry.

| Field                  | Default                       | Description                              |
|------------------------|-------------------------------|------------------------------------------|
| `kafka.bootstrap_servers`   | `localhost:9092`        | Kafka broker(s)                          |
| `kafka.schema_registry_url` | `http://localhost:8081` | Confluent Schema Registry URL            |
| `kafka.group_id`            | `cdc-platform`          | Consumer group ID                        |
| `kafka.auto_offset_reset`   | `earliest`              | Where to start consuming when no committed offset exists |
| `kafka.enable_idempotence`  | `true`                  | Idempotent producer for DLQ writes       |
| `kafka.acks`                | `all`                   | Producer acknowledgment level            |

**Debezium connector** (`connector.*`) — The platform deploys and manages the Debezium source connector via the Kafka Connect REST API.

| Field                        | Default                       | Description                              |
|------------------------------|-------------------------------|------------------------------------------|
| `connector.connect_url`     | `http://localhost:8083`        | Kafka Connect REST endpoint              |
| `connector.timeout_seconds` | `30.0`                         | HTTP timeout for Connect API calls       |
| `connector.retry_max_attempts` | `5`                         | Retries on connector deployment          |
| `connector.retry_wait_seconds` | `2.0`                       | Wait between retries                     |

**Pipeline tuning** — Controls backpressure, parallelism, schema monitoring, and lag reporting.

| Field                            | Default | Description                                            |
|----------------------------------|---------|--------------------------------------------------------|
| `max_buffered_messages`          | `1000`  | Max messages queued **per partition** before backpressure kicks in. Total memory ≈ this × num_partitions × avg_msg_size. |
| `partition_concurrency`          | `0`     | 0 = auto (one async task per assigned partition). Reserved for future use. |
| `schema_monitor_interval_seconds`| `30.0`  | How often to poll Schema Registry for version changes. Lower = faster detection, higher registry load. |
| `lag_monitor_interval_seconds`   | `15.0`  | How often to query and log consumer lag. Creates a short-lived Kafka admin connection on each poll. |
| `stop_on_incompatible_schema`    | `false` | When `true`, halt the pipeline on backward-incompatible schema changes (field removal, type narrowing). When `false`, log and continue. |

**Dead Letter Queue** (`dlq.*`) — Platform-managed error routing for sink write failures.

| Field              | Default | Description                              |
|--------------------|---------|------------------------------------------|
| `dlq.enabled`      | `true`  | Enable DLQ routing                       |
| `dlq.topic_suffix` | `dlq`   | Suffix appended to source topic for DLQ topic name |
| `dlq.max_retries`  | `3`     | Max retries before routing to DLQ        |
| `dlq.include_headers` | `true` | Include diagnostic headers on DLQ messages |

### Sink destinations (external)

Sinks are the external systems the platform writes to. Each sink is independently configured, retried, and monitored — one sink's failure doesn't affect others.

**Webhook** — HTTP POST/PUT/PATCH to any endpoint. Unbuffered (one request per event). Payload format: `{key, value, metadata: {topic, partition, offset}}`.

**PostgreSQL** — Batched inserts to a target table. Columns: `event_key`, `event_value`, `source_topic`, `source_partition`, `source_offset`. Set `upsert: true` for idempotent writes via `ON CONFLICT` (requires a unique constraint on `source_topic, source_partition, source_offset`).

**Apache Iceberg** — Batched appends or upserts to an Iceberg table. CDC metadata is stored in `_cdc_topic`, `_cdc_partition`, `_cdc_offset` columns. Supports auto table creation, partitioning, and S3/local warehouses.

### Retry configuration

All sinks share a retry config (configured in `platform.yaml` as a global default):

| Field                  | Default | Description                    |
|------------------------|---------|--------------------------------|
| `max_attempts`         | `5`     | Maximum retry attempts         |
| `initial_wait_seconds` | `1.0`   | Initial backoff delay          |
| `max_wait_seconds`     | `60.0`  | Maximum backoff delay          |
| `multiplier`           | `2.0`   | Exponential multiplier         |
| `jitter`               | `true`  | Add randomized jitter          |

## CLI

```
cdc validate <pipeline.yaml>                              Validate pipeline config
cdc validate <pipeline.yaml> --platform-config prod.yaml  Validate with custom platform
cdc deploy <pipeline.yaml>                                Register the Debezium connector
cdc deploy <pipeline.yaml> --platform-config prod.yaml    Deploy with custom platform
cdc health                                                Health with defaults
cdc health --platform-config prod.yaml                    Health from platform config
cdc consume <pipeline.yaml>                               Start a debug console consumer
cdc consume <pipeline.yaml> --platform-config prod.yaml   Consume with custom platform
cdc run <pipeline.yaml>                                   Run with default platform
cdc run <pipeline.yaml> --platform-config prod.yaml       Run with custom platform
```

## Architecture

### Platform boundary

The platform manages everything between the source database and the sink destinations. The config separation reinforces this boundary: the **platform config** governs everything inside the boundary (Kafka, Debezium, Schema Registry, DLQ, tuning), while the **pipeline config** governs the external connections (source database, sink destinations).

- **Kafka** — Topics are provisioned automatically from source table names. The platform owns the consumer group, offset lifecycle, and DLQ topics.
- **Schema Registry** — Debezium publishes Avro schemas; the platform monitors them for version changes and compatibility.
- **Debezium** — The platform deploys and manages the source connector via the Kafka Connect REST API.
- **Consumer + dispatch** — The platform's async consumer, per-partition queues, and worker tasks are the processing core.

The source database and sink destinations are the only things outside the platform boundary. The source is read-only (Debezium captures WAL changes). Sinks are write-only (the platform pushes events to them).

### Pipeline lifecycle

1. **Topic provisioning** — The platform auto-creates Kafka topics based on source tables and topic prefix (e.g. `cdc.public.customers`, `cdc.public.customers.dlq`)
2. **Connector deployment** — A Debezium PostgreSQL connector is registered via the Kafka Connect REST API
3. **Sink initialization** — All enabled sinks are started (connections opened, clients initialized)
4. **Schema + lag monitors started** — Background tasks begin polling the Schema Registry and Kafka for version changes and consumer lag
5. **Consume loop** — An async consumer polls Kafka in a background thread, deserializes Avro messages, and enqueues to per-partition bounded queues
6. **Parallel dispatch** — Each partition has its own async worker that drains its queue and dispatches to all sinks concurrently
7. **Offset management** — Offsets are committed at the min-watermark across all sinks (see below)
8. **Graceful shutdown** — On SIGINT/SIGTERM, monitors are stopped, partition workers are cancelled, all sinks are flushed and stopped, and a final offset commit is issued

### Exactly-once delivery

The platform uses **min-watermark offset commits** combined with **idempotent writes** to achieve exactly-once delivery semantics:

- Kafka offsets are **not** committed per-message. Instead, each sink tracks the highest offset it has durably flushed (`flushed_offsets`).
- After each dispatch, the runner computes the **minimum flushed offset** across all sinks for each partition. Only this watermark is committed to Kafka.
- On crash recovery, Kafka re-delivers from the last committed offset. Idempotent sinks (PostgreSQL with `upsert: true`, Iceberg in upsert mode) handle duplicates transparently.

This means a slow sink (e.g. Iceberg with `batch_size=1000`) won't cause data loss for a fast sink (e.g. webhook) — the committed offset is always safe for all sinks.

### Backpressure

The pipeline uses **bounded `asyncio.Queue`s** per partition to prevent unbounded memory growth.

```
Consumer poll loop                    Per-partition workers
     │                                     │
     ├─ poll() ──▸ deserialize             │
     │              │                      │
     │              ▼                      │
     │         await queue.put(msg) ──▸ [Queue maxsize=N] ──▸ worker drains ──▸ sinks
     │              │                      │
     │         blocks if full              │
     │         (backpressure)              │
```

**How it works:** When sinks are slow, queues fill up. `await queue.put()` blocks, which blocks the consumer's poll loop, which causes Kafka to stop fetching new messages. Once sinks catch up and workers drain the queue, the consumer resumes polling.

The queue size per partition is set to `max_buffered_messages` (default: 1000). This is per-partition, not shared — each `(topic, partition)` pair gets its own queue with that maxsize.

**Tuning considerations:**
- **Too small** (e.g. 10) — Frequent backpressure pauses, low throughput. The consumer spends more time blocked than polling.
- **Too large** (e.g. 100,000) — Defeats the purpose. Memory grows proportionally to `max_buffered_messages × num_partitions × avg_message_size`.
- **Kafka session timeout** — If backpressure blocks the poll loop longer than `session.timeout.ms` (default 45s), the broker will consider the consumer dead and trigger a rebalance. Size the queue and sink throughput so the worst-case drain time stays well under this timeout.

### Parallel Consumption

The pipeline runs as a **single-process asyncio** application. Each assigned Kafka partition gets its own async worker task:

```
                              ┌─ Queue(p0) ──▸ Worker(p0) ──▸ dispatch ──▸ sinks
Consumer poll ──▸ enqueue ──▸ ├─ Queue(p1) ──▸ Worker(p1) ──▸ dispatch ──▸ sinks
                              └─ Queue(p2) ──▸ Worker(p2) ──▸ dispatch ──▸ sinks
```

The consumer poll loop only enqueues messages to the correct partition's queue; dispatch happens in parallel per partition. A slow Iceberg flush on partition 0 does not block partition 1.

**Rebalance handling:** When Kafka triggers a rebalance (new consumer joins, existing one leaves), the pipeline uses `on_assign`/`on_revoke` callbacks:

- **`on_assign`** — Creates a new bounded queue + async worker task for each newly assigned partition.
- **`on_revoke`** — Cancels the worker task and discards the queue for each revoked partition. Queued but uncommitted messages are safe to discard because their offsets were never committed — the new partition owner will re-consume them from the last committed offset.

**Why single-process asyncio (not multiprocessing)?** CDC workloads are I/O-bound (network calls to sinks, S3 flushes, HTTP webhooks). Asyncio provides concurrency without the overhead of inter-process communication or GIL workarounds. For CPU-bound transformations, individual sinks can use `loop.run_in_executor()` to offload to a thread pool.

### Schema Evolution

A `SchemaMonitor` background task polls the Confluent Schema Registry for subject version changes. For each topic, both the key and value subjects are checked (`{topic}-key` and `{topic}-value`).

**Detection flow:**

1. On startup, the monitor records the current version for each subject as a baseline — no alerts are raised.
2. On each poll interval (`schema_monitor_interval_seconds`, default 30s), the monitor fetches `/subjects/{subject}/versions/latest` from the registry.
3. If the version has changed since the last observation:
   - A structured log event is emitted: `schema.version_changed` with `subject`, `previous_version`, `new_version`, `schema_id`, and `compatible` fields.
   - If `stop_on_incompatible_schema=True`, the monitor posts the *previous* schema to the registry's `/compatibility/subjects/{subject}/versions/latest` endpoint to check if the old schema is still backward-compatible with the new one. If not (field removed, type narrowed, etc.), it logs `schema.incompatible_change_detected` at ERROR level and calls the pipeline's `stop()` method.
   - If `stop_on_incompatible_schema=False` (default), the version change is logged at INFO level and the pipeline continues.

**How schema changes interact with sinks:**

| Sink | Behavior on schema change |
|------|---------------------------|
| Iceberg | Auto-evolves the table schema (PyIceberg handles column additions natively) |
| PostgreSQL | Column mismatch causes a write error → event routes to DLQ |
| Webhook | Passes through the new payload shape transparently |

The schema monitor provides early warning so operators can act before DLQ volume spikes. With `stop_on_incompatible_schema=True`, the pipeline halts preemptively on breaking changes rather than flooding the DLQ.

**Registry errors** (network failures, 5xx responses) are logged at DEBUG level and silently skipped — the monitor retries on the next interval. The pipeline is never stopped due to a registry connectivity issue.

### Observability

**Consumer lag monitoring** — A `LagMonitor` runs as a periodic async task (interval: `lag_monitor_interval_seconds`, default 15s). It queries Kafka for the committed offset and high watermark of each partition and logs a structured event:

```
event: consumer.lag
total_lag: 1247
partitions:
  - topic: cdc.public.customers, partition: 0, lag: 823, offset: 4177
  - topic: cdc.public.customers, partition: 1, lag: 424, offset: 5576
```

Lag data is also included in the health endpoint response under the `consumer_lag` key.

**Health endpoint** — `Pipeline.health()` returns:

```json
{
  "pipeline_id": "demo",
  "source": {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]},
  "sinks": [
    {"sink_id": "wh1", "status": "running"},
    {"sink_id": "iceberg-lake", "status": "running"}
  ],
  "consumer_lag": [
    {"topic": "cdc.public.customers", "partition": 0, "lag": 823, "offset": 4177}
  ]
}
```

**Structured log events reference:**

| Event | Level | When |
|-------|-------|------|
| `pipeline.partitions_assigned` | INFO | Kafka rebalance assigns partitions |
| `pipeline.partitions_revoked` | INFO | Kafka rebalance revokes partitions |
| `schema.version_changed` | INFO/WARN | Schema Registry version changed (WARN if incompatible) |
| `schema.incompatible_change_detected` | ERROR | Backward-incompatible schema change detected |
| `consumer.lag` | INFO | Periodic lag report |
| `pipeline.sink_write_error` | ERROR | Sink write failed (routed to DLQ) |

### Dead Letter Queue

When a sink write fails (after retries), the event is routed to a DLQ topic (`{source_topic}.dlq`) with diagnostic headers:

- `dlq.source_topic`, `dlq.source_partition`, `dlq.source_offset`
- `dlq.error_type`, `dlq.error_message`, `dlq.stacktrace`
- `dlq.timestamp`, `dlq.sink_id`

Other sinks are not affected by one sink's failure.

### Topic naming

| Topic                       | Purpose                    |
|-----------------------------|----------------------------|
| `{prefix}.{schema}.{table}` | CDC events (e.g. `cdc.public.customers`) |
| `{prefix}.{schema}.{table}.dlq` | Dead letter queue     |

## Project Structure

```
src/cdc_platform/
├── cli.py                          # Typer CLI (validate, deploy, health, consume, run)
├── config/
│   ├── models.py                   # Pydantic configuration models
│   ├── loader.py                   # YAML + template loader
│   ├── templates.py                # Template merging logic
│   └── templates/
│       ├── platform.yaml           # Default platform infrastructure config
│       └── postgres_cdc_v1.yaml    # Default PostgreSQL CDC pipeline template
├── sources/
│   └── debezium/
│       ├── client.py               # Async Kafka Connect REST client
│       └── config.py               # Debezium connector config builder
├── streaming/
│   ├── consumer.py                 # Avro-deserializing Kafka consumer
│   ├── producer.py                 # Idempotent Kafka producer
│   ├── dlq.py                      # Dead Letter Queue handler
│   ├── topics.py                   # Topic naming and admin utilities
│   ├── registry.py                 # Schema Registry integration
│   └── schema_monitor.py           # Schema Registry version change monitor
├── sinks/
│   ├── base.py                     # SinkConnector protocol
│   ├── factory.py                  # Sink factory (type -> class registry)
│   ├── webhook.py                  # HTTP webhook sink
│   ├── postgres.py                 # PostgreSQL batched sink
│   └── iceberg.py                  # Apache Iceberg lakehouse sink
├── pipeline/
│   └── runner.py                   # Pipeline orchestrator (partition queues, backpressure, monitors)
└── observability/
    ├── health.py                   # Component health probes
    └── metrics.py                  # Consumer lag metrics + LagMonitor periodic reporter

docker/
├── docker-compose.yml              # Full local stack
└── postgres/
    └── init.sql                    # Demo schema (customers + orders)

tests/
├── unit/                           # Unit tests (no Docker required)
└── integration/                    # Integration tests (requires Docker stack)
```

## Development

### Setup

```bash
git clone <repo-url>
cd cdc-platform
uv sync --extra dev
```

### Running tests

```bash
make test-unit               # Unit tests only
make test-integration        # Integration tests (requires `make up`)
```

### Linting and formatting

```bash
make lint                    # ruff check + mypy
make fmt                     # ruff format + autofix
```

### Docker stack

```bash
make up                      # Start all services
make down                    # Stop and remove containers
make clean                   # Full cleanup (volumes + orphans)
```

### Adding a new sink

1. Create `src/cdc_platform/sinks/my_sink.py` implementing the `SinkConnector` protocol
2. Implement `sink_id`, `flushed_offsets`, `start()`, `write()`, `flush()`, `stop()`, `health()`
3. Track `_flushed_offsets` — update after durable writes, not on buffering
4. Register the sink type in `sinks/factory.py`
5. Add the corresponding config model to `config/models.py`

## License

Apache 2.0 — see [LICENSE](LICENSE).
