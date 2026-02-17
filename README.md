# CDC Platform

A production-grade Change Data Capture platform that streams database changes through Kafka to multiple sink destinations with exactly-once delivery semantics.

## Overview

CDC Platform captures row-level changes from PostgreSQL (via Debezium) and fans them out to configurable sink destinations — webhooks, PostgreSQL replicas, and Apache Iceberg lakehouse tables. Events are serialized with Avro, schema-managed via Confluent Schema Registry, and delivered with exactly-once guarantees through min-watermark offset commits and idempotent writes.

```
┌──────────┐    ┌───────┐    ┌─────────────────┐    ┌──────────────┐
│ Postgres │───▸│ Kafka │───▸│  CDC Platform   │───▸│ Webhook      │
│ (source) │    │       │    │  (consumer +    │───▸│ PostgreSQL   │
│          │    │       │    │   fan-out)       │───▸│ Iceberg      │
└──────────┘    └───────┘    └─────────────────┘    └──────────────┘
     ▲                              │
  Debezium                     DLQ on failure
  connector                    (per-sink)
```

## Features

- **Multi-sink fan-out** — each CDC event is dispatched concurrently to all enabled sinks
- **Exactly-once delivery** — min-watermark offset commits ensure no data loss; idempotent upserts handle replay
- **Dead Letter Queue** — per-sink failures are routed to a DLQ topic with full diagnostic headers
- **Avro serialization** — schema evolution managed by Confluent Schema Registry
- **Retry with backoff** — configurable exponential backoff with jitter on all sink writes
- **Template-based config** — base templates with per-pipeline overrides, validated by Pydantic
- **CLI tooling** — validate configs, deploy connectors, check health, debug-consume, and run pipelines
- **Observability** — structured logging (structlog), health probes, consumer lag metrics

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

Pipelines are defined in YAML. Base templates provide sensible defaults; per-pipeline configs override only what's needed.

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

### Environment variables

Platform settings can also be configured via environment variables with the `CDC_` prefix:

```bash
CDC_SOURCE__HOST=localhost
CDC_SOURCE__PORT=5432
CDC_SOURCE__DATABASE=cdc_demo
CDC_KAFKA__BOOTSTRAP_SERVERS=localhost:9092
CDC_KAFKA__SCHEMA_REGISTRY_URL=http://localhost:8081
CDC_CONNECTOR__CONNECT_URL=http://localhost:8083
CDC_LOG_LEVEL=INFO
```

See `.env.example` for the full template.

### Source configuration

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

### Pipeline configuration

| Field                            | Default | Description                                            |
|----------------------------------|---------|--------------------------------------------------------|
| `max_buffered_messages`          | `1000`  | Max messages queued per partition before backpressure   |
| `partition_concurrency`          | `0`     | 0 = auto (one task per assigned partition)             |
| `schema_monitor_interval_seconds`| `30.0`  | How often to poll Schema Registry for changes          |
| `lag_monitor_interval_seconds`   | `15.0`  | How often to log consumer lag                          |
| `stop_on_incompatible_schema`    | `false` | Halt pipeline on backward-incompatible schema change   |

### Sink types

**Webhook** — HTTP POST/PUT/PATCH to any endpoint. Unbuffered (one request per event). Payload format: `{key, value, metadata: {topic, partition, offset}}`.

**PostgreSQL** — Batched inserts to a target table. Columns: `event_key`, `event_value`, `source_topic`, `source_partition`, `source_offset`. Set `upsert: true` for idempotent writes via `ON CONFLICT` (requires a unique constraint on `source_topic, source_partition, source_offset`).

**Apache Iceberg** — Batched appends or upserts to an Iceberg table. CDC metadata is stored in `_cdc_topic`, `_cdc_partition`, `_cdc_offset` columns. Supports auto table creation, partitioning, and S3/local warehouses.

### Retry configuration

All sinks share a retry config:

| Field                  | Default | Description                    |
|------------------------|---------|--------------------------------|
| `max_attempts`         | `5`     | Maximum retry attempts         |
| `initial_wait_seconds` | `1.0`   | Initial backoff delay          |
| `max_wait_seconds`     | `60.0`  | Maximum backoff delay          |
| `multiplier`           | `2.0`   | Exponential multiplier         |
| `jitter`               | `true`  | Add randomized jitter          |

## CLI

```
cdc validate <config>   Validate a pipeline configuration file
cdc deploy <config>     Register the Debezium connector
cdc health              Check health of Kafka, Schema Registry, and Connect
cdc consume <config>    Start a debug console consumer for CDC events
cdc run <config>        Run the full pipeline (source -> Kafka -> sinks)
```

## Architecture

### Pipeline lifecycle

1. **Topic creation** — CDC topics are auto-created based on the source tables and topic prefix (e.g. `cdc.public.customers`)
2. **Connector deployment** — A Debezium PostgreSQL connector is registered via the Kafka Connect REST API
3. **Sink initialization** — All enabled sinks are started (connections opened, clients initialized)
4. **Consume loop** — An async consumer polls Kafka in a background thread, deserializes Avro messages, and enqueues to per-partition bounded queues
5. **Parallel dispatch** — Each partition has its own async worker that drains its queue and dispatches to all sinks concurrently
6. **Offset management** — Offsets are committed at the min-watermark across all sinks (see below)
7. **Schema monitoring** — A background task polls Schema Registry for version changes, logging structured events and optionally halting on incompatible changes
8. **Lag monitoring** — Consumer lag per partition is periodically queried and logged
9. **Graceful shutdown** — On SIGINT/SIGTERM, monitors are stopped, partition workers are cancelled, all sinks are flushed and stopped, and a final offset commit is issued

### Exactly-once delivery

The platform uses **min-watermark offset commits** combined with **idempotent writes** to achieve exactly-once delivery semantics:

- Kafka offsets are **not** committed per-message. Instead, each sink tracks the highest offset it has durably flushed (`flushed_offsets`).
- After each dispatch, the runner computes the **minimum flushed offset** across all sinks for each partition. Only this watermark is committed to Kafka.
- On crash recovery, Kafka re-delivers from the last committed offset. Idempotent sinks (PostgreSQL with `upsert: true`, Iceberg in upsert mode) handle duplicates transparently.

This means a slow sink (e.g. Iceberg with `batch_size=1000`) won't cause data loss for a fast sink (e.g. webhook) — the committed offset is always safe for all sinks.

### Backpressure

The pipeline uses **bounded `asyncio.Queue`s** per partition to prevent unbounded memory growth. When sinks are slow, queues fill up, `await queue.put()` blocks, which blocks the consumer poll loop, which causes Kafka to stop fetching. Queue size is controlled by the `max_buffered_messages` config option (default: 1000).

### Parallel Consumption

Each assigned Kafka partition gets its own async worker task. The consumer poll loop only enqueues messages; dispatch happens in parallel per partition. This means a slow Iceberg flush on partition 0 doesn't block partition 1. On Kafka rebalance, workers are created/cancelled via `on_assign`/`on_revoke` callbacks.

### Schema Evolution

A `SchemaMonitor` background task polls the Confluent Schema Registry for subject version changes. On change:
- A structured log event is emitted with the previous/new version and compatibility status
- When `stop_on_incompatible_schema=True`, the monitor checks the registry's compatibility endpoint. If a change is not backward-compatible (field removed, type narrowed), the pipeline is signalled to stop.
- When `stop_on_incompatible_schema=False` (default), changes are logged but the pipeline continues.

### Observability

- **Consumer lag** — A `LagMonitor` periodically queries Kafka for consumer group lag per partition and logs structured events. Lag data is also included in the health endpoint response.
- **Structured logging** — All events (schema changes, lag, partition assignment, backpressure) are logged via structlog with machine-parseable fields.

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
│       └── postgres_cdc_v1.yaml    # Default PostgreSQL CDC template
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
│   └── runner.py                   # Pipeline orchestrator (fan-out + watermark commits)
└── observability/
    ├── health.py                   # Component health probes
    └── metrics.py                  # Consumer lag metrics

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
