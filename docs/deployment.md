# Deployment Guide

This guide covers deploying CDC Platform in production on Kubernetes. It covers the shared platform infrastructure, individual pipeline workers, health monitoring, and operational best practices.

## Architecture Overview

CDC Platform follows a **two-phase deployment model**:

1. **Platform** (deployed once) — shared infrastructure that all pipelines depend on: Kafka, Schema Registry, and Kafka Connect with Debezium plugins.
2. **Pipelines** (deployed on demand) — each pipeline is a `cdc run` process with its own config. It auto-registers its Debezium connector on the shared Kafka Connect, consumes CDC events from Kafka, and routes them to sinks.

```
 Source DB ──▸ Debezium (Kafka Connect) ──▸ Kafka ──▸ Pipeline Worker ──▸ Sinks
              ─────── Platform (shared) ──────        ── Per-pipeline ──
```

The platform Helm chart deploys the shared infrastructure. Each pipeline runs as a separate Kubernetes Deployment using the pipeline Docker image.

### What the Platform Chart Deploys

| Component | Purpose |
|-----------|---------|
| Kafka (Bitnami sub-chart) | Message broker for CDC events |
| Schema Registry (Bitnami sub-chart) | Avro schema storage and compatibility checks |
| Kafka Connect | Hosts Debezium connectors that capture WAL changes |

### What a Pipeline Is

A pipeline is a single `cdc run` process that:
- Registers a Debezium connector for its source database on the shared Kafka Connect
- Consumes CDC events from Kafka topics
- Routes events to configured sinks (webhooks, PostgreSQL, Iceberg)
- Exposes health endpoints for Kubernetes probes

Each pipeline runs independently. You deploy one Kubernetes Deployment per pipeline.

---

## Platform Deployment

### Prerequisites

- Kubernetes cluster (1.26+)
- `kubectl` configured for your cluster
- Helm 3.12+
- Container registry access (ghcr.io or your own)

### Using the Helm Chart

```bash
# Add the Bitnami dependency charts
cd helm/cdc-platform
helm dependency update

# Install with default values
helm install cdc-platform helm/cdc-platform/

# Install with custom values
helm install cdc-platform helm/cdc-platform/ -f my-values.yaml

# Upgrade
helm upgrade cdc-platform helm/cdc-platform/ -f my-values.yaml
```

### Configuration Reference

#### Kafka

```yaml
kafka:
  enabled: true          # Set false for external Kafka
  replicaCount: 3        # Broker count (3+ for production)
```

When `kafka.enabled=false`, pipelines connect to an external Kafka cluster. Set `kafka.externalBootstrapServers` in your values or configure it in each pipeline's platform config.

#### Schema Registry

```yaml
schemaRegistry:
  enabled: true          # Set false for external registry
```

When `schemaRegistry.enabled=false`, set `schemaRegistry.externalUrl` in your values.

#### Kafka Connect

```yaml
kafkaConnect:
  replicaCount: 1        # Usually 1 is sufficient
  image:
    repository: ghcr.io/baselyne-systems/cdc-kafka-connect
    tag: latest
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: "1"
      memory: 1Gi
```

The Kafka Connect image (`Dockerfile.connect`) extends `confluentinc/cp-kafka-connect-base` with Debezium PostgreSQL and MySQL connector plugins pre-installed.

### Using Managed Services Instead

If you use managed Kafka (Confluent Cloud, Amazon MSK, Redpanda Cloud) or a managed Schema Registry, skip the sub-charts:

```yaml
# values.yaml
kafka:
  enabled: false
  externalBootstrapServers: "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"

schemaRegistry:
  enabled: false
  externalUrl: "https://psrc-xxxxx.us-east-1.aws.confluent.cloud"
```

You still deploy Kafka Connect from the chart (it needs Debezium plugins), but point it at your managed Kafka.

Alternatively, if your managed service includes Connect hosting (e.g. Confluent Cloud managed connectors), you can skip the entire Helm chart and configure each pipeline's `platform.yaml` to point at your managed endpoints.

### Verifying Platform Health

```bash
# Check Kafka Connect is running
kubectl port-forward svc/cdc-platform-connect 8083:8083
curl http://localhost:8083/connectors

# Use the CDC CLI
cdc health --platform-config platform.yaml
```

### Kafka Connect with Debezium

The `Dockerfile.connect` image ships with Debezium PostgreSQL and MySQL connectors. To add custom connectors:

1. Create a new Dockerfile extending `ghcr.io/baselyne-systems/cdc-kafka-connect`:
   ```dockerfile
   FROM ghcr.io/baselyne-systems/cdc-kafka-connect:latest
   RUN confluent-hub install --no-prompt <vendor>/<connector>:<version>
   ```
2. Build and push to your registry
3. Update `kafkaConnect.image` in your Helm values

---

## Pipeline Deployment

### Pipeline Config Reference

A full annotated `pipeline.yaml`:

```yaml
pipeline_id: orders-cdc        # Unique identifier for this pipeline
topic_prefix: cdc               # Kafka topic prefix (topics: cdc.public.orders)

source:
  source_type: postgres          # postgres or mysql
  host: ${CDC_SOURCE_HOST}       # Env var substitution supported
  port: 5432
  database: ${CDC_SOURCE_DB}
  username: ${CDC_SOURCE_USER}
  password: ${CDC_SOURCE_PASSWORD}
  tables:
    - public.orders
    - public.order_items
  slot_name: orders_cdc_slot     # Unique per pipeline to avoid conflicts
  publication_name: orders_pub
  snapshot_mode: initial         # initial, never, when_needed, no_data

sinks:
  - sink_id: iceberg-lake
    sink_type: iceberg
    enabled: true
    iceberg:
      catalog_uri: ${ICEBERG_CATALOG_URI}
      warehouse: ${ICEBERG_WAREHOUSE}
      table_name: orders_cdc
      write_mode: upsert
      batch_size: 1000
      s3_endpoint: ${S3_ENDPOINT}
      s3_access_key_id: ${S3_ACCESS_KEY_ID}
      s3_secret_access_key: ${S3_SECRET_ACCESS_KEY}
      maintenance:
        enabled: true
        compaction_interval_seconds: 3600
```

### Running Locally

```bash
# With default platform config
cdc run pipeline.yaml

# With custom platform config
cdc run pipeline.yaml --platform-config platform.yaml
```

### Running with Docker

```bash
docker run --rm \
  -v ./pipeline.yaml:/config/pipeline.yaml \
  -v ./platform.yaml:/config/platform.yaml \
  -e CDC_SOURCE_PASSWORD=secret \
  -p 8080:8080 \
  ghcr.io/baselyne-systems/cdc-platform:latest
```

### Running on Kubernetes

Each pipeline is a Kubernetes Deployment with a ConfigMap for configuration.

#### Example Manifests

**ConfigMap** — pipeline and platform config:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: orders-cdc-config
data:
  pipeline.yaml: |
    pipeline_id: orders-cdc
    topic_prefix: cdc
    source:
      database: ${CDC_SOURCE_DB}
      password: ${CDC_SOURCE_PASSWORD}
      tables:
        - public.orders
      slot_name: orders_cdc_slot
    sinks:
      - sink_id: iceberg-lake
        sink_type: iceberg
        enabled: true
        iceberg:
          catalog_uri: ${ICEBERG_CATALOG_URI}
          warehouse: ${ICEBERG_WAREHOUSE}
          table_name: orders_cdc
          write_mode: upsert

  platform.yaml: |
    kafka:
      bootstrap_servers: cdc-platform-kafka:9092
      schema_registry_url: http://cdc-platform-schema-registry:8081
    connector:
      connect_url: http://cdc-platform-connect:8083
    health_port: 8080
    health_enabled: true
```

**Secret** — sensitive values:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: orders-cdc-secrets
type: Opaque
stringData:
  CDC_SOURCE_DB: production_db
  CDC_SOURCE_PASSWORD: supersecret
  ICEBERG_CATALOG_URI: "postgresql://catalog:5432/iceberg"
  ICEBERG_WAREHOUSE: "s3://my-bucket/warehouse"
```

**Deployment**:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-cdc
spec:
  replicas: 1
  strategy:
    type: Recreate    # NOT RollingUpdate — avoids consumer group rebalance storms
  selector:
    matchLabels:
      app: orders-cdc
  template:
    metadata:
      labels:
        app: orders-cdc
    spec:
      containers:
        - name: pipeline
          image: ghcr.io/baselyne-systems/cdc-platform:latest
          # Default CMD: cdc run /config/pipeline.yaml --platform-config /config/platform.yaml
          ports:
            - containerPort: 8080
              name: health
          envFrom:
            - secretRef:
                name: orders-cdc-secrets
          livenessProbe:
            httpGet:
              path: /healthz
              port: health
            initialDelaySeconds: 10
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: health
            initialDelaySeconds: 30
            periodSeconds: 15
          resources:
            requests:
              cpu: 250m
              memory: 256Mi
            limits:
              cpu: "1"
              memory: 512Mi
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: orders-cdc-config
```

#### Why `strategy: Recreate`?

`RollingUpdate` creates a new pod before terminating the old one. Both pods join the same Kafka consumer group simultaneously, causing a **rebalance storm** — partitions are assigned, revoked, and reassigned repeatedly until the old pod dies. `Recreate` ensures the old pod is fully terminated before the new one starts, resulting in a single clean rebalance.

#### Resource Recommendations

| Workload | CPU request | Memory request | Notes |
|----------|-------------|----------------|-------|
| Low-traffic pipeline (< 100 events/s) | 100m | 128Mi | |
| Medium-traffic pipeline | 250m | 256Mi | |
| High-traffic pipeline (> 1k events/s) | 500m–1000m | 512Mi–1Gi | Increase for Iceberg batch flushes |

### Multiple Pipelines

Each pipeline gets its own Deployment, ConfigMap, and (optionally) Secret. Pipelines share the same platform infrastructure but operate independently.

```
Platform (shared):
  └── Kafka, Schema Registry, Kafka Connect

Pipelines (independent):
  ├── orders-cdc (Deployment + ConfigMap)
  ├── customers-cdc (Deployment + ConfigMap)
  └── inventory-cdc (Deployment + ConfigMap)
```

Important: each pipeline must use a **unique `slot_name`** in its source config to avoid conflicts on the PostgreSQL replication slot.

### Pipeline Lifecycle

When a pipeline starts (`cdc run`):

1. **Topic provisioning** — Kafka topics are auto-created based on source tables
2. **Connector registration** — A Debezium connector is registered on Kafka Connect
3. **Sink initialization** — All enabled sinks are started
4. **Monitor startup** — Schema monitor + lag monitor begin polling
5. **Health server startup** — HTTP health endpoints become available
6. **Consume loop** — Events are consumed from Kafka and dispatched to sinks

On shutdown (SIGTERM from Kubernetes):

1. Health server stops (readiness probe fails, traffic drains)
2. Schema + lag monitors stop
3. Partition workers are cancelled
4. All sinks are flushed and stopped
5. Final offset commit

---

## Health & Monitoring

### Health Endpoints

The pipeline exposes two HTTP endpoints on the configured `health_port` (default: 8080):

| Endpoint | Purpose | K8s Probe |
|----------|---------|-----------|
| `GET /healthz` | Liveness — always returns 200 if the process is running | `livenessProbe` |
| `GET /readyz` | Readiness — returns 200 if all components are healthy, 503 otherwise | `readinessProbe` |

### Readiness Check Details

The `/readyz` endpoint delegates to `Pipeline.health()`, which checks:

- **Source connector status** — queries Kafka Connect REST API for connector and task state
- **Sink health** — each sink reports its connection status
- **Consumer lag** — current lag per partition (informational, does not fail the probe)

If any source or sink reports `"status": "error"`, the readiness probe returns 503.

### Integrating with Prometheus

CDC Platform uses structured logging (structlog). To expose metrics to Prometheus:

1. Use a log-based exporter (e.g. `mtail`, `grok_exporter`) to parse structured JSON logs
2. Key events to scrape:
   - `consumer.lag` — extract `total_lag` and per-partition lag values
   - `pipeline.sink_write_error` — count for error rate alerting
   - `schema.version_changed` — count for schema change tracking

### Troubleshooting Unhealthy Pipelines

| Symptom | Likely Cause | Action |
|---------|-------------|--------|
| `/readyz` returns 503, source status error | Debezium connector failed or is restarting | Check `kubectl logs` for the Connect pod; verify source DB is reachable |
| `/readyz` returns 503, sink status error | Sink connection lost | Check sink credentials and network connectivity |
| High consumer lag | Sinks are slow or backpressure is engaged | Increase sink batch size, add resources, or tune `max_buffered_messages` |
| Pod in CrashLoopBackOff | Config error or missing secrets | Check pod logs: `kubectl logs <pod>` |

---

## Configuration Deep Dive

### Platform Config

All fields with defaults. Only override what differs from your environment.

```yaml
kafka:
  bootstrap_servers: localhost:9092
  schema_registry_url: http://localhost:8081
  group_id: cdc-platform
  auto_offset_reset: earliest
  enable_idempotence: true
  acks: all

connector:
  connect_url: http://localhost:8083
  timeout_seconds: 30
  retry_max_attempts: 5
  retry_wait_seconds: 2

dlq:
  enabled: true
  topic_suffix: dlq
  max_retries: 3
  include_headers: true

retry:
  max_attempts: 5
  initial_wait_seconds: 1
  max_wait_seconds: 60
  multiplier: 2
  jitter: true

max_buffered_messages: 1000
partition_concurrency: 0
schema_monitor_interval_seconds: 30.0
lag_monitor_interval_seconds: 15.0
stop_on_incompatible_schema: false
health_port: 8080
health_enabled: true
```

### Environment Variable Overrides

Use `${VAR}` or `${VAR:-default}` syntax in YAML config files:

```yaml
source:
  password: ${CDC_SOURCE_PASSWORD}
  database: ${CDC_SOURCE_DB:-mydb}
```

### Secret Management Patterns

**Kubernetes Secrets** (recommended for K8s deployments):
```yaml
envFrom:
  - secretRef:
      name: pipeline-secrets
```

**Environment variables** (for Docker/local):
```bash
export CDC_SOURCE_PASSWORD=secret
cdc run pipeline.yaml
```

---

## Production Checklist

### Kafka
- [ ] Topic replication factor >= 3
- [ ] Partition count set based on expected throughput (1 partition ≈ 10 MB/s)
- [ ] Retention set appropriately (`retention.ms`, `retention.bytes`)
- [ ] Monitor under-replicated partitions

### Debezium
- [ ] Unique `slot_name` per pipeline (avoid slot conflicts)
- [ ] WAL retention configured (`wal_keep_size` or replication slot monitoring)
- [ ] Heartbeat table for low-traffic tables (prevents WAL bloat from inactive slots)
- [ ] `snapshot_mode: initial` for first deploy, `never` for subsequent deploys

### Iceberg
- [ ] Catalog URI and warehouse credentials configured
- [ ] Table maintenance enabled (`maintenance.enabled: true`)
- [ ] Compaction interval tuned for write volume
- [ ] Snapshot expiry configured to control storage growth
- [ ] See [docs/lakehouse.md](lakehouse.md) for time-travel and rollback procedures

### Security
- [ ] Pipeline image runs as non-root user (default in `Dockerfile`)
- [ ] Network policies restrict pod-to-pod traffic
- [ ] Secrets stored in Kubernetes Secrets or a vault, not in ConfigMaps
- [ ] Database credentials use least-privilege accounts
- [ ] TLS enabled for Kafka and Schema Registry in production

### Scaling
- [ ] One pipeline per pod (do not run multiple pipelines in one process)
- [ ] Resource requests and limits set based on workload
- [ ] `max_buffered_messages` tuned for memory vs throughput tradeoff
- [ ] Consumer lag monitored and alerted on

### Backup & Recovery
- [ ] Iceberg time-travel enabled for point-in-time recovery
- [ ] Kafka topic retention covers recovery window
- [ ] Debezium slot monitoring to prevent WAL disk exhaustion
- [ ] Documented rollback procedure (see [docs/lakehouse.md](lakehouse.md))

---

## Docker Compose (Local Development)

For local development and testing, use the existing Docker Compose stack:

```bash
# Start the full local stack (Kafka, Schema Registry, Connect, PostgreSQL)
make up

# Run a pipeline locally against the stack
cdc run examples/demo-config.yaml

# Tear down
make down
```

The local stack includes:

| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL | 5432 | Source database with logical replication |
| Kafka | 9092 | Message broker (KRaft mode) |
| Schema Registry | 8081 | Avro schema storage |
| Kafka Connect | 8083 | Debezium connector hosting |
| Kafka UI | 8080 | Web UI for topic inspection |

### Quick Start: Zero to Running Pipeline

```bash
# 1. Clone and install
git clone <repo-url> && cd cdc-platform
uv sync --extra dev

# 2. Start infrastructure
make up

# 3. Verify platform health
cdc health

# 4. Run the demo pipeline
cdc run examples/demo-config.yaml

# 5. (In another terminal) Insert test data
psql -h localhost -U cdc_user -d cdc_demo -c \
  "INSERT INTO public.customers (name, email) VALUES ('test', 'test@example.com');"
```
