# Deployment Guide

This guide covers deploying CDC Platform in production on Kubernetes. It covers the shared platform infrastructure, individual pipeline workers, health monitoring, and operational best practices.

## Architecture Overview

CDC Platform follows a **two-phase deployment model**:

1. **Platform** (deployed once) — shared infrastructure that all pipelines depend on. For `kafka`: Kafka, Schema Registry, and Kafka Connect with Debezium plugins. For `pubsub`: GCP project with Pub/Sub API. For `kinesis`: AWS account with Kinesis + DynamoDB.
2. **Pipelines** (deployed on demand) — each pipeline is a `cdc run` process with its own config. It provisions transport resources (topics/streams + connectors/replication slots), consumes CDC events via the configured `EventSource`, and routes them to sinks.

```
                        ┌── Transport (configurable) ──┐
 Source DB ──▸ Provisioner ──▸ EventSource ──▸ Pipeline Worker ──▸ Sinks
                        └──────────────────────────────┘
              ─── Platform (shared) ───        ── Per-pipeline ──
```

The platform uses a transport-agnostic architecture. The `transport_mode` setting in `platform.yaml` selects the event transport backend (`kafka`, `pubsub`, or `kinesis`). The core pipeline interacts only with protocol abstractions (`EventSource`, `Provisioner`, `ErrorRouter`, `SourceMonitor`), making the transport layer pluggable.

The platform Helm chart deploys the shared infrastructure for the Kafka transport. For Pub/Sub and Kinesis, the cloud provider manages the infrastructure — only pipeline workers are deployed. Each pipeline runs as a separate Kubernetes Deployment using the pipeline Docker image.

### What the Platform Chart Deploys

| Component | Purpose |
|-----------|---------|
| Kafka (Bitnami sub-chart) | Message broker for CDC events |
| Schema Registry (Bitnami sub-chart) | Avro schema storage and compatibility checks |
| Kafka Connect | Hosts Debezium connectors that capture WAL changes |

### What a Pipeline Is

A pipeline is a single `cdc run` process that:
- Provisions transport resources via the `Provisioner` (for Kafka: creates topics, registers a Debezium connector)
- Consumes CDC events via the `EventSource` (for Kafka: Avro-deserializing consumer with manual offset control)
- Routes events to configured sinks (webhooks, PostgreSQL, Iceberg)
- Monitors source health via the `SourceMonitor` (for Kafka: schema changes + consumer lag)
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

The Kafka Connect image (`Dockerfile.connect`) extends `confluentinc/cp-kafka-connect-base` with all four Debezium connector plugins pre-installed: PostgreSQL, MySQL, MongoDB, and SQL Server.

### Using Managed Kafka (MSK / GCP Managed Kafka)

For managed Kafka services with auth, configure `auth_mechanism` in `platform.yaml`:

```yaml
# AWS MSK with IAM auth
kafka:
  bootstrap_servers: "b-1.mycluster.kafka.us-east-1.amazonaws.com:9098"
  security_protocol: "SASL_SSL"
  auth_mechanism: "sasl_iam"
  aws_region: "us-east-1"
```

```yaml
# GCP Managed Kafka with OAuth
kafka:
  bootstrap_servers: "bootstrap.my-cluster.us-central1.managedkafka.my-project.cloud.goog:9092"
  security_protocol: "SASL_SSL"
  auth_mechanism: "sasl_oauthbearer"
  gcp_project_id: "my-project"
```

See `examples/platform-msk.yaml` and `examples/platform-gcp-kafka.yaml` for full examples.

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

The `Dockerfile.connect` image ships with Debezium connectors for PostgreSQL, MySQL, MongoDB, and SQL Server. To add custom connectors:

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
  # Supported values: postgres | mysql | mongodb | sqlserver
  source_type: postgres
  host: ${CDC_SOURCE_HOST}       # Env var substitution supported
  port: 5432                     # postgres: 5432 | mysql: 3306 | mongodb: 27017 | sqlserver: 1433
  database: ${CDC_SOURCE_DB}
  username: ${CDC_SOURCE_USER}
  password: ${CDC_SOURCE_PASSWORD}
  tables:
    # postgres/mysql/sqlserver: schema-qualified  → public.orders, dbo.orders
    # mongodb:                  db-qualified       → mydb.orders
    - public.orders
    - public.order_items
  snapshot_mode: initial         # initial | never | when_needed | no_data

  # -- PostgreSQL-specific (ignored for other source types) --
  slot_name: orders_cdc_slot     # Must be unique per pipeline
  publication_name: orders_pub

  # -- MySQL-specific --
  # mysql_server_id: 1           # Must be unique across the replication topology

  # -- MongoDB-specific --
  # replica_set_name: rs0        # Required for replica sets; omit for Atlas/SRV URIs
  # auth_source: admin

  # -- SQL Server-specific --
  # (no extra fields; CDC must be pre-enabled on the DB and each table)

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
    transport_mode: kafka
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

Important: source-type-specific uniqueness requirements apply per pipeline:
- **PostgreSQL** — `slot_name` must be unique per pipeline (replication slot conflicts crash the connector)
- **MySQL** — `mysql_server_id` must be unique across the entire MySQL replication topology
- **MongoDB / SQL Server** — no per-pipeline uniqueness constraint beyond `pipeline_id` and `topic_prefix`

### Pipeline Lifecycle

When a pipeline starts (`cdc run`):

1. **Transport provisioning** — The `Provisioner` creates transport resources. For Kafka: auto-creates topics and registers a Debezium connector on Kafka Connect.
2. **Error router initialization** — The `ErrorRouter` is created for dead-letter routing.
3. **Sink initialization** — All enabled sinks are started.
4. **Event source creation** — The `EventSource` is created for the configured `transport_mode`.
5. **Source monitor startup** — The `SourceMonitor` begins background polling (for Kafka: schema changes + consumer lag).
6. **Health server startup** — HTTP health endpoints become available.
7. **Consume loop** — The `EventSource` polls the transport, converts messages to `SourceEvent` objects, and dispatches to sinks via per-partition workers.

On shutdown (SIGTERM from Kubernetes):

1. Health server stops (readiness probe fails, traffic drains)
2. Source monitor stops
3. Partition workers are cancelled
4. All sinks are flushed and stopped
5. Final offset commit via `EventSource.commit_offsets()`

To teardown transport resources (remove Debezium connector):

```bash
cdc undeploy pipeline.yaml --platform-config platform.yaml
```

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

- **Source health** — delegates to `EventSource.health()`. For Kafka: queries the Kafka Connect REST API for Debezium connector and task state.
- **Sink health** — each sink reports its connection status.
- **Consumer lag** — delegates to `SourceMonitor.get_lag()`. For Kafka: current lag per partition (informational, does not fail the probe).

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
transport_mode: kafka              # Event transport backend: kafka | pubsub | kinesis

kafka:                             # Required when transport_mode: kafka
  bootstrap_servers: localhost:9092
  schema_registry_url: http://localhost:8081
  group_id: cdc-platform
  auto_offset_reset: earliest
  enable_idempotence: true
  acks: all
  topic_num_partitions: 1          # Partition count for auto-created topics
  topic_replication_factor: 1      # Replication factor for auto-created topics
  poll_batch_size: 1               # Messages per poll (1 = single-poll, >1 = batch)
  deser_pool_size: 1               # Thread pool for parallel Avro deser (1 = no pool)
  commit_interval_seconds: 0.0     # 0.0 = per-event sync commit, >0 = periodic async

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
  flush_interval_seconds: 0.0      # 0.0 = sync flush per msg, >0 = non-blocking

retry:
  max_attempts: 5
  initial_wait_seconds: 1
  max_wait_seconds: 60
  multiplier: 2
  jitter: true

max_buffered_messages: 1000
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

## GCP Deployment (Pub/Sub Transport)

For Pub/Sub transport, the cloud provider manages topics and subscriptions. Only the pipeline workers need to be deployed.

### Prerequisites

- GCP project with Pub/Sub API enabled
- Application Default Credentials configured (service account key or Workload Identity)
- PostgreSQL source with `wal_level = logical`
- Install: `pip install cdc-platform[gcp,wal]`

### Platform Config

```yaml
transport_mode: pubsub
pubsub:
  project_id: "my-gcp-project"
  ordering_enabled: true
  ack_deadline_seconds: 600
  max_outstanding_messages: 1000
wal_reader:
  publication_name: "cdc_publication"
  slot_name: "cdc_slot"
  batch_size: 100
  batch_timeout_seconds: 1.0
```

See `examples/platform-pubsub.yaml` for a full example.

### GKE with Workload Identity

For GKE deployments, use [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity) to provide Application Default Credentials to the pipeline pods without managing service account keys:

1. Create a GCP service account with `roles/pubsub.editor` and `roles/monitoring.viewer`
2. Bind the Kubernetes service account to the GCP service account
3. Annotate the pod service account with `iam.gke.io/gcp-service-account`

### What the Provisioner Creates

- Pub/Sub topics for each CDC table (e.g. `cdc-public-customers`)
- Subscriptions with ordering enabled and dead-letter policy
- DLQ topics (e.g. `cdc-public-customers-dlq`)
- PostgreSQL replication slot and publication

---

## AWS Deployment (Kinesis Transport)

For Kinesis transport, the cloud provider manages streams. Only the pipeline workers need to be deployed.

### Prerequisites

- AWS account with Kinesis and DynamoDB access
- IAM credentials configured (IAM role, instance profile, or environment variables)
- PostgreSQL source with `wal_level = logical`
- Install: `pip install cdc-platform[aws,wal]`

### Platform Config

```yaml
transport_mode: kinesis
kinesis:
  region: "us-east-1"
  shard_count: 2
  checkpoint_table_name: "cdc-kinesis-checkpoints"
  poll_interval_seconds: 1.0
  max_records_per_shard: 100
wal_reader:
  publication_name: "cdc_publication"
  slot_name: "cdc_slot"
  batch_size: 100
  batch_timeout_seconds: 1.0
```

See `examples/platform-kinesis.yaml` for a full example.

### EKS with IAM Roles for Service Accounts (IRSA)

For EKS deployments, use [IRSA](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html) to grant AWS permissions:

1. Create an IAM role with policies for `kinesis:*`, `dynamodb:*`, and `cloudwatch:GetMetricData`
2. Associate the role with the Kubernetes service account via IRSA annotation
3. The pipeline pods automatically receive temporary credentials

### What the Provisioner Creates

- Kinesis streams for each CDC table (e.g. `cdc-public-customers`)
- DLQ streams (e.g. `cdc-public-customers-dlq`)
- DynamoDB checkpoint table for shard sequence numbers
- PostgreSQL replication slot and publication

---

## Production Checklist

### Kafka
- [ ] `topic_replication_factor` >= 3 in platform config (`kafka.topic_replication_factor`)
- [ ] `topic_num_partitions` set based on expected throughput (`kafka.topic_num_partitions`, 1 partition ≈ 10 MB/s)
- [ ] Retention set appropriately (`retention.ms`, `retention.bytes`)
- [ ] Monitor under-replicated partitions

### Debezium (all source types)
- [ ] `snapshot_mode: initial` for first deploy, `never` for subsequent deploys
- [ ] Kafka Connect image built from `Dockerfile.connect` — includes all four connector plugins

#### PostgreSQL prerequisites
- [ ] `wal_level = logical` on the source instance
- [ ] Unique `slot_name` per pipeline (slot name conflicts cause connector failure)
- [ ] WAL retention configured (`wal_keep_size` or replication slot monitoring) — inactive slots block WAL reclamation
- [ ] Heartbeat table created for low-traffic sources (prevents WAL bloat)
- [ ] CDC user granted `REPLICATION` privilege and `SELECT` on watched tables

#### MySQL prerequisites
- [ ] `binlog_format = ROW` on the source instance
- [ ] `binlog_row_image = FULL` (required for before-images on UPDATE/DELETE)
- [ ] `mysql_server_id` unique across all MySQL replicas in the topology
- [ ] CDC user granted `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE`, `REPLICATION CLIENT`

#### MongoDB prerequisites
- [ ] Source must be a **replica set or sharded cluster** — change streams do not work on standalone nodes
- [ ] `replica_set_name` set in pipeline config (or use an SRV connection string)
- [ ] CDC user granted `read` on watched databases and `clusterMonitor` on `admin`
- [ ] Oplog retention sufficient to survive connector downtime (`storage.oplogMinRetentionHours`)

#### SQL Server prerequisites
- [ ] SQL Server Agent must be running (manages CDC capture and cleanup jobs)
- [ ] CDC enabled on the database: `EXEC sys.sp_cdc_enable_db`
- [ ] CDC enabled on each captured table: `EXEC sys.sp_cdc_enable_table @source_schema, @source_name, @role_name = NULL`
- [ ] CDC user granted `db_datareader` and `EXECUTE` on CDC stored procedures (`cdc` schema)
- [ ] Developer or Enterprise edition required (Express does not support SQL Server Agent)

### Google Pub/Sub
- [ ] GCP project with Pub/Sub API enabled
- [ ] Service account with `roles/pubsub.editor` and `roles/monitoring.viewer`
- [ ] `wal_level = logical` on the PostgreSQL source
- [ ] `wal_reader.slot_name` unique per pipeline
- [ ] `ack_deadline_seconds` set high enough for sink processing time (max 600)
- [ ] DLQ topic monitoring configured
- [ ] Subscription ordering enabled if event ordering matters

### Amazon Kinesis
- [ ] IAM role with `kinesis:*`, `dynamodb:*` permissions
- [ ] `wal_level = logical` on the PostgreSQL source
- [ ] `wal_reader.slot_name` unique per pipeline
- [ ] `shard_count` set based on expected throughput (1 shard ≈ 1 MB/s write, 2 MB/s read)
- [ ] DynamoDB checkpoint table provisioned capacity or on-demand billing configured
- [ ] CloudWatch monitoring for `MillisBehindLatest`
- [ ] Enhanced fan-out considered for high-throughput workloads

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
- [ ] For high-throughput: `poll_batch_size`, `deser_pool_size`, `commit_interval_seconds` tuned (see [docs/scaling.md](scaling.md))
- [ ] For Iceberg high-throughput: `write_executor_threads`, `flush_interval_seconds` tuned
- [ ] `commit_interval_seconds` tradeoff understood (redelivery window on crash)

### Backup & Recovery
- [ ] Iceberg time-travel enabled for point-in-time recovery
- [ ] Kafka topic retention covers recovery window
- [ ] Debezium slot monitoring to prevent WAL disk exhaustion
- [ ] Documented rollback procedure (see [docs/lakehouse.md](lakehouse.md))

---

## Docker Compose (Local Development)

For local development and testing, use the existing Docker Compose stack:

```bash
# Start the base stack (Kafka, Schema Registry, Connect, PostgreSQL)
make up

# Add MongoDB source (single-node replica set):
docker compose -f docker/docker-compose.yml -f docker/docker-compose.mongodb.yml up -d

# Add SQL Server source (Developer edition + CDC enabled):
docker compose -f docker/docker-compose.yml -f docker/docker-compose.sqlserver.yml up -d

# Run a pipeline locally against the stack
cdc run examples/demo-config.yaml

# Tear down
make down
```

### Helm Transport Mode

The Helm chart supports a `platform.transportMode` value (default: `kafka`). When set to a non-Kafka transport, Kafka Connect resources (deployment, service, configmap) are not rendered. Configure this in your values override:

```yaml
platform:
  transportMode: kafka           # or a future transport mode
  topicNumPartitions: 6          # topic partitions for auto-created topics
  topicReplicationFactor: 3      # replication factor for auto-created topics
```

The base local stack includes:

| Service | Port | Compose file | Purpose |
|---------|------|--------------|---------|
| PostgreSQL | 5432 | `docker-compose.yml` | Source database (logical replication pre-configured) |
| Kafka | 9092 | `docker-compose.yml` | Message broker (KRaft mode) |
| Schema Registry | 8081 | `docker-compose.yml` | Avro schema storage |
| Kafka Connect | 8083 | `docker-compose.yml` | Debezium connector hosting (all 4 connectors installed) |
| Kafka UI | 8080 | `docker-compose.yml` | Web UI for topic inspection |
| MongoDB | 27017 | `docker-compose.mongodb.yml` | Source database (single-node replica set) |
| SQL Server | 1433 | `docker-compose.sqlserver.yml` | Source database (CDC pre-enabled on `cdc_demo`) |

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
