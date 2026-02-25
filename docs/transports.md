# Transport Backends

The CDC platform supports multiple event transports via a pluggable architecture. Each transport implements the same four protocols (`EventSource`, `Provisioner`, `ErrorRouter`, `SourceMonitor`), so pipelines work identically regardless of the underlying transport.

## Transport Comparison

| Feature | Kafka | Google Pub/Sub | Amazon Kinesis |
|---------|-------|----------------|----------------|
| CDC Engine | Debezium (Kafka Connect) | Direct WAL Reader | Direct WAL Reader |
| Ordering | Per-partition | Per-ordering-key (virtual partitions) | Per-shard |
| Offset Management | Kafka consumer group | Per-message ack | DynamoDB checkpoints |
| DLQ | DLQ topic | DLQ topic + dead letter policy | DLQ stream |
| Schema Registry | Confluent Schema Registry | N/A (JSON) | N/A (JSON) |
| Auth | SASL (PLAIN, SCRAM, IAM, OAuth) | GCP Application Default Credentials | AWS IAM |
| Source DB Support | PostgreSQL, MySQL, MongoDB, SQL Server | PostgreSQL only | PostgreSQL only |

## Configuration

### Kafka (default)

```yaml
transport_mode: kafka
kafka:
  bootstrap_servers: "localhost:9092"
  schema_registry_url: "http://localhost:8081"
  group_id: "cdc-platform"
  # Auth (optional)
  security_protocol: "SASL_SSL"
  auth_mechanism: "sasl_iam"  # or sasl_plain, sasl_scram_256, sasl_scram_512, sasl_oauthbearer
  aws_region: "us-east-1"     # Required for sasl_iam
connector:
  connect_url: "http://localhost:8083"
```

### Google Pub/Sub

```yaml
transport_mode: pubsub
pubsub:
  project_id: "my-gcp-project"
  ordering_enabled: true
  ack_deadline_seconds: 600
  max_messages_per_pull: 100
  group_id: "cdc-platform"
  max_outstanding_messages: 1000
  max_delivery_attempts: 5
wal_reader:
  publication_name: "cdc_publication"
  slot_name: "cdc_slot"
  batch_size: 100
  batch_timeout_seconds: 1.0
```

**Requirements:** `pip install cdc-platform[gcp,wal]`

### Amazon Kinesis

```yaml
transport_mode: kinesis
kinesis:
  region: "us-east-1"
  shard_count: 2
  group_id: "cdc-platform"
  iterator_type: "TRIM_HORIZON"
  checkpoint_table_name: "cdc-kinesis-checkpoints"
  poll_interval_seconds: 1.0
  max_records_per_shard: 100
wal_reader:
  publication_name: "cdc_publication"
  slot_name: "cdc_slot"
  batch_size: 100
  batch_timeout_seconds: 1.0
```

**Requirements:** `pip install cdc-platform[aws,wal]`

## Kafka Auth Mechanisms

### AWS MSK (IAM)

```yaml
kafka:
  security_protocol: "SASL_SSL"
  auth_mechanism: "sasl_iam"
  aws_region: "us-east-1"
```

Uses `aws-msk-iam-sasl-signer-python` to generate short-lived OAuth tokens from IAM credentials.

### GCP Managed Kafka (OAuth)

```yaml
kafka:
  security_protocol: "SASL_SSL"
  auth_mechanism: "sasl_oauthbearer"
  gcp_project_id: "my-project"
```

Uses `google-auth` Application Default Credentials for OAuth token generation.

### SASL PLAIN / SCRAM

```yaml
kafka:
  security_protocol: "SASL_SSL"
  auth_mechanism: "sasl_scram_512"  # or sasl_plain, sasl_scram_256
  sasl_username: "${KAFKA_USER}"
  sasl_password: "${KAFKA_PASSWORD}"
  ssl_ca_location: "/path/to/ca.pem"
```

## Architecture

### WAL Reader (Non-Kafka Transports)

For Pub/Sub and Kinesis, the platform replaces Debezium with a direct WAL reader that:

1. Connects to PostgreSQL via the logical replication protocol
2. Creates a replication slot and publication
3. Decodes `pgoutput` binary messages into structured change events
4. Serializes changes to JSON and publishes via the transport-specific `WalPublisher`

The WAL reader runs as a background task alongside the `EventSource` consumer.

### Virtual Partitions (Pub/Sub)

Pub/Sub has no native partition concept. The platform maps `ordering_key` values to virtual partitions via consistent hashing (`hash(key) % 16`), preserving the per-partition queue architecture used by the pipeline runner.

### Shard-to-Partition Mapping (Kinesis)

Kinesis shards map directly to pipeline partitions. Each shard gets its own async reader task. Sequence numbers serve as offsets, checkpointed to DynamoDB.

## Migration Guide

### From Kafka to Pub/Sub

1. Install GCP dependencies: `pip install cdc-platform[gcp,wal]`
2. Create a GCP project with Pub/Sub API enabled
3. Set up Application Default Credentials (`gcloud auth application-default login`)
4. Update platform config to `transport_mode: pubsub` with `pubsub` and `wal_reader` sections
5. Ensure PostgreSQL has `wal_level = logical` in `postgresql.conf`
6. Start the pipeline — provisioner creates topics, subscriptions, replication slot

### From Kafka to Kinesis

1. Install AWS dependencies: `pip install cdc-platform[aws,wal]`
2. Configure AWS credentials (`aws configure` or IAM role)
3. Update platform config to `transport_mode: kinesis` with `kinesis` and `wal_reader` sections
4. Ensure PostgreSQL has `wal_level = logical` in `postgresql.conf`
5. Start the pipeline — provisioner creates streams, DynamoDB table, replication slot
