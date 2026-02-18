# Lakehouse Features

The Iceberg sink supports background table maintenance and time-travel operations to make CDC-to-lakehouse pipelines production-grade.

## Overview

CDC pipelines produce frequent micro-batches, which creates two problems for Iceberg tables:

1. **Small-file accumulation** — Each batch append creates new data files. Over time, hundreds of tiny files degrade query performance.
2. **Unbounded metadata growth** — Every write creates a new snapshot. Without expiry, the metadata layer grows indefinitely.

The lakehouse features address both issues with a background maintenance service and CLI commands for point-in-time queries and rollback.

## Table Maintenance

The `TableMaintenanceMonitor` is a background async service that runs two independent poll loops alongside the CDC pipeline, following the same pattern as `SchemaMonitor` and `LagMonitor`.

### Snapshot Expiry

Removes snapshots older than a configurable threshold to bound metadata growth.

**How it works:**
- Runs on a configurable interval (default: 1 hour)
- Calls `table.manage_snapshots().expire_snapshots_older_than(cutoff_ms).commit()`
- If the API is not available in your pyiceberg version, logs a warning and skips

### Compaction

Rewrites small files into larger ones to improve query performance. Uses a **partition-scoped, memory-bounded** strategy since pyiceberg <1.0 lacks `rewrite_data_files()`.

**Partitioned tables:**
1. Inspects manifest entries to build a `{partition_value: file_count}` map
2. For each partition exceeding `compaction_file_threshold` (default: 10 files):
   - Scans only that partition into memory
   - If rows exceed `compaction_max_rows_per_batch` (default: 500K), skips with a warning
   - Otherwise, overwrites the partition with the compacted data

**Unpartitioned tables:**
- Checks total file count against the threshold
- If total rows exceed `compaction_max_rows_per_batch`, skips (too large for in-process compaction)

**When to use external compaction:** If your partitions regularly exceed 500K rows, use Spark or Trino for compaction instead of the in-process service. The `compaction_max_rows_per_batch` setting is a safety valve, not a tuning knob.

### Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `maintenance.enabled` | `false` | Enable background maintenance |
| `maintenance.expire_snapshots_interval_seconds` | `3600.0` | How often to run snapshot expiry (seconds) |
| `maintenance.expire_snapshots_older_than_seconds` | `86400.0` | Expire snapshots older than this (seconds) |
| `maintenance.compaction_interval_seconds` | `7200.0` | How often to run compaction (seconds) |
| `maintenance.compaction_file_threshold` | `10` | Minimum files in a partition before compaction triggers |
| `maintenance.compaction_max_rows_per_batch` | `500000` | Safety limit — skip partitions larger than this |

### Example Configuration

```yaml
sinks:
  - sink_id: iceberg-lake
    sink_type: iceberg
    iceberg:
      catalog_uri: "sqlite:////tmp/cdc_catalog.db"
      warehouse: "file:///tmp/cdc_warehouse"
      table_name: customers_cdc
      write_mode: append
      batch_size: 500
      maintenance:
        enabled: true
        expire_snapshots_interval_seconds: 1800
        expire_snapshots_older_than_seconds: 43200
        compaction_interval_seconds: 3600
        compaction_file_threshold: 5
```

## Time Travel & Rollback

CLI commands for inspecting snapshots, querying historical data, and rolling back to a previous state.

### List Snapshots

```bash
cdc lakehouse snapshots pipeline.yaml
cdc lakehouse snapshots pipeline.yaml --sink-id iceberg-lake
```

Displays a table of all snapshots with their IDs, timestamps, operations, and summaries.

### Point-in-Time Query

```bash
cdc lakehouse query pipeline.yaml --snapshot-id 1234567890
cdc lakehouse query pipeline.yaml --snapshot-id 1234567890 --limit 50
```

Reads data as it existed at the specified snapshot. Useful for:
- Verifying data before/after a bad write
- Auditing historical state
- Debugging schema evolution issues

### Rollback

```bash
cdc lakehouse rollback pipeline.yaml --snapshot-id 1234567890 --yes
```

Rolls the table's current snapshot pointer back to a previous snapshot. Data written after that snapshot becomes invisible (but is not deleted until snapshot expiry runs).

**Warning:** Rollback changes the table's visible state for all readers. Use with caution in shared environments.

**Workflow example:**
1. Identify the bad snapshot: `cdc lakehouse snapshots pipeline.yaml`
2. Verify the good state: `cdc lakehouse query pipeline.yaml --snapshot-id <good_id>`
3. Roll back: `cdc lakehouse rollback pipeline.yaml --snapshot-id <good_id> --yes`

## pyiceberg Compatibility

| Feature | Minimum Version | Graceful Degradation |
|---------|----------------|---------------------|
| Snapshot expiry | `>=0.8` | Logs warning if API missing |
| Compaction (scan + overwrite) | `>=0.8` | Uses scan-and-overwrite pattern |
| Rollback (`set_current_snapshot`) | `>=0.9` | Raises `RuntimeError` with upgrade message |
| `rewrite_data_files()` | Not available in `<1.0` | Not used — uses partition-scoped overwrite instead |

The platform requires `pyiceberg >=0.8,<1`. All features degrade gracefully when APIs are not available.
