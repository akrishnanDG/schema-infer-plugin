# Schema Inference Plugin - Best Practices Guide

This guide covers recommended practices for deploying and operating the Schema Inference Plugin in production environments.

## Table of Contents

1. [Configuration](#configuration)
2. [Schema Inference Strategy](#schema-inference-strategy)
3. [Schema Registry Operations](#schema-registry-operations)
4. [Live Mode Operations](#live-mode-operations)
5. [Multi-Instance Scaling](#multi-instance-scaling)
6. [Security](#security)
7. [Performance Tuning](#performance-tuning)
8. [Schema Evolution](#schema-evolution)
9. [Monitoring and Observability](#monitoring-and-observability)
10. [Common Pitfalls](#common-pitfalls)

---

## Configuration

### Use YAML config files, not inline credentials

Store Kafka and Schema Registry credentials in a YAML config file rather than passing them as CLI arguments. CLI arguments appear in process lists and shell history.

```yaml
# cc-config.yaml
kafka:
  bootstrap_servers: "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  cloud_api_key: "YOUR_KAFKA_API_KEY"
  cloud_api_secret: "YOUR_KAFKA_API_SECRET"

schema_registry:
  url: "https://psrc-xxxxx.us-east-1.aws.confluent.cloud"
  cloud_api_key: "YOUR_SR_API_KEY"
  cloud_api_secret: "YOUR_SR_API_SECRET"
  compatibility: "BACKWARD"
```

Set restrictive file permissions on config files:
```bash
chmod 600 cc-config.yaml
```

### Separate config files per environment

Maintain separate config files for development, staging, and production. Never reuse API keys across environments.

```
configs/
  dev-config.yaml
  staging-config.yaml
  prod-config.yaml
```

### Set explicit compatibility levels

Always set `compatibility` in the config rather than relying on the Schema Registry global default. This prevents unexpected behavior if the global default changes.

```yaml
schema_registry:
  compatibility: "BACKWARD"   # Explicit, not inherited from global
```

---

## Schema Inference Strategy

### Start with a representative sample

The default `max_messages: 50` works for most topics, but topics with high structural variability may need more:

```bash
# Standard topics
schema-infer infer --topic orders --max-messages 50

# Topics with many optional fields or rare event types
schema-infer infer --topic complex-events --max-messages 500
```

### Use `--from-beginning` for initial bootstrap in live mode

When starting live mode on a topic that already has data, use `--from-beginning` to infer schemas from existing messages rather than waiting for new ones:

```bash
# First run: bootstrap from existing data
schema-infer --config config.yaml live --topic orders --register --from-beginning

# Subsequent runs: resume from committed offsets (--from-beginning has no effect)
schema-infer --config config.yaml live --topic orders --register
```

### Prefer JSON Schema for multi-event topics

Multi-event detection (discriminator-based `oneOf` schemas with `$ref` references) is only supported with JSON Schema. If your topic contains multiple event types, use `--format json-schema`:

```bash
schema-infer infer --topic events --format json-schema --register
```

Avro and Protobuf always produce flat schemas regardless of event type diversity.

### Use `--flatten` when multi-event detection is undesirable

If your topic has a discriminator-like field but you want a single unified schema, use `--flatten`:

```bash
schema-infer infer --topic events --flatten --register
```

### Validate before registering

Run inference without `--register` first to inspect the generated schema:

```bash
# Generate schema files for review
schema-infer infer --topic orders --output-dir ./schemas-review/

# Or test with sample data before connecting to Kafka
schema-infer infer --data-file sample-orders.jsonl --output schema-review.json

# Review, then register
schema-infer infer --topic orders --register
```

---

## Schema Registry Operations

### Understand compatibility modes

| Mode | Rule | Use When |
|------|------|----------|
| `BACKWARD` (recommended) | New schema can read data written with the old schema | Adding optional fields, evolving consumers first |
| `FORWARD` | Old schema can read data written with the new schema | Evolving producers first |
| `FULL` | Both directions | Strictest control |
| `NONE` | No checks | Development only |

The tool defaults to `BACKWARD`, which is the Confluent recommended default.

### Use Schema Registry contexts for isolation

When running in environments with shared Schema Registry clusters, use contexts to namespace your subjects:

```bash
schema-infer infer --topic orders --register --context my-team
# Registers as: :.my-team:orders-value
```

### Merging behavior

The tool merges with existing schemas in Schema Registry rather than replacing them:

- New fields are added (existing fields are never removed)
- Type conflicts preserve the existing type to avoid compatibility errors
- Nested objects and array items are recursively deep-merged

This means running inference multiple times against the same topic is safe — schemas only grow, never shrink.

### Handle the flat-to-multi-event transition

When a topic transitions from a flat schema to a multi-event `oneOf` schema, the tool temporarily sets subject compatibility to `NONE` for the registration, then restores it. This is automated and logged:

```
[timestamp] topic: Transitioning from flat to multi-event, temporarily set compatibility to NONE
[timestamp] topic: Registered 4 multi-event schemas (3 sub + 1 main)
[timestamp] topic: Restored compatibility to BACKWARD
```

If the registration fails, the `finally` block guarantees compatibility is restored.

---

## Live Mode Operations

### Choose appropriate batch settings

The two batch parameters control how often schemas are evaluated:

| Parameter | Default | Effect |
|-----------|---------|--------|
| `batch_size` | 100 | Process after accumulating N messages |
| `batch_timeout_seconds` | 60.0 | Process after N seconds regardless of message count |

Whichever threshold is reached first triggers a batch. For high-volume topics, increase `batch_size`. For low-volume topics, `batch_timeout_seconds` ensures timely processing.

```yaml
live:
  batch_size: 200           # High-volume: larger batches for efficiency
  batch_timeout_seconds: 60 # Low-volume: don't wait too long
```

### Use state persistence

State persistence (`persist_state: true`, the default) allows live mode to resume from where it left off after restarts. The state includes:

- Accumulated field statistics
- Detected format and discriminator
- Last inferred schema

Without persistence, each restart re-infers schemas from scratch (only from new messages unless `--from-beginning` is used).

### Set `min_records_before_register` appropriately

The default (`10`) prevents registering schemas from too few records. For topics with complex structures, increase this:

```yaml
live:
  min_records_before_register: 50   # Wait for more data before first registration
```

### Handle incompatible schemas

Choose a strategy based on your environment:

| Strategy | Behavior | Use When |
|----------|----------|----------|
| `skip` (default) | Log and skip registration | Production: safety first |
| `log` | Log and write to file for review | When you want manual review |
| `force` | Temporarily set compat to NONE and register | Controlled schema migrations |
| `fail` | Stop live mode entirely | When incompatibility signals a problem |

```bash
# Production: skip incompatible changes
schema-infer --config config.yaml live --topic orders --register --on-incompatible skip

# Migration: force-register known changes
schema-infer --config config.yaml live --topic orders --register --on-incompatible force
```

---

## Multi-Instance Scaling

### Use shared consumer groups

Multiple live mode instances can share a consumer group. Kafka distributes partitions across instances:

```bash
# Instance 1
schema-infer --config config.yaml live \
  --topic-pattern ".*" --register \
  --consumer-group schema-infer-prod

# Instance 2 (same consumer group)
schema-infer --config config.yaml live \
  --topic-pattern ".*" --register \
  --consumer-group schema-infer-prod
```

### Understand partition-0 ownership

Only the instance owning partition 0 of a topic registers schemas to Schema Registry. Other instances still process messages and build state, but defer registration to the partition-0 owner. This prevents race conditions during concurrent registration.

- **Single-partition topics**: Only one instance ever owns the partition, so registration works naturally.
- **Multi-partition topics**: The partition-0 owner is the sole registrator. If it goes down, the next rebalance assigns partition 0 to another instance, which takes over registration.

### Use shared state directories for faster handoff

When running multiple instances on shared storage (NFS, EFS), point `--state-dir` to a shared directory. On rebalance, the new partition owner loads persisted state instead of re-inferring from scratch:

```bash
schema-infer --config config.yaml live \
  --topic-pattern ".*" --register \
  --consumer-group schema-infer-prod \
  --state-dir /shared/schema-infer/state
```

---

## Security

### Minimum required permissions

**Kafka cluster:**
- `DESCRIBE` on cluster and topics (for discovery)
- `READ` on target topics and consumer groups (for message consumption)

**Schema Registry:**
- `DeveloperWrite` for schema registration
- `ResourceOwner` only if flat-to-multi-event transitions are expected (requires temporarily changing subject compatibility)

See the [Required Permissions](DOCUMENTATION.md#required-permissions) section in the documentation for full ACL and RBAC setup commands.

### Credential handling

- Credentials are read from YAML config files only, not from environment variables
- Credentials are redacted in log output (only first 8 characters shown with `***` prefix)
- Config files should have restrictive permissions (`chmod 600`)
- Never pass credentials as CLI arguments

### Input validation

The tool validates all inputs:
- Topic names: max 249 characters, pattern `[a-zA-Z0-9._-]+`
- Bootstrap servers: valid `host:port` format, port 1-65535
- Schema Registry URL: must start with `http://` or `https://`
- Max messages: 1 to 1,000,000
- Timeout: 1 to 3,600 seconds

---

## Performance Tuning

### Parallel processing for batch inference

Adjust `max_workers` based on your cluster's capacity:

```yaml
performance:
  max_workers: 8      # Parallel topic processing threads
  show_progress: true  # Progress bars for large batch jobs
```

### Optimize for large topic counts

For clusters with hundreds of topics, the live mode auto-scales batch size:

```yaml
live:
  batch_size: 100     # Auto-scaled: 100 for <50 topics, up to 50000 for 500+ topics
  max_concurrent_registrations: 5  # Rate-limit SR calls
```

### Memory management

For topics with deeply nested data:

```yaml
inference:
  max_depth: 20       # Max nesting depth (default)

performance:
  memory_limit_mb: 1024  # Increase for complex schemas
  batch_size: 100         # Reduce if memory is constrained
```

---

## Schema Evolution

### Closed content model

Generated JSON Schemas use `additionalProperties: false` (closed content model). Under `BACKWARD` compatibility on Confluent Cloud, this means:

- Adding new optional fields is allowed
- Removing fields is not allowed
- Changing field types is not allowed

This is the recommended model for schema evolution.

### Unified numeric types

All integers and floats are inferred as `number` (JSON Schema) / `double` (Avro/Protobuf). This prevents compatibility errors when a field appears as `5` in one batch and `5.5` in another.

### Schema merging across runs

Running inference multiple times is safe. The tool merges with existing schemas:

1. Fetches the current schema from Schema Registry
2. Deep-merges new fields into the existing schema
3. Preserves existing field types (never narrows)
4. Registers the merged result

This means schemas evolve monotonically — they can gain fields but never lose them.

---

## Monitoring and Observability

### Live mode output

Live mode logs all schema changes to stdout:

```
[10:14:23] orders: Processed 15 messages (total: 15). 5 fields detected.
[10:14:25] orders: Initial schema registered (ID: 106658)
[10:14:41] orders: Detected discriminator field 'event_type'
[10:14:48] orders: Transitioning from flat to multi-event, temporarily set compatibility to NONE
[10:14:53] orders: Registered 4 multi-event schemas (3 sub + 1 main)
[10:14:53] orders: Restored compatibility to BACKWARD
```

### Debug logging

Enable verbose logging for troubleshooting:

```yaml
performance:
  verbose_logging: true
```

Or check debug-level logs for:
- Schema merge skip reasons (logged at DEBUG level)
- Format detection confidence scores
- Discriminator detection candidates

### Summary output

Live mode prints periodic summaries (configurable via `summary_interval_seconds`) and a final summary on shutdown:

```
Live mode stopped.
  Processed 1250 messages across 12 topics in 5m 30s
  Registered 18 schema versions
  Detected 3 schema changes
```

---

## Common Pitfalls

### 1. Empty topic results

**Symptom**: Topics appear empty despite having data.

**Cause**: The tool reads from the latest offset by default (`auto_offset_reset: latest`). If no new messages arrive during the timeout window, the topic appears empty.

**Fix**: Increase the timeout or set `auto_offset_reset: earliest` in the config:
```bash
schema-infer infer --topic orders --timeout 60
```

### 2. Schema compatibility errors on first run

**Symptom**: `409 Incompatible schema` error when registering.

**Cause**: A schema already exists for the subject with a different structure.

**Fix**: Use `--on-incompatible force` for a one-time migration, or merge with the existing schema (the tool does this automatically when it can read the existing schema).

### 3. Multi-event detection not triggering

**Symptom**: Topic has multiple event types but the tool produces a flat schema.

**Cause**: Detection requires at least 5 records with 2+ distinct discriminator values and different field sets. The discriminator field must be present in 90%+ of records.

**Fix**: Ensure enough records are sampled (`--max-messages 200`) or manually specify the discriminator (`--discriminator event_type`).

### 4. Type changes between runs

**Symptom**: A field changes from `string` to `number` (or vice versa) between inference runs.

**Cause**: Different message samples produce different type inferences.

**Fix**: The schema merger preserves the existing type when conflicts occur. Run inference with more records (`--max-messages 500`) for a more representative sample.

### 5. Live mode not registering schemas

**Symptom**: Live mode processes messages but never registers schemas.

**Causes**:
- `--register` flag not set
- `min_records_before_register` threshold not reached
- In multi-instance mode, this instance doesn't own partition 0

**Fix**: Check the log output. If partition ownership is the issue, this is expected — the partition-0 owner handles registration.

### 6. Config file credentials not working

**Symptom**: Authentication failures despite correct credentials in config.

**Cause**: Using `sasl_username`/`sasl_password` for Confluent Cloud instead of `cloud_api_key`/`cloud_api_secret`, or vice versa.

**Fix**: Use `cloud_api_key`/`cloud_api_secret` for Confluent Cloud, `sasl_username`/`sasl_password` for Confluent Platform.

---

For detailed configuration reference, see [DOCUMENTATION.md](DOCUMENTATION.md). For examples, see [EXAMPLES.md](EXAMPLES.md).
