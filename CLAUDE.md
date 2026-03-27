# Schema Inference Plugin — Project Context

You are the senior engineer on this project. You know every design decision, tradeoff, and implementation detail. This file gives you the full context to act as the expert.

## What This Project Is

A CLI plugin that infers and generates schemas (JSON Schema, Avro, Protobuf) from Kafka topic data and registers them in Confluent Schema Registry. Think of it as **a Glue Crawler for Confluent Cloud** — it scans topics, detects structure and types, and produces schemas that Flink SQL, Tableflow, Connect, and ksqlDB can use.

**Version**: 1.4.4 | **Python**: 3.9+ | **Tests**: 251 | **Lines**: ~10,500 source + 5,400 tests

## Architecture

```
schema_infer/
├── plugin/
│   ├── cli.py          # Click CLI (infer, live, list-topics, validate-topics, version)
│   ├── live.py         # LiveModeOrchestrator — continuous consumer with thread pool
│   ├── auth.py         # AuthenticationManager — Cloud/Platform credential detection
│   └── optimistic.py   # OptimisticProcessor — one-shot batch reader
├── core/
│   ├── consumer.py     # KafkaConsumer wrapper
│   ├── registry.py     # SchemaRegistry client (register, merge, compat, config)
│   ├── discovery.py    # TopicDiscovery — prefix/pattern/filter-based topic listing
│   ├── inferrer.py     # SchemaInferrer — coordinates format detection → parsing → generation
│   ├── live_consumer.py # LiveConsumer — stable consumer group, rebalance callbacks, partition map
│   ├── incremental.py  # IncrementalSchemaState — running field statistics across batches
│   ├── merger.py       # SchemaMerger — deep recursive merge with existing SR schemas
│   └── state_store.py  # StateStore — JSON file persistence for live mode resume
├── formats/
│   ├── detector.py     # FormatDetector — JSON/CSV/TSV/key-value/raw-text detection
│   └── parsers.py      # ParserFactory + format-specific parsers
├── schemas/
│   ├── generators.py   # Avro/Protobuf/JSON Schema generators (closed content model)
│   └── inference.py    # SchemaAnalyzer — field analysis, type inference, discriminator detection
└── utils/
    ├── exceptions.py   # Custom exception hierarchy
    ├── validators.py   # Input validation (topic names, URLs, ports, schemas)
    ├── logger.py       # Logging setup
    └── performance.py  # PerformanceMonitor, BatchProcessor, CacheManager, MemoryManager
```

## Critical Design Decisions

### Closed Content Model
All JSON Schemas use `additionalProperties: false` and `"required": []` (all fields optional, nullable). This is **required** by Confluent Cloud's strict BACKWARD compatibility — without it, new fields cause validation failures. This applies uniformly across batch infer, live mode, and local inference (`--message`/`--data-file`).

### Unified Numeric Types
All integers and floats are inferred as `number` (JSON Schema) / `double` (Avro/Protobuf). This prevents compatibility errors when a field appears as `5` in one batch and `5.5` in another.

### Schema Merging (Never Destructive)
When merging with existing SR schemas:
- New fields are added (existing fields are never removed)
- Type conflicts widen to union (e.g., `["string", "integer", "null"]`) — never narrows
- Nested objects and array items are recursively deep-merged
- Running inference multiple times is always safe — schemas only grow

### Multi-Event Detection (JSON Schema Only)
Topics with multiple event types are auto-detected via discriminator field analysis:
- Scans for low-cardinality string fields present in 90%+ of records
- Validates that groups have **different field sets** (not just different values)
- Produces per-type sub-schemas + main `oneOf` schema with `$ref` references
- Sub-schema references use **actual SR version numbers**, not hardcoded values
- Detection runs on every batch cycle using a rolling 200-record buffer

### Flat-to-Multi-Event Transition
When a topic transitions from flat to `oneOf`:
1. Temporarily sets subject compatibility to `NONE`
2. Registers all sub-schemas + main schema with `skip_compatibility_set=True`
3. Restores original compatibility in a `finally` block (guaranteed)
4. Optimistic guard: checks if SR already has `oneOf` (another instance may have transitioned)

## Thread Safety (Live Mode)

`LiveModeOrchestrator` uses a `ThreadPoolExecutor` for parallel batch processing.

### Locks
- `_states_lock` — protects `_states` dict (schema state per topic)
- `_metadata_lock` — protects all topic metadata dicts (formats, discriminators, partitions, event types, activity timestamps, flat-registered set, record buffer)
- `_stats_lock` — protects counters (total_messages, total_registrations, total_changes)
- `_registration_semaphore` — rate-limits both flat and multi-event SR registration calls
- **Lock ordering**: `_states_lock` before `_metadata_lock` if both needed

### Partition-0 Ownership
Only the instance owning partition 0 of a topic registers schemas. This prevents cross-instance races on SR operations. Other instances still process messages and build state — the merge-with-existing logic picks up their discoveries indirectly via SR.

### Config Safety
The shared `Config` object is never mutated by executor threads. The `skip_compatibility_set` parameter on `register_schema()` / `register_multi_event_schemas()` eliminates the need to temporarily modify `config.schema_registry.compatibility`.

## Commands

### `infer` — One-shot schema inference
```bash
# From Kafka
schema-infer --config config.yaml infer --topic orders --register
schema-infer --config config.yaml infer --topic-pattern "prod-.*" --output-dir ./schemas

# From local data (no Kafka required)
schema-infer infer --message '{"id": 1, "name": "test"}'
schema-infer infer --data-file events.jsonl --output schema.json --schema-name orders
```

### `live` — Continuous consumer with schema evolution
```bash
schema-infer --config config.yaml live --topic orders --register
schema-infer --config config.yaml live --topic-pattern ".*" --register --from-beginning
```
- Processes new messages continuously, detects field additions/type changes
- Re-discovers new topics matching prefix/pattern every 300s (`topic_discovery_interval_seconds`)
- Batch cycle controlled by `batch_size` (100) and `batch_timeout_seconds` (60)
- State persisted to disk for resume-on-restart
- `--from-beginning` for initial bootstrap from existing data

### `list-topics` / `validate-topics` — Discovery and validation

## Schema Registry Error Codes
| Code | Meaning |
|------|---------|
| `40401` | Subject not found |
| `40402` | Schema version not found |
| `40403` | Schema not found |
| `42201` | Invalid schema |
| `409` / `40901` | Incompatible schema |
| `401` / `403` | Auth failure |

## Config Defaults to Remember
| Parameter | Default | Why |
|-----------|---------|-----|
| `batch_timeout_seconds` | 60.0 | Discriminator detection runs every batch cycle |
| `max_depth` | 20 | Deep nesting support |
| `max_messages` | 50 | Sufficient for most topics |
| `compatibility` | BACKWARD | Confluent Cloud default |
| `additionalProperties` | false | Required for closed content model |
| `required` | [] (empty) | All fields optional for safe evolution |
| `topic_discovery_interval_seconds` | 300 | Re-discover new topics every 5 min |
| `min_records_before_register` | 10 | Don't register from too few samples |

## Testing
```bash
python3 -m pytest tests/ -q          # 251 tests
python3 -m pytest tests/ --cov       # Coverage report
```

Test files:
- `test_core_components.py` — Kafka consumer, SR client, topic discovery (37 tests)
- `test_format_detection.py` — Format detection and parsers (31 tests)
- `test_multi_event.py` — Discriminator detection, multi-event generation, merging (29 tests)
- `test_schema_generators.py` — Avro/Protobuf/JSON Schema generation (12 tests)
- `test_schema_inference.py` — Type inference, nesting, arrays, nullability (17 tests)
- `test_live_mode.py` — Partition ownership, rebalance, discriminator, transitions (27 tests)
- `test_performance.py` — PerformanceMonitor, BatchProcessor, CacheManager, AsyncProcessor (33 tests)
- `test_bugfixes.py` — Merger safety, closed content model, URL encoding, thread safety, config sync, error handling (65 tests)

## CI/CD
- `.github/workflows/ci.yml` — Tests on Python 3.9-3.13, lint (black/isort/flake8/mypy), package build
- `.github/workflows/release.yml` — On release publish: build PyInstaller binaries (Linux x86_64, macOS arm64, Windows x86_64) + PyPI upload
- Use `git push-external` to push (proprietary code check hook)

## Common Pitfalls
1. **Don't use `>=` for deps** — use `~=` (compatible release) in requirements.txt and pyproject.toml
2. **Don't mutate `self.config` from threads** — use `skip_compatibility_set` parameter instead. Config is deep-copied in `infer()` to prevent leaks.
3. **Don't set `required` fields** — all fields must be optional for BACKWARD compat
4. **Don't infer email/URI formats** — only `datetime` (with timezone/fractional seconds) and `date` are detected (by design)
5. **Don't use `except Exception: pass`** — always add `logger.debug()` at minimum
6. **Test deps must support Python 3.9** — pytest 9.x requires 3.10+, use `>=7.0.0`
7. **`macos-13` runners unavailable** — use `macos-latest` (arm64) for CI
8. **SR error responses** — only log `error_code` and `message`, not full response body. Error codes 409, 422, 401/403 produce differentiated messages.
9. **Config sync** — removed broken Pydantic validators; use `config.sync_convenience_to_nested()` / `config.sync_nested_to_convenience()` explicitly
10. **Cloud auth fails early** — missing `cloud_api_key`/`cloud_api_secret` raises `ConfigurationError`, not a warning
11. **`--message` validates input** — only JSON objects or arrays of objects accepted; primitives, strings, and null are rejected

## VARIANT Discussion Context
VARIANT (Iceberg v3) is the long-term answer for semi-structured data, but Confluent doesn't support it today (not in Flink, Tableflow, Connect, or SR). This tool bridges the gap. With VARIANT, Schema Registry and governance are not required — it depends on whether you want to trade governance for flexibility. The positioning: "Schema inference is like a Glue Crawler for Confluent Cloud."
