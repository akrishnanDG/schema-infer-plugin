# Schema Inference Plugin

[![Version](https://img.shields.io/badge/version-1.4.3-blue.svg)](https://github.com/example/schema-infer-plugin)
[![Python](https://img.shields.io/badge/python-3.9+-green.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-comprehensive-green.svg)](tests/)

A CLI plugin that infers and generates schemas from Kafka topic data. Supports JSON Schema, Avro, and Protobuf output formats with nested structure analysis and automatic type detection.

## Features

- **Multi-Format Schema Generation**: JSON Schema, Avro, and Protobuf
- **Intelligent Data Analysis**: Automatic format detection, datetime/enum inference, deep nested analysis (max depth 20), and unified numeric types (all numbers inferred as `number`)
- **High Performance**: Parallel processing and optimized message reading
- **Flexible Topic Discovery**: Single topics, multiple topics, prefix/pattern matching
- **Enterprise Security**: Full Confluent Cloud and Confluent Platform authentication support
- **Schema Registry Integration**: Automatic registration with compatibility management
- **Multi-Event Detection**: Auto-detect topics with multiple event types, generate per-type schemas with `oneOf` references using accurate SR version numbers; discriminator re-evaluation runs on every batch cycle in live mode
- **Production Ready**: Schema validation, retry logic, deep recursive schema merging (objects + array items), flat-to-multi-event transition handling, and comprehensive error handling
- **Continuous Monitoring**: Live mode automatically discovers new topics matching prefix/pattern filters
- **Live Consumer Mode**: Continuously consume topics, detect schema evolution, and re-register updated schemas with `--from-beginning` bootstrap support
- **Horizontal Scaling**: Thread-safe multi-instance support with shared consumer groups for 1000+ topics

## Documentation

| Document | Description |
|----------|-------------|
| **[Complete Documentation](DOCUMENTATION.md)** | Full product documentation covering all features |
| **[Quick Start Guide](QUICK_START.md)** | Installation and first run |
| **[API Reference](API_REFERENCE.md)** | API documentation and class references |
| **[Examples](EXAMPLES.md)** | Usage examples for all supported workflows |
| **[Testing Guide](TESTING.md)** | Testing documentation and examples |
| **[Best Practices](BEST_PRACTICES.md)** | Production deployment and operational guidelines |
| **[Using with Terraform](TERRAFORM.md)** | Terraform module integration guide |
| **[Using with Tableflow](TABLEFLOW.md)** | Materialize topics as Iceberg/Delta tables |

## Quick Start

### Prerequisites

- Python 3.9+
- pip (included with Python; if missing, run `python3 -m ensurepip --upgrade`)
- Access to a Kafka cluster

### Installation

```bash
# Install from source
pip install git+https://github.com/akrishnanDG/schema-infer-plugin.git

# Or clone and install locally
git clone https://github.com/akrishnanDG/schema-infer-plugin.git
cd schema-infer-plugin
pip install .

# Verify installation
schema-infer --help
```

### Standalone Binaries

Pre-built binaries are available on the [Releases](https://github.com/akrishnanDG/schema-infer-plugin/releases) page. No Python installation required.

```bash
# Download (example: macOS Apple Silicon)
curl -L -o schema-infer \
  https://github.com/akrishnanDG/schema-infer-plugin/releases/latest/download/schema-infer-macos-arm64
chmod +x schema-infer
./schema-infer --help
```

### Docker

```bash
# Build
docker build -t schema-infer .

# Run with a config file
docker run --rm -v $(pwd)/config.yaml:/app/config.yaml \
  schema-infer --config /app/config.yaml infer --topic my-topic

# Run with inline Confluent Cloud credentials
docker run --rm schema-infer \
  --bootstrap-servers pkc-xxxxx.us-east-1.aws.confluent.cloud:9092 \
  --kafka-api-key YOUR_KEY --kafka-api-secret YOUR_SECRET \
  --schema-registry-url https://psrc-xxxxx.us-east-1.aws.confluent.cloud \
  --sr-api-key YOUR_SR_KEY --sr-api-secret YOUR_SR_SECRET \
  infer --topic orders --register

# Write schemas to a local directory
docker run --rm -v $(pwd)/schemas:/app/schemas \
  -v $(pwd)/config.yaml:/app/config.yaml \
  schema-infer --config /app/config.yaml infer \
  --topic-pattern ".*" --output-dir /app/schemas --register

# Live mode
docker run --rm -v $(pwd)/config.yaml:/app/config.yaml \
  -v $(pwd)/state:/app/state \
  schema-infer --config /app/config.yaml live \
  --topic orders --register --state-dir /app/state
```

### Basic Usage

```bash
# Generate JSON Schema from a topic (json-schema is the default format)
schema-infer infer --topic user-events --output user-schema.json

# Generate schema and register in Schema Registry
schema-infer infer --topic user-events --register

# Process multiple topics
schema-infer infer --topics "user-events,order-events,payment-events" --output-dir schemas/

# Generate Avro or Protobuf instead
schema-infer infer --topic user-events --format avro --register
schema-infer infer --topic user-events --format protobuf --output user-schema.proto

# Infer from JSON directly (no Kafka required)
schema-infer infer --message '{"user_id": "123", "name": "John", "age": 30}'
schema-infer infer --data-file sample-data.jsonl --output schema.json --schema-name orders
```

## Configuration

Create a YAML configuration file:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  security_protocol: "PLAINTEXT"

schema_registry:
  url: "http://localhost:8081"

inference:
  max_messages: 50
  timeout: 30

performance:
  show_progress: true
  verbose_logging: false
```

## Supported Formats

### Input Data Formats
- **JSON**: Automatic JSON parsing with nested object support
- **CSV**: Comma-separated values with header detection
- **Key-Value**: Space or equals-separated key-value pairs
- **Raw Text**: Fallback for unrecognized formats

### Intelligent Type Detection
- **Numbers**: All numeric types (int, float) unified as `number` in JSON Schema (`double` in Avro/Protobuf) to prevent compatibility errors when a field appears as integer in one batch and float in another
- **Datetime**: ISO 8601 timestamps detected and annotated with `format: "date-time"`
- **Date**: Date strings detected and annotated with `format: "date"`
- **Enum**: Low-cardinality string fields automatically identified as enums
- **Arrays**: Proper array types with item schemas (e.g., `array<string>`, `array<object>`)
- **Nested Objects**: Deep nesting preserved in all output formats (max depth: 20 levels)

### Output Schema Formats
- **JSON Schema**: Industry-standard JSON Schema (Draft 7)
- **Avro**: Apache Avro with nested record structures
- **Protobuf**: Protocol Buffers with nested message definitions

## Advanced Features

### Topic Discovery
```bash
# List all topics
schema-infer list-topics

# Process topics by prefix
schema-infer infer --topic-prefix "prod-" --format avro

# Process topics by pattern
schema-infer infer --topic-pattern ".*-events" --format json-schema
```

### Schema Registry Integration
```bash
# Register schema automatically (default: json-schema)
schema-infer infer --topic user-events --register

# Register all topics under a Schema Registry context
schema-infer infer --topic-pattern ".*" --register --context my-context --exclude-internal

# Configure compatibility levels
# In config.yaml:
schema_registry:
  compatibility: "BACKWARD"
  subject_name_strategy: "TopicNameStrategy"
```

### Multi-Event Schema Detection

When a topic contains multiple event types (e.g., `user_created`, `payment_processed`, `order_placed`), the tool automatically detects this and generates separate schemas per event type with a main `oneOf` schema using Schema Registry references.

**How it works:**
1. Auto-detects a discriminator field (`event_type`, `type`, `action`, etc.) -- re-evaluates every 500 records until found
2. Validates that different discriminator values produce different field sets (same schema with different values is not split)
3. Generates individual sub-schemas for each event type
4. Creates a main topic schema using `oneOf` with `$ref` to sub-schemas
5. Registers sub-schemas as separate subjects, then the main schema with references using actual SR version numbers
6. If a flat schema was previously registered, handles the transition by temporarily setting compatibility to NONE, then restoring it

```bash
# Auto-detect and split (default behavior)
schema-infer infer --topic events --output-dir ./schemas --register

# Override the discriminator field
schema-infer infer --topic events --discriminator event_type --output-dir ./schemas

# Force single flat schema (disable multi-event detection)
schema-infer infer --topic events --flatten --output-dir ./schemas
```

**Output for a topic with `user_created` and `payment_processed` events:**
```
schemas/events.json                     # main oneOf schema with $ref
schemas/events.user_created.json        # user event sub-schema
schemas/events.payment_processed.json   # payment event sub-schema
```

**Schema Registry subjects (with --register):**
```
events-user_created       # sub-schema subject
events-payment_processed  # sub-schema subject
events-value              # main schema with references to sub-schemas
```

> **Note:** Multi-event detection, schema references, and schema merging are JSON Schema features only. Avro and Protobuf formats use flat schemas. Multi-event is supported in both `infer` and `live` modes for JSON Schema.

### Live Consumer Mode (Schema Evolution and Topic Discovery)
```bash
# Continuously monitor a topic and register evolving schemas
schema-infer --config config.yaml live --topic orders --register

# Bootstrap from existing topic data (reads from beginning)
schema-infer --config config.yaml live --topic orders --register --from-beginning

# Monitor multiple topics with custom batch settings
schema-infer --config config.yaml live \
  --topics "orders,payments,users" \
  --register --format avro \
  --batch-size 200 --batch-timeout 60

# Scale to 1000+ topics with multiple instances sharing a consumer group
schema-infer --config config.yaml live \
  --topic-pattern ".*" --register \
  --consumer-group my-live-group \
  --state-dir /shared/state

# Handle incompatible schema changes
schema-infer --config config.yaml live \
  --topic orders --register --on-incompatible force
```

Live mode continuously reads new messages from topics, discovers new topics matching prefix/pattern filters, incrementally builds schemas, detects schema evolution (new fields, type changes), and re-registers updated schemas. By default, live mode only processes new messages (`latest`). Use `--from-beginning` to bootstrap schemas from existing topic data. Consumer offsets are tracked via Kafka consumer groups for resume-on-restart.

### Performance Optimization
```bash
# High-performance processing
schema-infer infer --topic large-topic --max-messages 5000 --format protobuf

# Parallel processing
# In config.yaml:
performance:
  max_workers: 8
  batch_size: 200
  enable_caching: true
```

## Confluent Cloud Setup

```yaml
kafka:
  bootstrap_servers: "pkc-xxxxx.us-west-2.aws.confluent.cloud:9092"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  cloud_api_key: "your-api-key"
  cloud_api_secret: "your-api-secret"

schema_registry:
  url: "https://psrc-xxxxx.us-west-2.aws.confluent.cloud"
  cloud_api_key: "your-api-key"
  cloud_api_secret: "your-api-secret"
```

**Note**: API keys and secrets are read directly from the YAML configuration file, not from environment variables.

## Confluent Platform Setup

```yaml
kafka:
  bootstrap_servers: "kafka-0.kafka.default.svc.cluster.local:9071"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  sasl_username: "kafka"
  sasl_password: "kafka-secret"

schema_registry:
  url: "https://schemaregistry:8081"
```

## Using with Terraform

The plugin integrates with the [Confluent Terraform Provider](https://registry.terraform.io/providers/confluentinc/confluent/latest) via a reusable Terraform module. Schemas are inferred during `terraform plan` and registered to Confluent Cloud Schema Registry through `confluent_schema` resources.

> **Note**: Terraform integration is supported with **Confluent Cloud** only.

```hcl
module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"

  bootstrap_servers = var.bootstrap_servers
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret

  topics = ["orders", "payments", "users"]
  format = "avro"
}

resource "confluent_schema" "inferred" {
  for_each     = module.inferred_schemas.schemas
  subject_name = "${each.key}-value"
  format       = "AVRO"
  schema       = each.value

  schema_registry_cluster {
    id = confluent_schema_registry_cluster.main.id
  }
  rest_endpoint = confluent_schema_registry_cluster.main.rest_endpoint
  credentials {
    key    = var.sr_api_key
    secret = var.sr_api_secret
  }
}
```

For the full Terraform guide including config file usage, module variables, `live` mode integration, and CI/CD patterns, see [TERRAFORM.md](TERRAFORM.md).

## Using with Confluent Tableflow

Confluent [Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html) materializes Kafka topics as Apache Iceberg or Delta Lake tables, but requires schemas registered in Schema Registry. Schema inference bridges the gap for schemaless topics.

```bash
# Infer and register schemas, then enable Tableflow
schema-infer --config cc-config.yaml infer \
  --topics "orders,payments,users" --format avro --register
```

Tableflow can also be fully automated with Terraform by combining the schema inference module with `confluent_tableflow_topic` resources.

For the full guide including Terraform integration, live mode, and troubleshooting, see [TABLEFLOW.md](TABLEFLOW.md).

## Use Cases

- **Schema Migration**: Migrate from untyped to typed data systems
- **API Documentation**: Generate schemas for API documentation
- **Data Governance**: Establish data contracts and validation rules
- **Development Acceleration**: Bootstrap schema definitions from existing data
- **Compliance**: Meet data governance and compliance requirements

## Examples

### E-commerce Platform
```bash
# Generate schemas for all e-commerce topics
schema-infer infer --topic-prefix "ecommerce-" --output-dir schemas/ --format avro

# Register critical schemas
schema-infer infer --topics "user-events,order-events,payment-events" --format avro --register
```

### IoT Data Processing
```bash
# Process IoT topics with high message count
schema-infer infer --topic-prefix "iot-" --max-messages 5000 --format json-schema --output-dir iot-schemas/
```

### Microservices Architecture
```bash
# Process different service topics
schema-infer infer --topics "user-service,order-service,payment-service" --format protobuf --output-dir microservices/

# Generate JSON schemas for API documentation
schema-infer infer --topic-pattern ".*-api" --format json-schema --output-dir api-docs/
```

## Performance Features

- **Parallel Processing**: Multi-threaded topic processing for message reading, schema generation, and schema registration
- **Optimized Message Reading**: Smart offset selection and batch processing
- **Connection Reuse**: Efficient Kafka consumer management
- **Progress Tracking**: Real-time progress bars and ETA
- **Memory Management**: Configurable memory limits and streaming processing
- **Parallel Schema Registration**: Concurrent schema registration to Schema Registry using configurable worker threads (controlled by `max_workers`)

## Configuration Options

### Topic Filtering
```yaml
topic_filter:
  internal_prefix: "__"
  exclude_internal: true
  additional_exclude_prefixes: ["__", "temp-", "backup-"]
  include_patterns: [".*-events", "prod-.*"]
```

### Performance Tuning
```yaml
performance:
  max_workers: 8          # Worker threads for parallel schema generation and registration
  batch_size: 100
  memory_limit_mb: 512
  enable_caching: true
  cache_ttl: 3600
  show_progress: true
  verbose_logging: false
```

### Schema Registry Configuration
```yaml
schema_registry:
  compatibility: "BACKWARD"  # NONE, BACKWARD, FORWARD, FULL
  subject_name_strategy: "TopicNameStrategy"  # TopicName, RecordName, TopicRecordName
  context: "my-context"  # Optional: prefix subjects with :.my-context:
```

Generated JSON Schemas use a closed content model (`additionalProperties: false`), which allows adding optional fields under BACKWARD compatibility per Confluent's schema evolution rules. This means schemas can safely evolve across multiple inference runs without compatibility errors.

### Schema Merging Behavior

When merging with existing schemas in Schema Registry:
- **New fields** are added to the existing schema
- **Existing fields** are preserved (never removed or narrowed)
- **Type conflicts**: existing type is kept to avoid compatibility errors
- **Nested objects**: recursively deep-merged at each level
- **Array items**: recursively merged -- `items.properties` are deep-merged to prevent `COMBINED_TYPE_SUBSCHEMAS_CHANGED` errors
- The existing schema is always used as the base, ensuring no existing types or structures are overwritten

### Live Consumer Configuration
```yaml
live:
  consumer_group: "schema-infer-live"   # Stable consumer group for offset tracking
  batch_size: 100                        # Messages per batch before re-inferring
  batch_timeout_seconds: 60.0            # Max wait for a batch
  initial_offset: "latest"              # Start from latest or earliest (or use --from-beginning)
  persist_state: true                    # Resume from where you left off
  state_dir: "~/.schema-infer/state"    # State persistence directory
  min_records_before_register: 10       # Min records before first registration
  on_incompatible: "skip"               # skip, log, force, or fail
```

## Testing

The plugin includes unit tests covering all functionality:

```bash
# Run all tests
python run_tests.py

# Run specific test types
python run_tests.py --type generators
python run_tests.py --type inference
python run_tests.py --type format
python run_tests.py --type core

# Run with coverage
python run_tests.py --coverage
```

## Troubleshooting

### Common Issues

1. **Connection Issues**: Check bootstrap servers and authentication
2. **Schema Registry Issues**: Verify URL and credentials
3. **Performance Issues**: Adjust timeout and worker settings
4. **Memory Issues**: Increase memory limits or reduce batch sizes
5. **Empty Topics**: When processing many topics, some may appear empty due to broker connection timeouts. The `infer` command uses a small reader pool (10 consumer connections) separate from the processing worker pool to avoid connection saturation. If you still see empty topics, try increasing `--timeout` to give readers more time per topic.
6. **Schema Compatibility Errors**: Generated schemas use a closed content model (`additionalProperties: false`) which supports BACKWARD-compatible evolution (adding optional fields). If you see `PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL` errors, ensure your schemas use `additionalProperties: false` -- an open content model (where `additionalProperties` is omitted or `true`) does not allow adding properties under BACKWARD compatibility on Confluent Cloud.

### Debug Mode

Enable verbose logging for detailed debugging:

```yaml
performance:
  verbose_logging: true
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

Apache License 2.0

## Support

- **Documentation**: [Complete Documentation](DOCUMENTATION.md)
- **Issues**: [GitHub Issues](https://github.com/akrishnanDG/schema-infer-plugin/issues)
- **Discussions**: [GitHub Discussions](https://github.com/akrishnanDG/schema-infer-plugin/discussions)
- **Support**: This is an open source tool with no formal support. Please report bugs and submit fixes via GitHub Issues.

---

See the [Quick Start Guide](QUICK_START.md) for installation steps or the [Complete Documentation](DOCUMENTATION.md) for full reference.
