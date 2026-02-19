# Schema Inference Plugin

[![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)](https://github.com/example/schema-infer-plugin)
[![Python](https://img.shields.io/badge/python-3.9+-green.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-comprehensive-green.svg)](tests/)

A powerful CLI plugin that automatically infers and generates schemas from Kafka topic data. Supports multiple schema formats (JSON Schema, Avro, Protobuf) and handles complex nested data structures with comprehensive data type detection.

## 🚀 Features

- **🎯 Multi-Format Schema Generation**: JSON Schema, Avro, and Protobuf
- **🔍 Intelligent Data Analysis**: Automatic format detection, datetime/enum inference, and deep nested analysis
- **⚡ High Performance**: Parallel processing and optimized message reading
- **🎛️ Flexible Topic Discovery**: Single topics, multiple topics, prefix/pattern matching
- **🔐 Enterprise Security**: Full Schema Inference Cloud and Platform authentication support
- **📊 Schema Registry Integration**: Automatic registration with compatibility management
- **🛡️ Production Ready**: Schema validation, retry logic, and comprehensive error handling
- **👁️ Continuous Monitoring**: Watch mode for automatic schema inference on new topics
- **🔴 Live Consumer Mode**: Continuously consume topics, detect schema evolution, and re-register updated schemas
- **📈 Horizontal Scaling**: Multi-instance support with shared consumer groups for 1000+ topics

## 📖 Documentation

| Document | Description |
|----------|-------------|
| **[📚 Complete Documentation](DOCUMENTATION.md)** | Comprehensive product documentation with all features |
| **[⚡ Quick Start Guide](QUICK_START.md)** | Get up and running in minutes |
| **[🔧 API Reference](API_REFERENCE.md)** | Complete API documentation and class references |
| **[💡 Examples](EXAMPLES.md)** | Comprehensive examples for all use cases |
| **[🧪 Testing Guide](TESTING.md)** | Testing documentation and examples |
| **[🔧 Using with Terraform](TERRAFORM.md)** | Terraform module integration guide |
| **[📊 Using with Tableflow](TABLEFLOW.md)** | Materialize topics as Iceberg/Delta tables |

## 🏃‍♂️ Quick Start

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

### Basic Usage

```bash
# Generate JSON Schema from a topic
schema-infer infer --topic user-events --output user-schema.json --format json-schema

# Generate Avro schema and register in Schema Registry
schema-infer infer --topic user-events --format avro --register

# Process multiple topics
schema-infer infer --topics "user-events,order-events,payment-events" --output-dir schemas/ --format avro
```

## ⚙️ Configuration

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

## 🎯 Supported Formats

### Input Data Formats
- **JSON**: Automatic JSON parsing with nested object support
- **CSV**: Comma-separated values with header detection
- **Key-Value**: Space or equals-separated key-value pairs
- **Raw Text**: Fallback for unrecognized formats

### Intelligent Type Detection
- **Datetime**: ISO 8601 timestamps detected and annotated with `format: "date-time"`
- **Date**: Date strings detected and annotated with `format: "date"`
- **Enum**: Low-cardinality string fields automatically identified as enums
- **Arrays**: Proper array types with item schemas (e.g., `array<string>`, `array<object>`)
- **Nested Objects**: Deep nesting preserved in all output formats

### Output Schema Formats
- **JSON Schema**: Industry-standard JSON Schema (Draft 7)
- **Avro**: Apache Avro with nested record structures
- **Protobuf**: Protocol Buffers with nested message definitions

## 🔧 Advanced Features

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
# Register schema automatically
schema-infer infer --topic user-events --format avro --register

# Register all topics under a Schema Registry context
schema-infer infer --topic-pattern ".*" --format avro --register --context my-context --exclude-internal

# Configure compatibility levels
# In config.yaml:
schema_registry:
  compatibility: "BACKWARD"
  subject_name_strategy: "TopicNameStrategy"
```

### Watch Mode (New Topic Detection)
```bash
# Watch for new topics and auto-register Avro schemas
schema-infer --config config.yaml watch --register --context production

# Custom interval and format
schema-infer --config config.yaml watch --interval 30 --format json-schema --output-dir ./schemas

# Watch with topic filtering
schema-infer --config config.yaml watch --topic-pattern "prod-.*" --register --context prod
```

Watch mode continuously polls the cluster, detects new topics, infers schemas, and optionally registers them to Schema Registry. Topics are only processed once.

### Live Consumer Mode (Schema Evolution Detection)
```bash
# Continuously monitor a topic and register evolving schemas
schema-infer --config config.yaml live --topic orders --register

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

Unlike `watch` (which only detects new topics), `live` mode continuously reads new messages from existing topics, incrementally builds schemas, detects schema evolution (new fields, type changes), and re-registers updated schemas. Consumer offsets are tracked via Kafka consumer groups for resume-on-restart.

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

## ☁️ Schema Inference Cloud Setup

```yaml
kafka:
  bootstrap_servers: "pkc-xxxxx.us-west-2.aws.schema-infer.cloud:9092"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  cloud_api_key: "your-api-key"
  cloud_api_secret: "your-api-secret"

schema_registry:
  url: "https://psrc-xxxxx.us-west-2.aws.schema-infer.cloud"
  cloud_api_key: "your-api-key"
  cloud_api_secret: "your-api-secret"
```

**Note**: API keys and secrets are read directly from the YAML configuration file, not from environment variables.

## 🏢 Schema Inference Platform Setup

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

The Schema Inference Plugin integrates with the [Confluent Terraform Provider](https://registry.terraform.io/providers/confluentinc/confluent/latest) via a reusable Terraform module. Schemas are inferred automatically during `terraform plan` and registered to Confluent Cloud Schema Registry through `confluent_schema` resources.

> **Note**: Terraform integration is supported with **Confluent Cloud** only.

### Prerequisites

- Terraform >= 1.0
- Python 3.9+ (the module auto-installs `schema-infer` on first run)
- A Confluent Cloud cluster with data in topics

### Quick Start (Inline Variables)

No YAML config file needed -- pass Confluent Cloud credentials directly as Terraform variables:

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

### Using a Config File

If you already have a `cc-config.yaml` for the CLI, you can reference it instead:

```hcl
module "inferred_schemas" {
  source      = "github.com/akrishnanDG/terraform-schema-infer"
  config_file = "${path.module}/cc-config.yaml"
  topics      = ["orders", "payments", "users"]
  format      = "avro"
}
```

### How It Works

1. `terraform plan` calls the `schema-infer` CLI via Terraform's `external` data source
2. The CLI connects to Kafka, samples messages from each topic, and infers the schema
3. The inferred schema string is passed to the `confluent_schema` resource
4. `terraform apply` registers the schema in Schema Registry via the Confluent provider

### Module Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `topics` | Yes | - | List of Kafka topic names to infer schemas from |
| `format` | No | `avro` | Output format: `avro`, `protobuf`, or `json-schema` |
| `max_messages` | No | `100` | Maximum messages to sample per topic |
| `config_file` | No | - | Path to YAML config file (Option A) |
| `bootstrap_servers` | No | - | Confluent Cloud bootstrap servers (Option B) |
| `kafka_api_key` | No | - | Confluent Cloud Kafka API key (Option B) |
| `kafka_api_secret` | No | - | Confluent Cloud Kafka API secret (Option B) |

### Ongoing Usage

- **New topic?** Add it to the `topics` list, run `terraform plan/apply`
- **Schema evolved?** Run `terraform plan` -- it re-infers and shows the diff
- **Remove a topic?** Remove from the list, `terraform apply` destroys the schema

### Alternative: Generate Schemas Separately

Instead of inferring inline during `terraform plan`, generate schema files first and let Terraform read them from disk. This allows human review and works with `watch` and `live` modes.

```bash
# Generate schema files
schema-infer --config cc-config.yaml infer \
  --topic-pattern ".*" --format avro --output-dir ./schemas/ --exclude-internal

# Or run live mode to continuously update schemas as data evolves
schema-infer --config cc-config.yaml live \
  --topic-pattern ".*" --format avro --output-dir ./schemas/
```

```hcl
# Terraform reads schema files from disk -- no Kafka connection during plan
locals {
  schema_files = fileset("${path.module}/schemas", "*.avsc")
  topic_schemas = {
    for f in local.schema_files :
    trimsuffix(f, ".avsc") => file("${path.module}/schemas/${f}")
  }
}

resource "confluent_schema" "inferred" {
  for_each     = local.topic_schemas
  subject_name = "${each.key}-value"
  format       = "AVRO"
  schema       = each.value
  # ... cluster config, credentials
}
```

For the full Terraform guide including `watch` mode, `live` mode, and CI/CD integration, see [TERRAFORM.md](TERRAFORM.md).

## Using with Confluent Tableflow

Confluent [Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html) materializes Kafka topics as Apache Iceberg or Delta Lake tables, but requires schemas registered in Schema Registry. Schema inference bridges the gap for schemaless topics.

```bash
# Infer and register schemas, then enable Tableflow
schema-infer --config cc-config.yaml infer \
  --topics "orders,payments,users" --format avro --register
```

Or fully automated with Terraform:

```hcl
# Infer schemas
module "inferred_schemas" {
  source            = "github.com/akrishnanDG/terraform-schema-infer"
  bootstrap_servers = var.bootstrap_servers
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret
  topics            = ["orders", "payments", "users"]
  format            = "avro"
}

# Register schemas
resource "confluent_schema" "inferred" {
  for_each     = module.inferred_schemas.schemas
  subject_name = "${each.key}-value"
  format       = "AVRO"
  schema       = each.value
  # ... cluster config, credentials
}

# Enable Tableflow
resource "confluent_tableflow_topic" "materialized" {
  for_each      = module.inferred_schemas.schemas
  display_name  = each.key
  table_formats = ["ICEBERG"]
  managed_storage {}
  depends_on    = [confluent_schema.inferred]
  # ... environment, cluster, credentials
}
```

For the full guide including live mode, file-based approach, and troubleshooting, see [TABLEFLOW.md](TABLEFLOW.md).

## 💼 Use Cases

- **🔄 Schema Migration**: Migrate from untyped to typed data systems
- **📚 API Documentation**: Generate schemas for API documentation
- **🛡️ Data Governance**: Establish data contracts and validation rules
- **⚡ Development Acceleration**: Quickly bootstrap schema definitions
- **📋 Compliance**: Meet data governance and compliance requirements

## 📊 Examples

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

## ⚡ Performance Features

- **🔄 Parallel Processing**: Multi-threaded topic processing for message reading, schema generation, and schema registration
- **📈 Optimized Message Reading**: Smart offset selection and batch processing
- **🔗 Connection Reuse**: Efficient Kafka consumer management
- **📊 Progress Tracking**: Real-time progress bars and ETA
- **💾 Memory Management**: Configurable memory limits and streaming processing
- **📤 Parallel Schema Registration**: Concurrent schema registration to Schema Registry using configurable worker threads (controlled by `max_workers`)

## 🎛️ Configuration Options

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
  max_workers: 8          # Worker threads for parallel message reading, schema generation, and registration
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

### Live Consumer Configuration
```yaml
live:
  consumer_group: "schema-infer-live"   # Stable consumer group for offset tracking
  batch_size: 100                        # Messages per batch before re-inferring
  batch_timeout_seconds: 30.0            # Max wait for a batch
  initial_offset: "latest"              # Start from latest or earliest
  persist_state: true                    # Resume from where you left off
  state_dir: "~/.schema-infer/state"    # State persistence directory
  min_records_before_register: 10       # Min records before first registration
  on_incompatible: "skip"               # skip, log, force, or fail
```

## 🧪 Testing

The plugin includes comprehensive unit tests covering all functionality:

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

## 🆘 Troubleshooting

### Common Issues

1. **🔌 Connection Issues**: Check bootstrap servers and authentication
2. **📋 Schema Registry Issues**: Verify URL and credentials
3. **⚡ Performance Issues**: Adjust timeout and worker settings
4. **💾 Memory Issues**: Increase memory limits or reduce batch sizes
5. **📭 Empty Topics**: When processing many topics in parallel with a high `max_workers` value, some topics may appear empty due to broker connection saturation. Each worker creates its own consumer connection, and too many concurrent connections can cause timeouts during the initial offset fetch. Reduce `max_workers` or increase `--timeout` to mitigate this.

### Debug Mode

Enable verbose logging for detailed debugging:

```yaml
performance:
  verbose_logging: true
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

Apache License 2.0

## 🆘 Support

- **📚 Documentation**: [Complete Documentation](DOCUMENTATION.md)
- **🐛 Issues**: [GitHub Issues](https://github.com/akrishnanDG/schema-infer-plugin/issues)
- **💬 Discussions**: [GitHub Discussions](https://github.com/akrishnanDG/schema-infer-plugin/discussions)
- **🏢 Schema Inference Support**: This is an open source tool, there is no support provided but please feel free to raise and fix issues.

---

**Ready to get started?** Check out the [Quick Start Guide](QUICK_START.md) or dive into the [Complete Documentation](DOCUMENTATION.md) for comprehensive information about all features and capabilities.
