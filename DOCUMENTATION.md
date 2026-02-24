# Schema Inference Plugin - Product Documentation

## Table of Contents

1. [Overview](#overview)
2. [Features](#features)
3. [Installation](#installation)
4. [Quick Start](#quick-start)
5. [Configuration](#configuration)
6. [Usage Guide](#usage-guide)
7. [Schema Formats](#schema-formats)
8. [Data Format Support](#data-format-support)
9. [Performance & Optimization](#performance--optimization)
10. [Advanced Features](#advanced-features)
11. [Using with Terraform](#using-with-terraform)
12. [Using with Confluent Tableflow](#using-with-confluent-tableflow)
13. [Troubleshooting](#troubleshooting)
14. [API Reference](#api-reference)
15. [Examples](#examples)
16. [Best Practices](#best-practices)
17. [Limitations](#limitations)
18. [Support](#support)

---

## Overview

The **Schema Inference Plugin** is a CLI tool that automatically infers and generates schemas from Kafka topic data. It supports multiple schema formats (JSON Schema, Avro, Protobuf) and can handle complex nested data structures with comprehensive data type detection.

### Key Benefits

- **Automatic Schema Discovery**: No manual schema writing required
- **Multi-Format Support**: Generate JSON Schema, Avro, and Protobuf schemas
- **Deep Nested Analysis**: Handles complex nested objects and arrays
- **High Performance**: Optimized for large topics and high-throughput scenarios
- **Cloud & Platform Support**: Works with Confluent Cloud and Confluent Platform
- **Production Ready**: Robust error handling and comprehensive logging

### Use Cases

- **Schema Migration**: Migrate from untyped to typed data systems
- **API Documentation**: Generate schemas for API documentation
- **Data Governance**: Establish data contracts and validation rules
- **Development Acceleration**: Quickly bootstrap schema definitions
- **Compliance**: Meet data governance and compliance requirements

---

## Features

### Core Features

#### Multi-Format Schema Generation
- **JSON Schema**: Industry-standard JSON Schema (Draft 7)
- **Avro**: Apache Avro with nested record structures
- **Protobuf**: Protocol Buffers with nested message definitions

#### Intelligent Data Analysis
- **Automatic Format Detection**: JSON, CSV, key-value, raw text
- **Deep Nested Analysis**: Up to 20 levels of nesting
- **Comprehensive Type Detection**: string, number (unified int/float), boolean, null, arrays, objects
- **Array Handling**: Arrays of primitives, objects, and mixed types

#### High Performance
- **Parallel Processing**: Multi-threaded topic processing
- **Optimized Message Reading**: Smart offset selection and batch processing
- **Connection Reuse**: Efficient Kafka consumer management
- **Progress Tracking**: Real-time progress bars and ETA

#### Flexible Topic Discovery
- **Single Topic**: Process individual topics
- **Multiple Topics**: Process comma-separated topic lists
- **Prefix Matching**: Process all topics with specific prefixes
- **Pattern Matching**: Regex-based topic selection
- **Smart Filtering**: Exclude internal topics and system topics

#### Enterprise Security
- **Confluent Cloud**: Full API key/secret authentication
- **Confluent Platform**: SASL/SSL authentication support
- **Schema Registry**: Secure schema registration and management
- **Configurable Security**: Flexible authentication mechanisms

### Advanced Features

#### Schema Registry Integration
- **Automatic Registration**: Register schemas in Schema Registry
- **Compatibility Levels**: Configurable compatibility settings
- **Subject Strategies**: TopicName, RecordName, TopicRecordName strategies
- **Version Management**: Automatic versioning and evolution with accurate version references
- **Deep Schema Merging**: Recursively merges nested objects and array items with existing SR schemas
- **Flat-to-Multi-Event Transition**: Automatically handles structural schema changes with temporary compatibility override

#### Customization Options
- **Configurable Depth**: Adjustable nesting depth limits
- **Format Preferences**: Force specific data format detection
- **Output Options**: File output, directory output, or registry registration
- **Verbose Logging**: Detailed debugging and monitoring

#### Developer Experience
- **Progress Indicators**: Visual progress bars and status updates
- **Comprehensive Logging**: Detailed operation logs
- **Error Handling**: Graceful error recovery and reporting
- **Configuration Management**: YAML-based configuration system

---

## Installation

### Prerequisites

- **Python 3.9+**: Required for running the plugin
- **pip**: Included with Python; if missing, run `python3 -m ensurepip --upgrade`
- **Kafka Access**: Access to Kafka cluster (Confluent Cloud, Confluent Platform, or any Kafka cluster)
- **Schema Registry Access**: Optional, for schema registration

### Quick Installation

```bash
# Install directly from GitHub
pip install git+https://github.com/akrishnanDG/schema-infer-plugin.git

# Verify installation
schema-infer --help
```

### Install from Source

```bash
# Clone the repository
git clone https://github.com/akrishnanDG/schema-infer-plugin.git
cd schema-infer-plugin

# Install the package
pip install .

# For development (editable mode with live code changes)
pip install -e ".[dev]"

# Verify installation
schema-infer --help
```

### Docker Installation

```bash
# Build Docker image
docker build -t schema-infer .

# Run with Docker
docker run -v $(pwd)/config:/app/config schema-infer \
  --config /app/config/schema-infer.yaml infer --topic my-topic
```

---

## Quick Start

### 1. Basic Schema Inference

```bash
# Infer schema from a single topic
schema-infer infer --topic user-events --output user-schema.json --format json-schema
```

### 2. Multiple Topics

```bash
# Process multiple topics
schema-infer infer --topics "user-events,order-events,payment-events" --output-dir schemas/ --format avro
```

### 3. Topic Prefix Matching

```bash
# Process all topics with specific prefix
schema-infer infer --topic-prefix "prod-" --output-dir schemas/ --format protobuf
```

### 4. Register in Schema Registry

```bash
# Register schema in Schema Registry
schema-infer infer --topic user-events --format avro --register
```

### 5. List Available Topics

```bash
# List all topics (excluding internal topics)
schema-infer list-topics
```

---

## Configuration

### Configuration File Structure

The plugin uses YAML configuration files for comprehensive settings:

```yaml
# Kafka Configuration
kafka:
  bootstrap_servers: "localhost:9092"
  auto_offset_reset: "latest"
  session_timeout_ms: 30000
  heartbeat_interval_ms: 10000
  security_protocol: "PLAINTEXT"
  
  # Confluent Cloud Authentication
  cloud_api_key: "your-api-key"
  cloud_api_secret: "your-api-secret"

# Schema Registry Configuration
schema_registry:
  url: "http://localhost:8081"
  auth: null
  compatibility: "BACKWARD"
  subject_name_strategy: "TopicNameStrategy"
  context: null  # Optional: prefix subjects with :.context-name:

  # Confluent Cloud Authentication
  cloud_api_key: "your-api-key"
  cloud_api_secret: "your-api-secret"

# Schema Inference Configuration
inference:
  max_messages: 50
  timeout: 30
  max_depth: 20
  confidence_threshold: 0.8
  auto_detect_format: true
  forced_data_format: null

# Performance Configuration
performance:
  background: false
  max_workers: 4
  batch_size: 100
  memory_limit_mb: 512
  enable_caching: true
  cache_ttl: 3600
  show_progress: true
  verbose_logging: false

# Topic Filtering Configuration
topic_filter:
  internal_prefix: "_"
  exclude_internal: true
  additional_exclude_prefixes: ["__", "temp-", "backup-"]
  include_patterns: [".*-events", "prod-.*"]

# Live Consumer Mode Configuration
live:
  consumer_group: "schema-infer-live"     # Stable consumer group for offset tracking
  batch_size: 100                          # Messages per batch (auto-scales with topic count)
  batch_timeout_seconds: 60.0             # Max seconds to wait for batch_size messages
  state_dir: "~/.schema-infer/state"      # Directory for persisting schema state
  persist_state: true                      # Enable state persistence for resume-on-restart
  initial_offset: "latest"                # Where to start if no committed offsets (earliest/latest)
  min_records_before_register: 10         # Min records before first schema registration
  on_incompatible: "skip"                 # Behavior on incompatible schemas: skip, log, force, fail
  idle_evict_seconds: 3600                # Evict idle topic state from memory after this many seconds
  max_concurrent_registrations: 5         # Max parallel schema registrations
  summary_interval_seconds: 60            # Periodic status summary interval (for many topics)
```

### Configuration Notes

**Important**: API keys and secrets are read directly from the YAML configuration file, not from environment variables. This provides better security and easier configuration management.

For advanced use cases, you can still use environment variables for other settings, but the plugin's authentication is handled through the YAML configuration file.

### Configuration Examples

#### Confluent Cloud Configuration

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

#### Confluent Platform Configuration

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

---

## Usage Guide

### Command Structure

```bash
schema-infer [GLOBAL_OPTIONS] COMMAND [COMMAND_OPTIONS]
```

### Global Options

- `--config, -c`: Path to YAML configuration file
- `--help, -h`: Show help message
- `--version, -v`: Show version information

### Commands

#### `infer` - Schema Inference

Generate schemas from Kafka topics.

```bash
schema-infer infer [OPTIONS]
```

**Options:**
- `--topic, -t`: Single topic name
- `--topics`: Comma-separated list of topics
- `--topic-prefix`: Process topics with specific prefix
- `--topic-pattern`: Regex pattern for topic selection
- `--output, -o`: Output file path (single topic)
- `--output-dir, -d`: Output directory (multiple topics)
- `--format, -f`: Schema format (json-schema, avro, protobuf)
- `--register, -r`: Register schema in Schema Registry
- `--max-messages, -m`: Maximum messages to read (default: 50)
- `--timeout, -T`: Timeout in seconds (default: 30)
- `--exclude-internal`: Exclude internal topics
- `--internal-prefix`: Custom internal topic prefix
- `--additional-exclude-prefixes`: Additional prefixes to exclude
- `--include-patterns`: Patterns to include

**Examples:**

```bash
# Single topic with JSON Schema
schema-infer infer --topic user-events --output user-schema.json --format json-schema

# Multiple topics with Avro
schema-infer infer --topics "user-events,order-events" --output-dir schemas/ --format avro

# Prefix matching with Protobuf
schema-infer infer --topic-prefix "prod-" --output-dir schemas/ --format protobuf

# Register in Schema Registry
schema-infer infer --topic user-events --format avro --register

# Custom configuration
schema-infer infer --topic user-events --max-messages 5000 --timeout 60 --format json-schema
```

#### `list-topics` - Topic Discovery

List available topics with filtering options.

```bash
schema-infer list-topics [OPTIONS]
```

**Options:**
- `--exclude-internal`: Exclude internal topics
- `--internal-prefix`: Custom internal topic prefix
- `--additional-exclude-prefixes`: Additional prefixes to exclude
- `--include-patterns`: Patterns to include
- `--show-metadata`: Show topic metadata

**Examples:**

```bash
# List all topics
schema-infer list-topics

# Exclude internal topics
schema-infer list-topics --exclude-internal

# Custom filtering
schema-infer list-topics --internal-prefix "internal-" --additional-exclude-prefixes "temp-,backup-"
```

#### `validate-topics` - Topic Validation

Validate topics for schema inference.

```bash
schema-infer validate-topics [OPTIONS]
```

**Options:**
- `--topics`: Comma-separated list of topics to validate
- `--check-connectivity`: Check Kafka connectivity
- `--check-schema-registry`: Check Schema Registry connectivity

**Examples:**

```bash
# Validate specific topics
schema-infer validate-topics --topics "user-events,order-events"

# Full validation
schema-infer validate-topics --topics "user-events" --check-connectivity --check-schema-registry
```

#### `watch` - Continuous Monitoring

Continuously monitor the cluster for new topics, infer schemas, and register them automatically.

```bash
# Basic watch with registration
schema-infer --config config.yaml watch --register --context production

# Watch with custom settings
schema-infer --config config.yaml watch \
  --topic-pattern "prod-.*" \
  --format avro \
  --register \
  --context production \
  --interval 30 \
  --max-messages 100 \
  --output-dir ./schemas
```

| Option | Default | Description |
|--------|---------|-------------|
| `--topic-pattern` | `.*` | Regex pattern to filter topics |
| `--format` | `avro` | Schema format (avro, protobuf, json-schema) |
| `--output-dir` | `./schemas` | Directory for schema files |
| `--register` | off | Register schemas to Schema Registry |
| `--context` | none | Schema Registry context prefix |
| `--interval` | `60` | Polling interval in seconds |
| `--max-messages` | `50` | Messages to sample per topic |
| `--exclude-internal` | on | Exclude internal topics |

The watch command:
- Detects new topics on each polling cycle
- Infers schemas and saves to disk
- Validates schemas before registration
- Skips previously processed topics
- Handles errors gracefully without stopping
- Stops cleanly on Ctrl+C with a summary

#### `live` - Live Consumer Mode (Schema Evolution)

Continuously consume messages from Kafka topics, incrementally build schemas, detect schema evolution, and re-register updated schemas to Schema Registry. Unlike `infer` (one-shot) and `watch` (new topics only), `live` tracks how data shapes change over time in existing topics.

```bash
# Basic: monitor one topic and register changes
schema-infer --config config.yaml live --topic orders --register

# Bootstrap from existing topic data (reads from beginning)
schema-infer --config config.yaml live --topic orders --register --from-beginning

# Multiple topics with custom batch tuning
schema-infer --config config.yaml live \
  --topics "orders,payments,users" \
  --register --format avro \
  --batch-size 200 --batch-timeout 60

# Pattern matching with output files
schema-infer --config config.yaml live \
  --topic-pattern "^prod-.*" \
  --output-dir ./schemas --register

# Force-register incompatible changes
schema-infer --config config.yaml live \
  --topic orders --register --on-incompatible force
```

| Option | Default | Description |
|--------|---------|-------------|
| `--topic / -t` | none | Single topic name |
| `--topics` | none | Comma-separated topic list |
| `--topic-prefix` | none | Match topics by prefix |
| `--topic-pattern` | none | Match topics by regex |
| `--format / -f` | `avro` | Schema format (avro, protobuf, json-schema) |
| `--output-dir` | none | Directory for schema files |
| `--register` | off | Register/update schemas in Schema Registry |
| `--context` | none | Schema Registry context prefix |
| `--consumer-group` | `schema-infer-live` | Consumer group for offset tracking |
| `--batch-size` | `100` | Messages per batch (auto-scales with topic count) |
| `--batch-timeout` | `60.0` | Seconds to wait for a batch |
| `--state-dir` | `~/.schema-infer/state/` | State persistence directory |
| `--no-persist-state` | off | Disable state persistence |
| `--data-format` | `auto` | Force data format (json, csv, key-value) |
| `--on-incompatible` | `skip` | Behavior on incompatible schemas (skip, log, force, fail) |
| `--from-beginning` | off | Start from earliest offset for initial bootstrap |
| `--exclude-internal` | on | Exclude internal topics |

The live command:
- By default, processes only new messages arriving after the consumer starts (`latest`)
- Use `--from-beginning` to bootstrap schemas from existing topic data (only applies when no committed offsets exist for the consumer group — subsequent runs resume from committed offsets)
- Incrementally merges field statistics across batches (Counter-based)
- Detects structural schema changes: new fields, removed fields, type changes, nullability changes
- Re-evaluates discriminator detection on every batch cycle until one is found, using a buffer of recent records (up to 200) across batches
- Checks compatibility before registration (`--on-incompatible` controls behavior)
- Persists state to disk for resume-on-restart after Ctrl+C
- Auto-scales batch size and thread pool workers based on topic count
- Thread-safe parallel processing with `_metadata_lock` protecting shared topic metadata
- Supports multi-instance horizontal scaling via shared consumer groups

##### Choosing the Right Command

| | `infer` | `watch` | `live` |
|---|---------|---------|--------|
| **Purpose** | One-shot schema generation | Detect new topics | Track schema evolution |
| **Runs** | Once, then exits | Continuously (polls for new topics) | Continuously (consumes messages) |
| **Processes** | Existing messages | New topics only (each topic once) | New messages on existing topics |
| **Schema updates** | No | No | Yes -- detects field additions, type changes |
| **Offset tracking** | No (reads recent messages) | No | Yes (consumer group) |
| **Resume on restart** | N/A | No (in-memory set) | Yes (persisted state + committed offsets) |
| **Best for** | Initial schema bootstrap | Topic discovery automation | Production schema governance |

##### Incompatibility Strategies

When `live` mode detects a schema change that fails the Schema Registry compatibility check:

| Strategy | Behavior |
|----------|----------|
| `skip` (default) | Log warning, skip registration, continue consuming |
| `log` | Same as skip + write the incompatible schema to `{output_dir}/{topic}.incompatible.{ext}` |
| `force` | Temporarily set compatibility to NONE, register, restore original level |
| `fail` | Log error and exit |

##### Multi-Instance Scaling

For 1000+ topics, run multiple instances with the **same consumer group and state directory**:

```bash
# Instance 1
schema-infer --config config.yaml live \
  --topic-pattern ".*" --register \
  --consumer-group my-live-group \
  --state-dir /shared/nfs/schema-state

# Instance 2 (same machine or different host)
schema-infer --config config.yaml live \
  --topic-pattern ".*" --register \
  --consumer-group my-live-group \
  --state-dir /shared/nfs/schema-state
```

Kafka distributes partitions across instances. On rebalance (instance added/removed), state for affected topics is persisted to disk by the losing instance and loaded by the gaining instance. Batch size and worker threads auto-scale with topic count.

#### `version` - Version Information

Show version and build information.

```bash
schema-infer version
```

---

## Schema Formats

### JSON Schema

JSON Schema is a vocabulary that allows you to annotate and validate JSON documents.

**Features:**
- Industry-standard format
- Rich validation capabilities
- Excellent tooling support
- Human-readable structure

**Example Output:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "user-events",
  "description": "Auto-generated JSON Schema for user-events",
  "type": "object",
  "properties": {
    "userId": {
      "type": "string",
      "description": "User identifier",
      "examples": ["user123", "user456"]
    },
    "profile": {
      "type": "object",
      "properties": {
        "firstName": {
          "type": "string",
          "description": "User's first name"
        },
        "address": {
          "type": "object",
          "properties": {
            "street": {
              "type": "string"
            },
            "city": {
              "type": "string"
            }
          },
          "required": ["street", "city"]
        }
      },
      "required": ["firstName"]
    }
  },
  "required": ["userId", "profile"]
}
```

### Avro Schema

Apache Avro is a data serialization system with rich data structures.

**Features:**
- Compact binary format
- Schema evolution support
- Strong typing
- Confluent Platform integration

**Example Output:**

```json
{
  "type": "record",
  "name": "user_events",
  "namespace": "com.schema-infer.schema.infer",
  "doc": "Auto-generated Avro schema for user-events",
  "fields": [
    {
      "name": "userid",
      "type": "string",
      "doc": "User identifier"
    },
    {
      "name": "profile",
      "type": {
        "type": "record",
        "name": "profile_record",
        "fields": [
          {
            "name": "firstname",
            "type": "string",
            "doc": "User's first name"
          },
          {
            "name": "address",
            "type": {
              "type": "record",
              "name": "address_record",
              "fields": [
                {
                  "name": "street",
                  "type": "string"
                },
                {
                  "name": "city",
                  "type": "string"
                }
              ]
            }
          }
        ]
      }
    }
  ]
}
```

### Protobuf Schema

Protocol Buffers is a language-neutral, platform-neutral extensible mechanism for serializing structured data.

**Features:**
- Efficient binary format
- Cross-language support
- Backward compatibility
- High performance

**Example Output:**

```protobuf
syntax = "proto3";

package com_schema-infer_schema_infer;

message user_events {
  // Auto-generated schema for user-events
  string userid = 1; // User identifier
  profile_message profile = 2; // User profile
  
  message profile_message {
    string firstname = 1; // User's first name
    address_message address = 2; // User address
    
    message address_message {
      string street = 1;
      string city = 2;
    }
  }
}
```

---

## Data Format Support

### Supported Input Formats

#### JSON Format
- **Detection**: Automatic JSON parsing
- **Features**: Nested objects, arrays, all data types
- **Example**: `{"userId": "123", "profile": {"name": "John"}}`

#### CSV Format
- **Detection**: Comma-separated values with headers
- **Features**: Automatic header detection, multiple separators
- **Example**: `name,age,city\nJohn,30,New York`

#### Key-Value Format
- **Detection**: Space or equals-separated key-value pairs
- **Features**: Multiple separators, quoted values
- **Example**: `name=John age=30 city="New York"`

#### Raw Text Format
- **Detection**: Fallback for unrecognized formats
- **Features**: Plain text handling, special characters
- **Example**: `This is plain text content`

### Data Type Detection

The plugin automatically detects and handles:

- **Primitive Types**: string, number (all integers and floats unified), boolean, null
- **Complex Types**: objects, arrays, unions
- **Nested Structures**: Up to 20 levels deep
- **Array Types**: Arrays of primitives, objects, mixed types
- **Nullable Fields**: Optional vs required field detection

### Format Detection Algorithm

1. **JSON Detection**: Attempt JSON parsing
2. **CSV Detection**: Check for comma-separated structure
3. **Key-Value Detection**: Look for key-value patterns
4. **Raw Text Fallback**: Handle as plain text

---

## Performance & Optimization

### Performance Features

#### Parallel Processing
- **Multi-threaded**: Process multiple topics simultaneously
- **Configurable Workers**: Adjust thread count based on system resources
- **Load Balancing**: Distribute work across available threads

#### Optimized Message Reading
- **Smart Offset Selection**: Read from optimal positions
- **Batch Processing**: Process messages in batches
- **Connection Reuse**: Minimize connection overhead
- **Early Termination**: Stop when sufficient data is collected

#### Memory Management
- **Configurable Limits**: Set memory usage limits
- **Streaming Processing**: Process large datasets without memory issues
- **Garbage Collection**: Efficient memory cleanup

### Performance Configuration

```yaml
performance:
  max_workers: 4          # Number of parallel workers
  batch_size: 100         # Messages per batch
  memory_limit_mb: 512    # Memory limit in MB
  enable_caching: true    # Enable result caching
  cache_ttl: 3600         # Cache TTL in seconds
  show_progress: true     # Show progress bars
  verbose_logging: false  # Enable verbose logging
```

### Performance Tuning

#### For Large Topics
```yaml
inference:
  max_messages: 5000      # Increase message limit
  timeout: 60             # Increase timeout

performance:
  max_workers: 8          # Increase worker count
  batch_size: 200         # Increase batch size
```

#### For High-Throughput Scenarios
```yaml
performance:
  max_workers: 16         # Maximum workers
  batch_size: 500         # Large batches
  memory_limit_mb: 2048   # More memory
  enable_caching: true    # Enable caching
```

#### For Development/Testing
```yaml
inference:
  max_messages: 100       # Fewer messages
  timeout: 10             # Shorter timeout

performance:
  max_workers: 2          # Fewer workers
  show_progress: true     # Show progress
  verbose_logging: true   # Verbose logging
```

---

## Advanced Features

### Schema Registry Integration

#### Automatic Registration
```bash
# Register schema in Schema Registry
schema-infer infer --topic user-events --format avro --register
```

#### Compatibility Levels
```yaml
schema_registry:
  compatibility: "BACKWARD"  # BACKWARD, FORWARD, FULL, NONE
```

BACKWARD compatibility is the recommended default. Generated JSON Schemas use a **closed content model** (`additionalProperties: false`), which allows adding optional fields across schema versions under BACKWARD compatibility per Confluent's schema evolution rules.

Key compatibility behavior for JSON Schema on Confluent Cloud:
- **Closed content model** (`additionalProperties: false`): adding optional fields is BACKWARD compatible
- **Open content model** (`additionalProperties: true` or omitted): adding properties is NOT allowed under BACKWARD

The tool generates closed content model schemas by default, so schema evolution (adding fields discovered in subsequent inference runs) works correctly with BACKWARD compatibility.

#### Schema Merging

When registering schemas that already exist in Schema Registry, the tool deep-merges rather than replacing:

- **New fields** are added to the existing schema
- **Existing fields** are preserved (never removed or type-narrowed)
- **Type conflicts**: existing type is kept to avoid `COMBINED_TYPE_SUBSCHEMAS_CHANGED` errors
- **Nested objects**: recursively merged at each level of nesting
- **Array items**: recursively merged — `items.properties` are deep-merged so nested fields inside arrays are preserved
- **Unified numeric types**: all integers and floats are inferred as `number` to prevent compatibility errors when a field appears as `5` in one batch and `5.5` in another
- The existing schema is always used as the base, ensuring no existing types or structures are overwritten

#### Subject Name Strategies
```yaml
schema_registry:
  subject_name_strategy: "TopicNameStrategy"  # TopicName, RecordName, TopicRecordName
```

### Multi-Event Schema Detection

Topics that contain multiple event types (e.g., user events and payment events in the same topic) are automatically detected and handled. Instead of producing a single flat schema where every field is nullable, the tool generates separate schemas per event type with a main `oneOf` composition schema.

#### How Detection Works

1. The tool scans for candidate discriminator fields (top-level string fields with low cardinality)
2. Fields with well-known names (`event_type`, `type`, `action`, `kind`, etc.) are prioritized
3. A candidate is only accepted if grouping records by its values produces groups with **different field sets** — if all groups have identical fields, the candidate is rejected (it's just a value variation, not different event types)
4. In live mode, detection runs on **every batch cycle** until a discriminator is found, using a rolling buffer of recent records (up to 200) across batches — so topics with uniform early data can still be classified as multi-event once diverse events appear
5. If a valid discriminator is found, per-type schemas are generated; otherwise, a flat schema is produced
6. Schema references use the **actual version number** from Schema Registry, not a hardcoded value
7. If a flat schema was previously registered and a discriminator is later detected, the tool handles the transition by temporarily setting subject compatibility to NONE, registering the `oneOf` schema, then restoring the original compatibility

#### Auto-Detection (Default)

```bash
# Automatically detects event types and generates per-type schemas
schema-infer infer --topic events --output-dir ./schemas --register
```

#### Override Discriminator

```bash
# Specify which field identifies event types
schema-infer infer --topic events --discriminator event_type --output-dir ./schemas
```

#### Disable Multi-Event (Flat Schema)

```bash
# Force a single merged schema
schema-infer infer --topic events --flatten --output-dir ./schemas
```

#### Output Structure

For a topic `events` with `user_created` and `payment_processed` events:

**Files:**
- `events.json` - Main schema with `oneOf` referencing sub-schemas
- `events.user_created.json` - Standalone user event schema
- `events.payment_processed.json` - Standalone payment event schema

**Schema Registry subjects (with --register):**
- `events-user_created` - Sub-schema subject
- `events-payment_processed` - Sub-schema subject
- `events-value` - Main topic schema with `references` pointing to sub-schemas

#### Limitations

- Multi-event detection, schema references, and schema merging are **JSON Schema only** features
- Avro and Protobuf formats always produce flat schemas regardless of topic content
- Multi-event is supported in `infer` and `live` modes for JSON Schema; watch mode uses flat schemas
- Using `--discriminator` with `--format avro` or `--format protobuf` will show a warning and fall back to flat schema

### Topic Filtering

#### Internal Topic Exclusion
```yaml
topic_filter:
  internal_prefix: "_"
  exclude_internal: true
  additional_exclude_prefixes: ["__", "temp-", "backup-"]
```

#### Pattern-Based Filtering
```yaml
topic_filter:
  include_patterns: [".*-events", "prod-.*"]
```

### Custom Configuration

#### Environment-Specific Settings
```yaml
# Development
kafka:
  bootstrap_servers: "localhost:9092"
  security_protocol: "PLAINTEXT"

# Production
kafka:
  bootstrap_servers: "kafka-cluster:9092"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  sasl_username: "kafka"
  sasl_password: "secure-password"
```

#### Format-Specific Settings
```yaml
inference:
  forced_data_format: "json"  # Force specific format
  max_depth: 3                # Limit nesting depth
  confidence_threshold: 0.9   # Higher confidence threshold
```

---

## Using with Terraform

The Schema Inference Plugin integrates with the [Confluent Terraform Provider](https://registry.terraform.io/providers/confluentinc/confluent/latest) via a reusable Terraform module. This enables Infrastructure-as-Code workflows where schemas are inferred from Confluent Cloud Kafka topics during `terraform plan` and registered to Confluent Cloud Schema Registry through `confluent_schema` resources.

> **Note**: Terraform integration is supported with **Confluent Cloud** only.

### Overview

The Terraform module uses Terraform's built-in `external` data source to call the `schema-infer` CLI. The inferred schema string feeds directly into `confluent_schema` resources managed by the official Confluent provider. The module auto-installs `schema-infer` if it is not already present.

```
terraform plan
    |
    +-> external data source calls schema-infer CLI
    |   (connects to Kafka, samples messages, infers schema)
    |
    +-> confluent_schema compares inferred schema against state
    |   (shows diff if schema evolved)
    |
    +-> terraform apply registers via Confluent provider
```

### Prerequisites

- Terraform >= 1.0
- Python 3.9+ (the module auto-installs `schema-infer` on first run)
- A Confluent Cloud Kafka cluster with data in topics
- Confluent Terraform Provider configured for Confluent Cloud Schema Registry access

### Installation

Reference the module from your Terraform configuration. No separate install step is required -- `schema-infer` is installed automatically on the first `terraform plan`.

```hcl
terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"
  # ... configuration below
}
```

### Configuration Option A: Inline Variables (Recommended)

Pass Confluent Cloud credentials directly as Terraform variables. No YAML config file needed. Credentials can come from `terraform.tfvars`, environment variables, Terraform Cloud variable sets, or HashiCorp Vault.

```hcl
module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"

  bootstrap_servers = "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret

  topics       = ["orders", "payments", "users"]
  format       = "avro"
  max_messages = 100
}
```

### Configuration Option B: YAML Config File

If you already have a configuration file for the CLI, reference it directly:

```hcl
module "inferred_schemas" {
  source      = "github.com/akrishnanDG/terraform-schema-infer"
  config_file = "${path.module}/cc-config.yaml"
  topics      = ["orders", "payments", "users"]
  format      = "avro"
}
```

### Registering Schemas with Confluent Provider

Wire the module output into `confluent_schema` resources:

```hcl
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

### Module Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `topics` | Yes | - | List of Confluent Cloud Kafka topic names to infer schemas from |
| `format` | No | `avro` | Output format: `avro`, `protobuf`, or `json-schema` |
| `max_messages` | No | `100` | Maximum messages to sample per topic |
| `config_file` | No | `""` | Path to YAML config file (Option B) |
| `bootstrap_servers` | No | `""` | Confluent Cloud bootstrap servers (Option A) |
| `kafka_api_key` | No | `""` | Confluent Cloud Kafka API key (Option A, sensitive) |
| `kafka_api_secret` | No | `""` | Confluent Cloud Kafka API secret (Option A, sensitive) |

### Module Outputs

| Output | Type | Description |
|--------|------|-------------|
| `schemas` | `map(string)` | Map of topic name to inferred schema string |

### Complete Example

```hcl
terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

locals {
  topics = ["orders", "payments", "users", "inventory", "shipments"]
}

module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"

  bootstrap_servers = var.bootstrap_servers
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret

  topics       = local.topics
  format       = "avro"
  max_messages = 100
}

resource "confluent_schema" "inferred" {
  for_each = module.inferred_schemas.schemas

  subject_name = "${each.key}-value"
  format       = "AVRO"
  schema       = each.value

  schema_registry_cluster {
    id = var.sr_cluster_id
  }

  rest_endpoint = var.sr_rest_endpoint

  credentials {
    key    = var.sr_api_key
    secret = var.sr_api_secret
  }
}

output "registered_subjects" {
  value = [for k, v in confluent_schema.inferred : v.subject_name]
}
```

### Terraform Lifecycle Behavior

| Action | What happens |
|--------|-------------|
| `terraform plan` | Connects to Kafka, samples messages from each topic, infers schemas, shows diff against state |
| `terraform apply` | Registers inferred schemas to Schema Registry via the Confluent provider |
| `terraform plan` (again) | Re-infers schemas; if topic data hasn't changed, shows no diff |
| `terraform destroy` | Deletes `confluent_schema` resources from Schema Registry |

### Ongoing Usage

- **New topic**: Add it to the `topics` list, run `terraform plan/apply`
- **Schema evolved**: Run `terraform plan` -- it re-infers and shows the diff. Apply to register the new version.
- **Remove a topic**: Remove it from the list, run `terraform apply` to destroy the schema resource

### Alternative: Generate Schemas Separately, Register with Terraform

Instead of inferring schemas inline during `terraform plan`, you can generate schema files separately using the CLI and have Terraform read them from disk. This decouples schema generation from Terraform runs, allows human review, and works with `watch` and `live` modes.

#### One-Shot Generation

```bash
# Generate schema files
schema-infer --config cc-config.yaml infer \
  --topic-pattern ".*" --format avro --output-dir ./schemas/ --exclude-internal
```

#### Continuous Detection with Watch Mode

Run as a service to detect new topics and generate schema files automatically:

```bash
schema-infer --config cc-config.yaml watch \
  --topic-pattern ".*" --format avro --output-dir ./schemas/ --interval 60
```

#### Schema Evolution with Live Mode

Run as a service to continuously consume messages, detect schema evolution, and update schema files:

```bash
schema-infer --config cc-config.yaml live \
  --topic-pattern ".*" --format avro --output-dir ./schemas/
```

#### Terraform Reads Schema Files from Disk

```hcl
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

No Kafka connection is needed during `terraform plan` -- Terraform just reads the files. New or updated schema files trigger changes on the next `terraform apply`. This works well with CI/CD pipelines that trigger on git commits to the `schemas/` directory.

For the full guide including CI/CD integration examples, see [TERRAFORM.md](TERRAFORM.md).

### Limitations

- The inline module approach runs on every `terraform plan`, connecting to Kafka each time
- Requires Python 3.9+ on the machine running Terraform
- Credentials passed via inline variables are written to a temporary file during inference (deleted immediately after)
- Errors from the CLI surface as generic `external` data source errors in Terraform output

---

## Using with Confluent Tableflow

Confluent [Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html) materializes Kafka topics as Apache Iceberg or Delta Lake tables. Tableflow uses Schema Registry as the source of truth for table structure -- **if a topic doesn't have a schema registered, Tableflow can't materialize it**.

Schema inference solves this by reading messages from schemaless topics, inferring the structure, and registering schemas so Tableflow can create tables.

> **Note**: Tableflow is a Confluent Cloud feature. This integration is supported with **Confluent Cloud** only.

### Quick Start (CLI)

```bash
# Infer schema and register to Schema Registry
schema-infer --config cc-config.yaml infer \
  --topic orders --format avro --register

# Then enable Tableflow via Confluent CLI
confluent tableflow topic create orders \
  --cluster lkc-xxxxx \
  --environment env-xxxxx \
  --table-formats ICEBERG
```

### Fully Automated with Terraform

Combine schema inference, schema registration, and Tableflow enablement in a single Terraform config:

```hcl
locals {
  topics = ["orders", "payments", "users"]
}

# Step 1: Infer schemas
module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"

  bootstrap_servers = var.bootstrap_servers
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret

  topics       = local.topics
  format       = "avro"
  max_messages = 100
}

# Step 2: Register schemas
resource "confluent_schema" "inferred" {
  for_each     = module.inferred_schemas.schemas
  subject_name = "${each.key}-value"
  format       = "AVRO"
  schema       = each.value
  # ... cluster config, credentials
}

# Step 3: Enable Tableflow
resource "confluent_tableflow_topic" "materialized" {
  for_each = module.inferred_schemas.schemas

  environment {
    id = var.environment_id
  }
  kafka_cluster {
    id = var.kafka_cluster_id
  }

  display_name  = each.key
  table_formats = ["ICEBERG"]
  managed_storage {}

  credentials {
    key    = var.tableflow_api_key
    secret = var.tableflow_api_secret
  }

  # Schema must be registered before Tableflow can materialize
  depends_on = [confluent_schema.inferred]

  lifecycle {
    prevent_destroy = true
  }
}
```

The `depends_on` is critical -- Tableflow will fail if the schema isn't registered first.

### Schema Evolution with Live Mode

Use `live` mode to continuously track schema changes. When new fields appear, the schema file is updated, Terraform registers the new version, and Tableflow automatically reflects the updated table structure.

```bash
# Live mode detects schema evolution and updates files
schema-infer --config cc-config.yaml live \
  --topics "orders,payments,users" \
  --format avro --output-dir ./schemas/
```

A CI/CD pipeline triggers `terraform apply` when schema files change, updating Schema Registry. Tableflow picks up the new table structure automatically.

### Tableflow Configuration Options

| Option | Description |
|--------|-------------|
| `table_formats` | `["ICEBERG"]`, `["DELTA"]`, or `["ICEBERG", "DELTA"]` |
| `managed_storage {}` | Confluent-managed storage |
| `byob_aws { bucket_name, provider_integration_id }` | Bring your own S3 bucket |
| `error_handling { mode }` | `SUSPEND`, `SKIP`, or `LOG` on bad records |

For the full Tableflow integration guide, see [TABLEFLOW.md](TABLEFLOW.md).

---

## Troubleshooting

### Common Issues

#### Connection Issues

**Problem**: Cannot connect to Kafka cluster
```
Error: Failed to connect to Kafka cluster
```

**Solutions**:
1. Check bootstrap servers configuration
2. Verify network connectivity
3. Check authentication credentials
4. Validate security protocol settings

```yaml
kafka:
  bootstrap_servers: "correct-server:9092"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  sasl_username: "correct-username"
  sasl_password: "correct-password"
```

#### Schema Registry Issues

**Problem**: Cannot connect to Schema Registry
```
Error: Schema Registry connection failed
```

**Solutions**:
1. Check Schema Registry URL
2. Verify authentication credentials
3. Check SSL certificate settings
4. Validate network connectivity

```yaml
schema_registry:
  url: "https://correct-registry-url"
  cloud_api_key: "correct-api-key"
  cloud_api_secret: "correct-api-secret"
```

#### Schema Registry Error Codes

When schema registration or retrieval fails, the Schema Registry returns specific error codes. These codes appear in the CLI output as `Schema Registry error <code>: <message>`.

| Error Code | HTTP Status | Meaning | Common Cause |
|------------|-------------|---------|--------------|
| `40401` | 404 | Subject not found | The subject does not exist in the registry. This is normal on first registration and handled automatically. |
| `40402` | 404 | Schema version not found | Requested a specific version that does not exist for the subject. |
| `40403` | 404 | Schema not found | Requested a schema by ID that does not exist in the registry. |
| `42201` | 422 | Invalid schema | The generated schema is syntactically invalid. Check your data for malformed or mixed-format messages. |
| `42202` | 422 | Invalid version | An invalid version number was specified in the request. |
| `409` | 409 | Incompatible schema | The new schema is not compatible with the previous version under the subject's compatibility level. See below. |
| `401` | 401 | Authentication failed | API key or secret is incorrect, expired, or missing. |
| `403` | 403 | Authorization failed | The API key does not have permission for this operation. Check your ACLs or RBAC role assignments in Confluent Cloud. |

**Compatibility violations (error 409)**

This is the most common registration failure. It means the new schema breaks the subject's compatibility rules. Common causes:

- **Field removed** under `BACKWARD` compatibility — consumers using the new schema can't read old messages missing the field
- **Field type changed** (e.g., `string` to `integer`) — not compatible under any mode except `NONE`
- **`additionalProperties` changed** from `true` to `false` — rejects messages with extra fields
- **Structural change** (e.g., flat `object` to `oneOf`) — the schema shape changed fundamentally

To resolve compatibility errors:
```bash
# Check the current compatibility level for a subject
curl -u <api-key>:<api-secret> \
  https://<sr-url>/config/<subject-name>
```

> **Note**: When the tool detects a transition from a single-event (flat) schema to a multi-event (oneOf) schema, it automatically handles the compatibility override — temporarily setting the subject to `NONE` for the registration, then restoring the original level. No manual intervention is needed for this case.

#### Required Permissions

The tool requires specific permissions for both Kafka cluster access and Schema Registry access. Below are the minimum permissions needed for each operation.

**Kafka Cluster (Data)**

| Operation | Resource Type | Resource | Permission | Used By |
|-----------|--------------|----------|------------|---------|
| `DESCRIBE` | Cluster | `kafka-cluster` | Cluster-level metadata access | All commands |
| `DESCRIBE` | Topic | `*` or specific topics | List and discover topics | `list-topics`, `watch`, `infer` with `--topic-prefix`/`--topic-pattern` |
| `READ` | Topic | Target topic(s) | Consume messages from topics | `infer`, `live` |
| `READ` | Consumer Group | `schema-infer-*` | Consumer group membership | `infer`, `live` |

**Schema Registry**

| Operation | API Endpoint | Permission | Used By |
|-----------|-------------|------------|---------|
| Read schemas | `GET /subjects`, `GET /subjects/{subject}/versions` | Read | Schema merging (reads existing schemas before registering) |
| Register schemas | `POST /subjects/{subject}/versions` | Write | `infer --register`, `live` |
| Read config | `GET /config/{subject}` | Read | Flat-to-multi-event transition (reads current compatibility level) |
| Write config | `PUT /config/{subject}` | Owner-level | Flat-to-multi-event transition (temporarily overrides compatibility) |

> **Note**: If you only need to infer schemas without registering (no `--register` flag), read access to the Kafka cluster is sufficient and no Schema Registry permissions are needed. Owner-level access on Schema Registry is only required when the tool transitions a subject from flat to multi-event schema, which requires temporarily changing the subject's compatibility level.

##### ACL-Based Access (Confluent Cloud & Platform)

Create a service account with an API key scoped to the Kafka cluster, then assign the following ACLs:

```bash
# Kafka cluster — minimum ACLs for schema inference (read-only)
confluent kafka acl create --allow --service-account <sa-id> \
  --operations DESCRIBE --cluster-scope

confluent kafka acl create --allow --service-account <sa-id> \
  --operations DESCRIBE,READ --topic '*' --prefix

confluent kafka acl create --allow --service-account <sa-id> \
  --operations READ --consumer-group 'schema-infer-' --prefix
```

##### RBAC Roles

RBAC provides a more granular and manageable alternative to ACLs. The table below maps each use case to the minimum RBAC role required.

**Kafka Cluster Roles**

| Use Case | Minimum Role | Scope | Description |
|----------|-------------|-------|-------------|
| Infer only (no registration) | `DeveloperRead` | Topic-level or cluster-level | Read messages and list topics |
| Infer + register | `DeveloperRead` | Topic-level or cluster-level | Same as above; registration is a Schema Registry operation |
| Live mode | `DeveloperRead` | Topic-level or cluster-level | Continuous consumption requires the same read permissions |
| Topic discovery (`list-topics`, `watch`) | `DeveloperRead` | Cluster-level | Needs `DESCRIBE` on all topics to discover them |

**Schema Registry Roles**

| Use Case | Minimum Role | Scope | Description |
|----------|-------------|-------|-------------|
| Read existing schemas (for merging) | `DeveloperRead` | Subject-level or global | Reads latest schema versions before merging |
| Register schemas | `DeveloperWrite` | Subject-level or global | Registers new schema versions |
| Flat-to-multi-event transition | `ResourceOwner` | Subject-level | Temporarily overrides subject compatibility from `BACKWARD` to `NONE` during schema structure transition, then restores it |

**Confluent Cloud — RBAC setup commands**

```bash
# 1. Create a service account
confluent iam service-account create schema-infer-sa \
  --description "Service account for schema inference tool"

# 2. Kafka cluster — DeveloperRead on all topics (or scope to specific topics)
confluent iam rbac role-binding create \
  --principal User:<sa-id> \
  --role DeveloperRead \
  --resource Topic:* \
  --kafka-cluster <kafka-cluster-id> \
  --environment <env-id>

# 3. Kafka cluster — DeveloperRead on consumer group prefix
confluent iam rbac role-binding create \
  --principal User:<sa-id> \
  --role DeveloperRead \
  --resource Group:schema-infer- \
  --prefix \
  --kafka-cluster <kafka-cluster-id> \
  --environment <env-id>

# 4. Schema Registry — DeveloperWrite for registration
confluent iam rbac role-binding create \
  --principal User:<sa-id> \
  --role DeveloperWrite \
  --resource Subject:* \
  --schema-registry-cluster <sr-cluster-id> \
  --environment <env-id>

# 5. Schema Registry — ResourceOwner (only if flat-to-multi-event transitions are expected)
confluent iam rbac role-binding create \
  --principal User:<sa-id> \
  --role ResourceOwner \
  --resource Subject:* \
  --schema-registry-cluster <sr-cluster-id> \
  --environment <env-id>
```

**Confluent Platform — RBAC setup**

```bash
# Kafka cluster — DeveloperRead
confluent iam rbac role-binding create \
  --principal User:<username> \
  --role DeveloperRead \
  --resource Topic:* \
  --kafka-cluster-id <kafka-cluster-id>

# Schema Registry — DeveloperWrite
confluent iam rbac role-binding create \
  --principal User:<username> \
  --role DeveloperWrite \
  --resource Subject:* \
  --schema-registry-cluster-id <sr-cluster-id>

# Schema Registry — ResourceOwner (only for flat-to-multi-event transitions)
confluent iam rbac role-binding create \
  --principal User:<username> \
  --role ResourceOwner \
  --resource Subject:* \
  --schema-registry-cluster-id <sr-cluster-id>
```

**Scoping permissions to specific topics**

For least-privilege access, scope Kafka and Schema Registry roles to specific topics or subjects instead of using wildcards:

```bash
# Kafka — read access to a specific topic
confluent iam rbac role-binding create \
  --principal User:<sa-id> \
  --role DeveloperRead \
  --resource Topic:my-topic \
  --kafka-cluster <kafka-cluster-id> \
  --environment <env-id>

# Kafka — read access to topics with a shared prefix
confluent iam rbac role-binding create \
  --principal User:<sa-id> \
  --role DeveloperRead \
  --resource Topic:prod- \
  --prefix \
  --kafka-cluster <kafka-cluster-id> \
  --environment <env-id>

# Schema Registry — write access to matching subjects only
confluent iam rbac role-binding create \
  --principal User:<sa-id> \
  --role DeveloperWrite \
  --resource Subject:my-topic-value \
  --schema-registry-cluster <sr-cluster-id> \
  --environment <env-id>
```

#### Performance Issues

**Problem**: Slow processing or timeouts
```
Warning: Processing timeout exceeded
```

**Solutions**:
1. Increase timeout settings
2. Reduce message count
3. Increase worker count
4. Optimize batch size

```yaml
inference:
  max_messages: 50
  timeout: 60

performance:
  max_workers: 8
  batch_size: 200
```

#### Memory Issues

**Problem**: Out of memory errors
```
Error: Memory limit exceeded
```

**Solutions**:
1. Increase memory limit
2. Reduce batch size
3. Process fewer topics simultaneously
4. Enable caching

```yaml
performance:
  memory_limit_mb: 2048
  batch_size: 100
  max_workers: 4
  enable_caching: true
```

### Debug Mode

Enable verbose logging for detailed debugging:

```yaml
performance:
  verbose_logging: true
```

Or use command-line option:
```bash
schema-infer --config config.yaml infer --topic my-topic --verbose
```

### Log Analysis

Check logs for specific error patterns:

```bash
# Check for connection errors
grep -i "connection" logs/schema-infer.log

# Check for authentication errors
grep -i "auth" logs/schema-infer.log

# Check for timeout errors
grep -i "timeout" logs/schema-infer.log
```

---

## API Reference

### Configuration Classes

#### KafkaConfig
```python
class KafkaConfig(BaseModel):
    bootstrap_servers: str
    auto_offset_reset: str = "latest"
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    security_protocol: str = "PLAINTEXT"
    cloud_api_key: Optional[str] = None
    cloud_api_secret: Optional[str] = None
```

#### SchemaRegistryConfig
```python
class SchemaRegistryConfig(BaseModel):
    url: str
    auth: Optional[Dict[str, str]] = None
    compatibility: str = "BACKWARD"
    subject_name_strategy: str = "TopicNameStrategy"
    cloud_api_key: Optional[str] = None
    cloud_api_secret: Optional[str] = None
```

#### InferenceConfig
```python
class InferenceConfig(BaseModel):
    max_messages: int = 50
    timeout: int = 30
    max_depth: int = 20
    confidence_threshold: float = 0.8
    auto_detect_format: bool = True
    forced_data_format: Optional[str] = None
```

### Core Classes

#### SchemaInferrer
```python
class SchemaInferrer:
    def __init__(self, max_depth: int = 20, confidence_threshold: float = 0.8):
        pass
    
    def infer_schema(self, data: List[Dict], name: str) -> InferredSchema:
        pass
    
    def process_topics_parallel(self, topic_messages: Dict, output_format: str) -> Dict:
        pass
```

#### TopicDiscovery
```python
class TopicDiscovery:
    def __init__(self, config: Config):
        pass
    
    def discover_topics(self, topic_name: str = None, topics: List[str] = None) -> List[str]:
        pass
```

### Schema Generators

#### JSONSchemaGenerator
```python
class JSONSchemaGenerator(BaseSchemaGenerator):
    def generate(self, schema: InferredSchema) -> str:
        pass
    
    def get_file_extension(self) -> str:
        return "json"
```

#### AvroGenerator
```python
class AvroGenerator(BaseSchemaGenerator):
    def generate(self, schema: InferredSchema) -> str:
        pass
    
    def get_file_extension(self) -> str:
        return "avsc"
```

#### ProtobufGenerator
```python
class ProtobufGenerator(BaseSchemaGenerator):
    def generate(self, schema: InferredSchema) -> str:
        pass
    
    def get_file_extension(self) -> str:
        return "proto"
```

---

## Examples

### Example 1: E-commerce Platform

**Scenario**: Generate schemas for an e-commerce platform with user events, orders, and payments.

```bash
# Generate schemas for all e-commerce topics
schema-infer infer --topic-prefix "ecommerce-" --output-dir schemas/ --format avro

# Register critical schemas in Schema Registry
schema-infer infer --topics "user-events,order-events,payment-events" --format avro --register
```

**Configuration**:
```yaml
kafka:
  bootstrap_servers: "ecommerce-kafka:9092"
  security_protocol: "SASL_SSL"

schema_registry:
  url: "https://ecommerce-registry:8081"
  compatibility: "BACKWARD"

topic_filter:
  include_patterns: ["ecommerce-.*"]
  exclude_internal: true
```

### Example 2: IoT Data Processing

**Scenario**: Process IoT sensor data with high throughput and complex nested structures.

```bash
# Process IoT topics with high message count
schema-infer infer --topic-prefix "iot-" --max-messages 5000 --format json-schema --output-dir iot-schemas/
```

**Configuration**:
```yaml
inference:
  max_messages: 500
  timeout: 120
  max_depth: 6

performance:
  max_workers: 16
  batch_size: 500
  memory_limit_mb: 4096
  enable_caching: true
```

### Example 3: Microservices Architecture

**Scenario**: Generate schemas for microservices communication with different data formats.

```bash
# Process different service topics
schema-infer infer --topics "user-service,order-service,payment-service" --format protobuf --output-dir microservices/

# Generate JSON schemas for API documentation
schema-infer infer --topic-pattern ".*-api" --format json-schema --output-dir api-docs/
```

**Configuration**:
```yaml
topic_filter:
  include_patterns: [".*-service", ".*-api"]
  additional_exclude_prefixes: ["internal-", "temp-"]

inference:
  auto_detect_format: true
  confidence_threshold: 0.9
```

### Example 4: Data Migration

**Scenario**: Migrate from untyped to typed data system with schema validation.

```bash
# Generate schemas for migration
schema-infer infer --topic-prefix "legacy-" --format avro --register

# Validate schema compatibility
schema-infer validate-topics --topics "legacy-user-events,legacy-order-events"
```

**Configuration**:
```yaml
schema_registry:
  compatibility: "FULL"
  subject_name_strategy: "TopicNameStrategy"

inference:
  max_messages: 5000
  confidence_threshold: 0.95
```

---

## Best Practices

### Schema Design

#### 1. **Consistent Naming**
- Use consistent field naming conventions
- Follow camelCase or snake_case consistently
- Use descriptive field names

#### 2. **Proper Data Types**
- Use appropriate data types for fields
- Avoid overly broad types (e.g., string for numbers)
- Use nullable fields appropriately

#### 3. **Schema Evolution**
- Design schemas for backward compatibility
- Use optional fields for new additions
- Plan for schema versioning

### Performance Optimization

#### 1. **Batch Processing**
- Process multiple topics in batches
- Use appropriate batch sizes
- Monitor memory usage

#### 2. **Resource Management**
- Configure appropriate worker counts
- Set memory limits
- Enable caching for repeated operations

#### 3. **Network Optimization**
- Use connection pooling
- Minimize network round trips
- Optimize message reading strategies

### Security

#### 1. **Authentication**
- Use secure authentication mechanisms
- Rotate credentials regularly
- Store API keys and secrets securely in YAML configuration files

#### 2. **Network Security**
- Use SSL/TLS for all connections
- Validate certificates
- Use secure network configurations

#### 3. **Access Control**
- Implement proper access controls
- Use least privilege principle
- Monitor access patterns

### Monitoring and Logging

#### 1. **Comprehensive Logging**
- Enable verbose logging for debugging
- Log performance metrics
- Monitor error rates

#### 2. **Progress Tracking**
- Use progress bars for long operations
- Provide ETA estimates
- Show detailed status information

#### 3. **Error Handling**
- Implement graceful error handling
- Provide meaningful error messages
- Log detailed error information

---

## Limitations

### Current Limitations

#### 1. **Data Format Support**
- Limited to text-based formats
- No support for binary formats (Avro, Protobuf input)
- CSV parsing has limitations with complex structures

#### 2. **Schema Complexity**
- Maximum nesting depth of 20 levels (configurable via `max_depth`)
- Limited support for recursive structures
- No support for circular references

#### 3. **Performance**
- Memory usage scales with topic size
- Processing time increases with message count
- Limited parallel processing for single large topics

#### 4. **Platform Support**
- Requires Python 3.9+
- Limited Windows support
- No native mobile support

### Known Issues

#### 1. **Large Topics**
- May timeout on very large topics
- Memory usage can be high
- Processing time can be long

#### 2. **Complex Nested Data**
- Deep nesting may not be fully captured
- Array of arrays has limited support
- Mixed type arrays may not be optimal

#### 3. **Schema Registry**
- Schema references supported for JSON Schema multi-event topics only
- Limited subject name strategy options

### Workarounds

#### 1. **Large Topics**
```yaml
inference:
  max_messages: 50  # Reduce message count
  timeout: 60         # Increase timeout

performance:
  max_workers: 1      # Reduce parallelism
  batch_size: 50      # Smaller batches
```

#### 2. **Complex Data**
```yaml
inference:
  max_depth: 3        # Limit nesting depth
  confidence_threshold: 0.7  # Lower threshold
```

#### 3. **Memory Issues**
```yaml
performance:
  memory_limit_mb: 1024  # Increase memory limit
  enable_caching: false  # Disable caching
```

---

## Support

### Getting Help

#### 1. **Documentation**
- Check this documentation first
- Review examples and best practices
- Look for troubleshooting guides

#### 2. **Community Support**
- GitHub Issues: Report bugs and request features
- GitHub Discussions: Ask questions and share experiences
- Stack Overflow: Tag questions with `schema-infer`

#### 3. **Professional Support**
- Schema Inference Support: For enterprise customers
- Professional Services: For implementation help
- Training: For team training and certification

### Reporting Issues

#### 1. **Bug Reports**
Include the following information:
- Plugin version
- Configuration file (sanitized)
- Error messages and logs
- Steps to reproduce
- Expected vs actual behavior

#### 2. **Feature Requests**
Include the following information:
- Use case description
- Expected functionality
- Workarounds currently used
- Priority and impact

#### 3. **Performance Issues**
Include the following information:
- System specifications
- Topic sizes and message counts
- Configuration settings
- Performance metrics
- Expected performance

### Contributing

#### 1. **Development Setup**
```bash
git clone https://github.com/akrishnanDG/schema-infer-plugin.git
cd schema-infer-plugin
pip install -e ".[dev]"
```

#### 2. **Running Tests**
```bash
python run_tests.py --coverage
```

#### 3. **Code Style**
- Follow PEP 8 guidelines
- Use type hints
- Add comprehensive tests
- Update documentation

### Version History

#### v1.0.0 (Current)
- Initial release
- Multi-format schema generation
- Comprehensive data type support
- Performance optimization
- Enterprise security features

#### Roadmap
- Binary format support
- Enhanced schema evolution
- Advanced performance features
- Extended platform support

---

## Conclusion

The Schema Inference Plugin provides a production-ready solution for automatic schema generation from Kafka topics. It supports multiple schema formats (JSON Schema, Avro, Protobuf), comprehensive data type detection, and integration with Confluent Cloud and Confluent Platform.

Common use cases include migrating from untyped to typed systems, establishing data governance, and accelerating development workflows.

For more information, examples, and support, visit the [GitHub repository](https://github.com/akrishnanDG/schema-infer-plugin).
