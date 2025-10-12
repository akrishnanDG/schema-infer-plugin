# Schema Inference Schema Inference Plugin - Product Documentation

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
11. [Troubleshooting](#troubleshooting)
12. [API Reference](#api-reference)
13. [Examples](#examples)
14. [Best Practices](#best-practices)
15. [Limitations](#limitations)
16. [Support](#support)

---

## Overview

The **Schema Inference Schema Inference Plugin** is a powerful CLI tool that automatically infers and generates schemas from Kafka topic data. It supports multiple schema formats (JSON Schema, Avro, Protobuf) and can handle complex nested data structures with comprehensive data type detection.

### Key Benefits

- **🚀 Automatic Schema Discovery**: No manual schema writing required
- **📊 Multi-Format Support**: Generate JSON Schema, Avro, and Protobuf schemas
- **🔍 Deep Nested Analysis**: Handles complex nested objects and arrays
- **⚡ High Performance**: Optimized for large topics and high-throughput scenarios
- **☁️ Cloud & Platform Support**: Works with Schema Inference Cloud and Schema Inference Platform
- **🛡️ Production Ready**: Robust error handling and comprehensive logging

### Use Cases

- **Schema Migration**: Migrate from untyped to typed data systems
- **API Documentation**: Generate schemas for API documentation
- **Data Governance**: Establish data contracts and validation rules
- **Development Acceleration**: Quickly bootstrap schema definitions
- **Compliance**: Meet data governance and compliance requirements

---

## Features

### Core Features

#### 🎯 **Multi-Format Schema Generation**
- **JSON Schema**: Industry-standard JSON Schema (Draft 7)
- **Avro**: Apache Avro with nested record structures
- **Protobuf**: Protocol Buffers with nested message definitions

#### 🔍 **Intelligent Data Analysis**
- **Automatic Format Detection**: JSON, CSV, key-value, raw text
- **Deep Nested Analysis**: Up to 5 levels of nesting
- **Comprehensive Type Detection**: string, int, float, boolean, null, arrays, objects
- **Array Handling**: Arrays of primitives, objects, and mixed types

#### ⚡ **High Performance**
- **Parallel Processing**: Multi-threaded topic processing
- **Optimized Message Reading**: Smart offset selection and batch processing
- **Connection Reuse**: Efficient Kafka consumer management
- **Progress Tracking**: Real-time progress bars and ETA

#### 🎛️ **Flexible Topic Discovery**
- **Single Topic**: Process individual topics
- **Multiple Topics**: Process comma-separated topic lists
- **Prefix Matching**: Process all topics with specific prefixes
- **Pattern Matching**: Regex-based topic selection
- **Smart Filtering**: Exclude internal topics and system topics

#### 🔐 **Enterprise Security**
- **Schema Inference Cloud**: Full API key/secret authentication
- **Schema Inference Platform**: SASL/SSL authentication support
- **Schema Registry**: Secure schema registration and management
- **Configurable Security**: Flexible authentication mechanisms

### Advanced Features

#### 📊 **Schema Registry Integration**
- **Automatic Registration**: Register schemas in Schema Registry
- **Compatibility Levels**: Configurable compatibility settings
- **Subject Strategies**: TopicName, RecordName, TopicRecordName strategies
- **Version Management**: Automatic versioning and evolution

#### 🎨 **Customization Options**
- **Configurable Depth**: Adjustable nesting depth limits
- **Format Preferences**: Force specific data format detection
- **Output Options**: File output, directory output, or registry registration
- **Verbose Logging**: Detailed debugging and monitoring

#### 🛠️ **Developer Experience**
- **Progress Indicators**: Visual progress bars and status updates
- **Comprehensive Logging**: Detailed operation logs
- **Error Handling**: Graceful error recovery and reporting
- **Configuration Management**: YAML-based configuration system

---

## Installation

### Prerequisites

- **Python 3.8+**: Required for running the plugin
- **Schema Inference CLI**: Required for plugin integration
- **Kafka Access**: Access to Kafka cluster (Schema Inference Cloud or Platform)
- **Schema Registry Access**: Optional, for schema registration

### Quick Installation

```bash
# Clone the repository
git clone https://github.com/schema-inferinc/schema-infer-plugin.git
cd schema-infer-plugin

# Install the plugin
python3 install-plugin.py

# Verify installation
schema-infer --help
```

### Manual Installation

```bash
# Install dependencies
pip3 install -r requirements.txt

# Copy plugin to Schema Inference CLI directory
cp schema-infer-schema /usr/local/bin/
chmod +x /usr/local/bin/schema-infer-schema

# Verify installation
schema-infer --help
```

### Docker Installation

```bash
# Build Docker image
docker build -t schema-infer-schema-infer .

# Run with Docker
docker run -v $(pwd)/config:/app/config schema-infer-schema-infer \
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
  
  # Schema Inference Cloud Authentication
  cloud_api_key: "your-api-key"
  cloud_api_secret: "your-api-secret"

# Schema Registry Configuration
schema_registry:
  url: "http://localhost:8081"
  verify_ssl: true
  auth: null
  compatibility: "NONE"
  subject_name_strategy: "TopicNameStrategy"
  
  # Schema Inference Cloud Authentication
  cloud_api_key: "your-api-key"
  cloud_api_secret: "your-api-secret"

# Schema Inference Configuration
inference:
  max_messages: 50
  timeout: 30
  max_depth: 5
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
```

### Configuration Notes

**Important**: API keys and secrets are read directly from the YAML configuration file, not from environment variables. This provides better security and easier configuration management.

For advanced use cases, you can still use environment variables for other settings, but the plugin's authentication is handled through the YAML configuration file.

### Configuration Examples

#### Schema Inference Cloud Configuration

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
  verify_ssl: true
```

#### Schema Inference Platform Configuration

```yaml
kafka:
  bootstrap_servers: "kafka-0.kafka.default.svc.cluster.local:9071"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  sasl_username: "kafka"
  sasl_password: "kafka-secret"

schema_registry:
  url: "https://schemaregistry:8081"
  verify_ssl: false
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
- Schema Inference Platform integration

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

- **Primitive Types**: string, int, float, boolean, null
- **Complex Types**: objects, arrays, unions
- **Nested Structures**: Up to 5 levels deep
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

#### Subject Name Strategies
```yaml
schema_registry:
  subject_name_strategy: "TopicNameStrategy"  # TopicName, RecordName, TopicRecordName
```

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
  verify_ssl: true
  cloud_api_key: "correct-api-key"
  cloud_api_secret: "correct-api-secret"
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
    verify_ssl: bool = True
    auth: Optional[Dict[str, str]] = None
    compatibility: str = "NONE"
    subject_name_strategy: str = "TopicNameStrategy"
    cloud_api_key: Optional[str] = None
    cloud_api_secret: Optional[str] = None
```

#### InferenceConfig
```python
class InferenceConfig(BaseModel):
    max_messages: int = 50
    timeout: int = 30
    max_depth: int = 5
    confidence_threshold: float = 0.8
    auto_detect_format: bool = True
    forced_data_format: Optional[str] = None
```

### Core Classes

#### SchemaInferrer
```python
class SchemaInferrer:
    def __init__(self, max_depth: int = 5, confidence_threshold: float = 0.8):
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
- Maximum nesting depth of 5 levels
- Limited support for recursive structures
- No support for circular references

#### 3. **Performance**
- Memory usage scales with topic size
- Processing time increases with message count
- Limited parallel processing for single large topics

#### 4. **Platform Support**
- Requires Python 3.8+
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
- Limited compatibility level support
- No support for schema references
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
- Stack Overflow: Tag questions with `schema-infer-schema-infer`

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
git clone https://github.com/schema-inferinc/schema-infer-plugin.git
cd schema-infer-plugin
pip install -r requirements-dev.txt
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

The Schema Inference Schema Inference Plugin provides a powerful, flexible, and production-ready solution for automatic schema generation from Kafka topics. With support for multiple schema formats, comprehensive data type detection, and enterprise-grade security, it's the ideal tool for modern data architectures.

Whether you're migrating from untyped to typed systems, establishing data governance, or accelerating development workflows, this plugin provides the tools and flexibility you need to succeed.

For more information, examples, and support, visit our [GitHub repository](https://github.com/schema-inferinc/schema-infer-plugin) or contact Schema Inference Support.
