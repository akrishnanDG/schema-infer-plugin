# Schema Inference Plugin

[![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)](https://github.com/example/schema-infer-plugin)
[![Python](https://img.shields.io/badge/python-3.8+-green.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-comprehensive-green.svg)](tests/)

A powerful CLI plugin that automatically infers and generates schemas from Kafka topic data. Supports multiple schema formats (JSON Schema, Avro, Protobuf) and handles complex nested data structures with comprehensive data type detection.

## 🚀 Features

- **🎯 Multi-Format Schema Generation**: JSON Schema, Avro, and Protobuf
- **🔍 Intelligent Data Analysis**: Automatic format detection and deep nested analysis
- **⚡ High Performance**: Parallel processing and optimized message reading
- **🎛️ Flexible Topic Discovery**: Single topics, multiple topics, prefix/pattern matching
- **🔐 Enterprise Security**: Full Schema Inference Cloud and Platform authentication support
- **📊 Schema Registry Integration**: Automatic registration with compatibility management
- **🛡️ Production Ready**: Robust error handling and comprehensive logging

## 📖 Documentation

| Document | Description |
|----------|-------------|
| **[📚 Complete Documentation](DOCUMENTATION.md)** | Comprehensive product documentation with all features |
| **[⚡ Quick Start Guide](QUICK_START.md)** | Get up and running in minutes |
| **[🔧 API Reference](API_REFERENCE.md)** | Complete API documentation and class references |
| **[💡 Examples](EXAMPLES.md)** | Comprehensive examples for all use cases |
| **[🧪 Testing Guide](TESTING.md)** | Testing documentation and examples |

## 🏃‍♂️ Quick Start

### Installation

```bash
# Install the plugin
python3 install-plugin.py

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
  verify_ssl: false

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

# Configure compatibility levels
# In config.yaml:
schema_registry:
  compatibility: "BACKWARD"
  subject_name_strategy: "TopicNameStrategy"
```

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
  verify_ssl: false
```

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

- **🔄 Parallel Processing**: Multi-threaded topic processing
- **📈 Optimized Message Reading**: Smart offset selection and batch processing
- **🔗 Connection Reuse**: Efficient Kafka consumer management
- **📊 Progress Tracking**: Real-time progress bars and ETA
- **💾 Memory Management**: Configurable memory limits and streaming processing

## 🎛️ Configuration Options

### Topic Filtering
```yaml
topic_filter:
  internal_prefix: "_"
  exclude_internal: true
  additional_exclude_prefixes: ["__", "temp-", "backup-"]
  include_patterns: [".*-events", "prod-.*"]
```

### Performance Tuning
```yaml
performance:
  max_workers: 4
  batch_size: 100
  memory_limit_mb: 512
  enable_caching: true
  cache_ttl: 3600
  show_progress: true
  verbose_logging: false
```

### Schema Registry Compatibility
```yaml
schema_registry:
  compatibility: "NONE"  # NONE, BACKWARD, FORWARD, FULL
  subject_name_strategy: "TopicNameStrategy"  # TopicName, RecordName, TopicRecordName
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
- **🐛 Issues**: [GitHub Issues](https://github.com/schema-inferinc/schema-infer-plugin/issues)
- **💬 Discussions**: [GitHub Discussions](https://github.com/schema-inferinc/schema-infer-plugin/discussions)
- **🏢 Schema Inference Support**: This is an open source tool, there is no support provided but please feel free to raise and fix issues.

---

**Ready to get started?** Check out the [Quick Start Guide](QUICK_START.md) or dive into the [Complete Documentation](DOCUMENTATION.md) for comprehensive information about all features and capabilities.
