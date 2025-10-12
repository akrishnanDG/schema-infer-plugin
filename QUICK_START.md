# Quick Start Guide

Get up and running with the Schema Inference Plugin in minutes.

## 🚀 Installation

```bash
# Install the plugin
python3 install-plugin.py

# Verify installation
schema-infer --help
```

## ⚡ Basic Usage

### 1. Single Topic Schema Generation

```bash
# Generate JSON Schema
schema-infer infer --topic user-events --output user-schema.json --format json-schema

# Generate Avro Schema
schema-infer infer --topic user-events --output user-schema.avsc --format avro

# Generate Protobuf Schema
schema-infer infer --topic user-events --output user-schema.proto --format protobuf
```

### 2. Multiple Topics

```bash
# Process multiple topics
schema-infer infer --topics "user-events,order-events,payment-events" --output-dir schemas/ --format avro
```

### 3. Topic Prefix Matching

```bash
# Process all topics with prefix
schema-infer infer --topic-prefix "prod-" --output-dir schemas/ --format json-schema
```

### 4. Register in Schema Registry

```bash
# Register schema automatically
schema-infer infer --topic user-events --format avro --register
```

## 🔧 Configuration

### Basic Configuration File

Create `config.yaml`:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"

schema_registry:
  url: "http://localhost:8081"

inference:
  max_messages: 50
  timeout: 30

performance:
  show_progress: true
  verbose_logging: false
```

### Use Configuration

```bash
schema-infer --config config.yaml infer --topic my-topic --format avro
```

## ☁️ Schema Inference Cloud Setup

### 1. Get Your Credentials

From Schema Inference Cloud Console:
- Bootstrap servers
- API key and secret
- Schema Registry URL

### 2. Create Cloud Config

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

### 3. Run with Cloud Config

```bash
schema-infer --config cloud-config.yaml infer --topic my-topic --format avro
```

## 🏢 Schema Inference Platform Setup

### 1. Platform Config

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

### 2. Run with Platform Config

```bash
schema-infer --config platform-config.yaml infer --topic my-topic --format avro
```

## 📊 Common Use Cases

### Generate Schemas for API Documentation

```bash
# Generate JSON schemas for API topics
schema-infer infer --topic-pattern ".*-api" --format json-schema --output-dir api-schemas/
```

### Migrate to Typed System

```bash
# Generate and register Avro schemas
schema-infer infer --topic-prefix "legacy-" --format avro --register
```

### Process IoT Data

```bash
# High-throughput IoT data processing
schema-infer infer --topic-prefix "iot-" --max-messages 10000 --format protobuf --output-dir iot-schemas/
```

## 🔍 Discovery Commands

### List Topics

```bash
# List all topics
schema-infer list-topics

# Exclude internal topics
schema-infer list-topics --exclude-internal
```

### Validate Topics

```bash
# Validate specific topics
schema-infer validate-topics --topics "user-events,order-events"
```

## 🎯 Tips & Tricks

### Performance Optimization

```bash
# For large topics
schema-infer infer --topic large-topic --max-messages 5000 --timeout 60

# For multiple topics
schema-infer infer --topics "topic1,topic2,topic3" --output-dir schemas/
```

### Debug Mode

```bash
# Enable verbose logging
schema-infer --config config.yaml infer --topic my-topic --verbose
```

### Custom Filtering

```bash
# Exclude internal topics
schema-infer infer --topic-prefix "prod-" --exclude-internal

# Custom internal prefix
schema-infer infer --topic-prefix "app-" --internal-prefix "internal-"
```

## 🆘 Troubleshooting

### Connection Issues

```bash
# Test connectivity
schema-infer validate-topics --topics "test-topic" --check-connectivity
```

### Common Errors

1. **"No topics found"**
   - Check topic names
   - Verify Kafka connectivity
   - Check topic permissions

2. **"Schema generation failed"**
   - Check message format
   - Verify data is not binary
   - Increase timeout

3. **"Authentication failed"**
   - Check API keys
   - Verify credentials
   - Check security protocol

### Get Help

```bash
# Show help
schema-infer --help
schema-infer infer --help
schema-infer list-topics --help
```

## 📚 Next Steps

1. **Read the full documentation**: [DOCUMENTATION.md](DOCUMENTATION.md)
2. **Explore examples**: [examples/](examples/) directory
3. **Run tests**: `python run_tests.py`
4. **Join the community**: GitHub Issues and Discussions

## 🎉 You're Ready!

You now have everything you need to start generating schemas from your Kafka topics. Happy schema inferring! 🚀
