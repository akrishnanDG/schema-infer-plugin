# Examples

Comprehensive examples for the Schema Inference Schema Inference Plugin.

## Table of Contents

1. [Basic Examples](#basic-examples)
2. [Enterprise Examples](#enterprise-examples)
3. [Cloud Examples](#cloud-examples)
4. [Advanced Examples](#advanced-examples)
5. [Integration Examples](#integration-examples)
6. [Troubleshooting Examples](#troubleshooting-examples)

---

## Basic Examples

### Example 1: Simple User Events

**Scenario**: Generate schema for user events topic.

**Data Structure**:
```json
{
  "userId": "user123",
  "eventType": "login",
  "timestamp": "2025-01-15T10:30:00Z",
  "metadata": {
    "ipAddress": "192.168.1.1",
    "userAgent": "Mozilla/5.0..."
  }
}
```

**Command**:
```bash
schema-infer infer --topic user-events --output user-events-schema.json --format json-schema
```

**Generated JSON Schema**:
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "user-events",
  "type": "object",
  "properties": {
    "userId": {
      "type": "string",
      "examples": ["user123"]
    },
    "eventType": {
      "type": "string",
      "examples": ["login"]
    },
    "timestamp": {
      "type": "string",
      "examples": ["2025-01-15T10:30:00Z"]
    },
    "metadata": {
      "type": "object",
      "properties": {
        "ipAddress": {
          "type": "string"
        },
        "userAgent": {
          "type": "string"
        }
      },
      "required": ["ipAddress", "userAgent"]
    }
  },
  "required": ["userId", "eventType", "timestamp", "metadata"]
}
```

### Example 2: E-commerce Order Processing

**Scenario**: Generate Avro schema for order processing system.

**Data Structure**:
```json
{
  "orderId": "order-12345",
  "customerId": "customer-67890",
  "orderDate": "2025-01-15T10:30:00Z",
  "totalAmount": 99.99,
  "items": [
    {
      "productId": "product-001",
      "quantity": 2,
      "price": 49.99
    }
  ],
  "shippingAddress": {
    "street": "123 Main St",
    "city": "New York",
    "zipCode": "10001"
  }
}
```

**Command**:
```bash
schema-infer infer --topic order-events --output order-schema.avsc --format avro
```

**Generated Avro Schema**:
```json
{
  "type": "record",
  "name": "order_events",
  "namespace": "com.schema-infer.schema.infer",
  "fields": [
    {
      "name": "orderid",
      "type": "string"
    },
    {
      "name": "customerid",
      "type": "string"
    },
    {
      "name": "orderdate",
      "type": "string"
    },
    {
      "name": "totalamount",
      "type": "double"
    },
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "items_record",
          "fields": [
            {
              "name": "productid",
              "type": "string"
            },
            {
              "name": "quantity",
              "type": "int"
            },
            {
              "name": "price",
              "type": "double"
            }
          ]
        }
      }
    },
    {
      "name": "shippingaddress",
      "type": {
        "type": "record",
        "name": "shippingaddress_record",
        "fields": [
          {
            "name": "street",
            "type": "string"
          },
          {
            "name": "city",
            "type": "string"
          },
          {
            "name": "zipcode",
            "type": "string"
          }
        ]
      }
    }
  ]
}
```

### Example 3: IoT Sensor Data

**Scenario**: Generate Protobuf schema for IoT sensor data.

**Data Structure**:
```json
{
  "sensorId": "sensor-001",
  "timestamp": "2025-01-15T10:30:00Z",
  "location": {
    "latitude": 40.7128,
    "longitude": -74.0060
  },
  "readings": [
    {
      "type": "temperature",
      "value": 22.5,
      "unit": "celsius"
    },
    {
      "type": "humidity",
      "value": 65.0,
      "unit": "percent"
    }
  ]
}
```

**Command**:
```bash
schema-infer infer --topic iot-sensor-data --output sensor-schema.proto --format protobuf
```

**Generated Protobuf Schema**:
```protobuf
syntax = "proto3";

package com_schema-infer_schema_infer;

message iot_sensor_data {
  string sensorid = 1;
  string timestamp = 2;
  location_message location = 3;
  repeated readings_message readings = 4;
  
  message location_message {
    double latitude = 1;
    double longitude = 2;
  }
  
  message readings_message {
    string type = 1;
    double value = 2;
    string unit = 3;
  }
}
```

---

## Enterprise Examples

### Example 4: Multi-Service Architecture

**Scenario**: Generate schemas for microservices communication.

**Configuration**:
```yaml
# microservices-config.yaml
kafka:
  bootstrap_servers: "kafka-cluster:9092"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  sasl_username: "kafka"
  sasl_password: "secure-password"

schema_registry:
  url: "https://schemaregistry:8081"
  verify_ssl: false
  compatibility: "BACKWARD"

topic_filter:
  include_patterns: [".*-service", ".*-api"]
  exclude_internal: true
  additional_exclude_prefixes: ["internal-", "temp-"]

performance:
  max_workers: 8
  batch_size: 200
  show_progress: true
```

**Commands**:
```bash
# Generate schemas for all service topics
schema-infer --config microservices-config.yaml infer --topic-pattern ".*-service" --format avro --output-dir service-schemas/

# Generate JSON schemas for API documentation
schema-infer --config microservices-config.yaml infer --topic-pattern ".*-api" --format json-schema --output-dir api-docs/

# Register critical schemas
schema-infer --config microservices-config.yaml infer --topics "user-service,order-service,payment-service" --format avro --register
```

### Example 5: Data Migration Project

**Scenario**: Migrate from legacy untyped system to typed system.

**Configuration**:
```yaml
# migration-config.yaml
kafka:
  bootstrap_servers: "legacy-kafka:9092"
  security_protocol: "PLAINTEXT"

schema_registry:
  url: "http://new-schemaregistry:8081"
  compatibility: "FULL"
  subject_name_strategy: "TopicNameStrategy"

inference:
  max_messages: 5000
  timeout: 120
  confidence_threshold: 0.95

performance:
  max_workers: 4
  batch_size: 100
  memory_limit_mb: 2048
```

**Migration Process**:
```bash
# Step 1: Analyze legacy topics
schema-infer --config migration-config.yaml list-topics

# Step 2: Generate schemas for legacy topics
schema-infer --config migration-config.yaml infer --topic-prefix "legacy-" --format avro --output-dir legacy-schemas/

# Step 3: Register schemas in new registry
schema-infer --config migration-config.yaml infer --topic-prefix "legacy-" --format avro --register

# Step 4: Validate schema compatibility
schema-infer --config migration-config.yaml validate-topics --topics "legacy-user-events,legacy-order-events" --check-schema-registry
```

### Example 6: Compliance and Governance

**Scenario**: Establish data governance with schema validation.

**Configuration**:
```yaml
# governance-config.yaml
kafka:
  bootstrap_servers: "governance-kafka:9092"
  security_protocol: "SASL_SSL"

schema_registry:
  url: "https://governance-registry:8081"
  compatibility: "FULL"
  subject_name_strategy: "TopicNameStrategy"

topic_filter:
  include_patterns: ["prod-.*", "staging-.*"]
  exclude_internal: true

inference:
  max_messages: 5000
  confidence_threshold: 0.9
  max_depth: 6

performance:
  max_workers: 16
  batch_size: 500
  enable_caching: true
```

**Governance Process**:
```bash
# Generate schemas for all production topics
schema-infer --config governance-config.yaml infer --topic-prefix "prod-" --format avro --register

# Generate documentation schemas
schema-infer --config governance-config.yaml infer --topic-prefix "prod-" --format json-schema --output-dir governance-docs/

# Validate all topics
schema-infer --config governance-config.yaml validate-topics --check-connectivity --check-schema-registry
```

---

## Cloud Examples

### Example 7: Schema Inference Cloud Setup

**Scenario**: Generate schemas using Schema Inference Cloud.

**Configuration**:
```yaml
# cloud-config.yaml
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
  compatibility: "BACKWARD"

performance:
  max_workers: 4
  show_progress: true
  verbose_logging: false
```

**Commands**:
```bash
# List topics in Schema Inference Cloud
schema-infer --config cloud-config.yaml list-topics

# Generate schemas for cloud topics
schema-infer --config cloud-config.yaml infer --topic-prefix "cloud-" --format avro --register

# Process high-throughput topics
schema-infer --config cloud-config.yaml infer --topic "high-volume-events" --max-messages 10000 --format protobuf --output-dir cloud-schemas/
```

### Example 8: Multi-Environment Setup

**Scenario**: Generate schemas across development, staging, and production environments.

**Development Configuration**:
```yaml
# dev-config.yaml
kafka:
  bootstrap_servers: "dev-kafka:9092"
  security_protocol: "PLAINTEXT"

schema_registry:
  url: "http://dev-registry:8081"
  verify_ssl: false

inference:
  max_messages: 100
  timeout: 10

performance:
  max_workers: 2
  verbose_logging: true
```

**Production Configuration**:
```yaml
# prod-config.yaml
kafka:
  bootstrap_servers: "prod-kafka:9092"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  sasl_username: "prod-kafka"
  sasl_password: "secure-password"

schema_registry:
  url: "https://prod-registry:8081"
  verify_ssl: true
  compatibility: "FULL"

inference:
  max_messages: 5000
  timeout: 60

performance:
  max_workers: 8
  batch_size: 200
  enable_caching: true
```

**Environment-Specific Commands**:
```bash
# Development
schema-infer --config dev-config.yaml infer --topic "dev-user-events" --format json-schema --output dev-schema.json

# Staging
schema-infer --config staging-config.yaml infer --topic-prefix "staging-" --format avro --output-dir staging-schemas/

# Production
schema-infer --config prod-config.yaml infer --topic-prefix "prod-" --format avro --register
```

---

## Advanced Examples

### Example 9: Custom Format Detection

**Scenario**: Handle custom data formats with forced format detection.

**Configuration**:
```yaml
# custom-format-config.yaml
kafka:
  bootstrap_servers: "localhost:9092"

inference:
  auto_detect_format: false
  forced_data_format: "json"
  max_messages: 2000
  confidence_threshold: 0.7

performance:
  max_workers: 6
  batch_size: 150
```

**Commands**:
```bash
# Force JSON format detection
schema-infer --config custom-format-config.yaml infer --topic "custom-format-topic" --format avro

# Process with custom confidence threshold
schema-infer --config custom-format-config.yaml infer --topic "low-confidence-topic" --format json-schema
```

### Example 10: Performance Optimization

**Scenario**: Optimize for large-scale data processing.

**High-Performance Configuration**:
```yaml
# performance-config.yaml
kafka:
  bootstrap_servers: "high-perf-kafka:9092"
  security_protocol: "SASL_SSL"

inference:
  max_messages: 5000
  timeout: 120
  max_depth: 4

performance:
  max_workers: 16
  batch_size: 500
  memory_limit_mb: 4096
  enable_caching: true
  cache_ttl: 7200
  show_progress: true
  verbose_logging: false
```

**Commands**:
```bash
# Process large topics with high performance
schema-infer --config performance-config.yaml infer --topic "large-topic" --max-messages 10000 --format protobuf

# Process multiple topics in parallel
schema-infer --config performance-config.yaml infer --topics "topic1,topic2,topic3,topic4" --format avro --output-dir high-perf-schemas/
```

### Example 11: Complex Nested Data

**Scenario**: Handle deeply nested and complex data structures.

**Data Structure**:
```json
{
  "user": {
    "profile": {
      "personal": {
        "name": "John Doe",
        "address": {
          "home": {
            "street": "123 Main St",
            "city": "New York",
            "coordinates": {
              "latitude": 40.7128,
              "longitude": -74.0060
            }
          },
          "work": {
            "street": "456 Business Ave",
            "city": "New York"
          }
        }
      },
      "preferences": {
        "notifications": {
          "email": true,
          "sms": false,
          "push": true
        }
      }
    },
    "orders": [
      {
        "orderId": "order-001",
        "items": [
          {
            "product": {
              "id": "product-001",
              "details": {
                "name": "Laptop",
                "specifications": {
                  "cpu": "Intel i7",
                  "ram": "16GB",
                  "storage": "512GB SSD"
                }
              }
            },
            "quantity": 1,
            "price": 999.99
          }
        ]
      }
    ]
  }
}
```

**Configuration**:
```yaml
# complex-data-config.yaml
kafka:
  bootstrap_servers: "localhost:9092"

inference:
  max_messages: 2000
  max_depth: 6
  confidence_threshold: 0.8

performance:
  max_workers: 4
  batch_size: 100
  show_progress: true
  verbose_logging: true
```

**Commands**:
```bash
# Generate schema for complex nested data
schema-infer --config complex-data-config.yaml infer --topic "complex-user-data" --format json-schema --output complex-schema.json

# Generate Avro schema with deep nesting
schema-infer --config complex-data-config.yaml infer --topic "complex-user-data" --format avro --output complex-schema.avsc
```

---

## Integration Examples

### Example 12: CI/CD Pipeline Integration

**Scenario**: Integrate schema generation into CI/CD pipeline.

**GitHub Actions Workflow**:
```yaml
# .github/workflows/schema-generation.yml
name: Schema Generation

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  generate-schemas:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        python install-plugin.py
    
    - name: Generate schemas
      run: |
        # Create config file with secrets from GitHub Actions
        cat > config.yaml << EOF
        kafka:
          bootstrap_servers: "${{ secrets.KAFKA_BOOTSTRAP_SERVERS }}"
          security_protocol: "SASL_SSL"
          sasl_mechanism: "PLAIN"
          cloud_api_key: "${{ secrets.CLOUD_API_KEY }}"
          cloud_api_secret: "${{ secrets.CLOUD_API_SECRET }}"
        schema_registry:
          url: "${{ secrets.SCHEMA_REGISTRY_URL }}"
          cloud_api_key: "${{ secrets.CLOUD_API_KEY }}"
          cloud_api_secret: "${{ secrets.CLOUD_API_SECRET }}"
        EOF
        
        schema-infer --config config.yaml infer --topic-prefix "prod-" --format avro --output-dir schemas/
    
    - name: Upload schemas
      uses: actions/upload-artifact@v3
      with:
        name: generated-schemas
        path: schemas/
```

### Example 13: Docker Integration

**Scenario**: Run schema generation in Docker containers.

**Dockerfile**:
```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN python install-plugin.py

COPY config/ /app/config/

CMD ["schema-infer", "schema", "--config", "/app/config/schema-infer.yaml", "infer", "--topic-prefix", "prod-", "--format", "avro", "--output-dir", "/app/schemas/"]
```

**Docker Compose**:
```yaml
version: '3.8'

services:
  schema-generator:
    build: .
    volumes:
      - ./config:/app/config
      - ./schemas:/app/schemas
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - SCHEMA_REGISTRY_URL=http://schemaregistry:8081
    depends_on:
      - kafka
      - schemaregistry
```

### Example 14: Kubernetes Integration

**Scenario**: Deploy schema generation as Kubernetes job.

**Kubernetes Job**:
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: schema-generation
spec:
  template:
    spec:
      containers:
      - name: schema-generator
        image: schema-infer-schema-infer:latest
        command:
        - schema-infer
        - schema
        - --config
        - /app/config/schema-infer.yaml
        - infer
        - --topic-prefix
        - prod-
        - --format
        - avro
        - --output-dir
        - /app/schemas/
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-cluster:9092"
        - name: SCHEMA_REGISTRY_URL
          value: "https://schemaregistry:8081"
        volumeMounts:
        - name: config
          mountPath: /app/config
        - name: schemas
          mountPath: /app/schemas
      volumes:
      - name: config
        configMap:
          name: schema-config
      - name: schemas
        persistentVolumeClaim:
          claimName: schema-storage
      restartPolicy: Never
```

---

## Troubleshooting Examples

### Example 15: Connection Issues

**Problem**: Cannot connect to Kafka cluster.

**Debug Configuration**:
```yaml
# debug-config.yaml
kafka:
  bootstrap_servers: "localhost:9092"
  security_protocol: "PLAINTEXT"

performance:
  verbose_logging: true
```

**Debug Commands**:
```bash
# Test connectivity
schema-infer --config debug-config.yaml validate-topics --topics "test-topic" --check-connectivity

# List topics with verbose logging
schema-infer --config debug-config.yaml list-topics --show-metadata
```

### Example 16: Performance Issues

**Problem**: Slow processing or timeouts.

**Optimized Configuration**:
```yaml
# optimized-config.yaml
kafka:
  bootstrap_servers: "localhost:9092"

inference:
  max_messages: 50
  timeout: 60

performance:
  max_workers: 4
  batch_size: 100
  memory_limit_mb: 1024
  show_progress: true
  verbose_logging: true
```

**Optimized Commands**:
```bash
# Process with optimized settings
schema-infer --config optimized-config.yaml infer --topic "large-topic" --max-messages 1000 --timeout 60

# Process multiple topics with progress tracking
schema-infer --config optimized-config.yaml infer --topics "topic1,topic2,topic3" --format avro --output-dir schemas/
```

### Example 17: Schema Registry Issues

**Problem**: Cannot register schemas in Schema Registry.

**Registry Debug Configuration**:
```yaml
# registry-debug-config.yaml
kafka:
  bootstrap_servers: "localhost:9092"

schema_registry:
  url: "http://localhost:8081"
  verify_ssl: false
  compatibility: "NONE"

performance:
  verbose_logging: true
```

**Registry Debug Commands**:
```bash
# Test Schema Registry connectivity
schema-infer --config registry-debug-config.yaml validate-topics --topics "test-topic" --check-schema-registry

# Register schema with debug logging
schema-infer --config registry-debug-config.yaml infer --topic "test-topic" --format avro --register
```

---

These examples demonstrate the versatility and power of the Schema Inference Schema Inference Plugin across various use cases, from simple single-topic scenarios to complex enterprise deployments. Each example includes configuration files, commands, and expected outputs to help you get started quickly.
