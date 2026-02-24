# Using Schema Inference with Confluent Tableflow

This guide covers how to use the Schema Inference Plugin to bootstrap schemas for Confluent Cloud [Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html), which materializes Kafka topics as Apache Iceberg or Delta Lake tables.

> **Note**: Tableflow is a Confluent Cloud feature. Schema inference for Tableflow is supported with **Confluent Cloud** only.

## Why Schema Inference + Tableflow

Tableflow uses Confluent Cloud Schema Registry as the source of truth for defining table structure. **If a topic doesn't have a schema registered, Tableflow can't materialize it into a table.**

Many Kafka topics are created without schemas -- producers write raw JSON, CSV, or other formats directly. For topics with unserialized JSON messages, use `json-schema` as the output format since it matches the data already on the wire. Schema inference bridges this gap:

```
Schemaless Kafka topics
    |
    +-> schema-infer reads messages, infers structure
    |
    +-> Registers JSON Schema to Schema Registry
    |
    +-> Tableflow materializes topic as Iceberg/Delta Lake table
```

## Prerequisites

- Confluent Cloud Kafka cluster with data in topics
- Confluent Cloud Schema Registry
- Tableflow enabled on the Confluent Cloud environment
- Tableflow API keys (separate from Kafka/SR API keys)
- `schema-infer` CLI installed (`pip install git+https://github.com/akrishnanDG/schema-infer-plugin.git`)

## Quick Start (CLI Only)

### Step 1: Infer and Register Schemas

```bash
# Single topic
schema-infer --config cc-config.yaml infer \
  --topic orders --format json-schema --register

# Multiple topics
schema-infer --config cc-config.yaml infer \
  --topics "orders,payments,users" --format json-schema --register

# All topics matching a pattern
schema-infer --config cc-config.yaml infer \
  --topic-pattern ".*" --format json-schema --register --exclude-internal
```

### Step 2: Enable Tableflow

```bash
# Enable Tableflow on the topic via Confluent CLI
confluent tableflow topic create orders \
  --cluster lkc-xxxxx \
  --environment env-xxxxx \
  --table-formats ICEBERG
```

### Step 3: Verify

In the Confluent Cloud UI, navigate to the topic and check the Tableflow tab. The topic should show materialization status and the inferred schema driving the table structure.

## Using with Terraform

Combine the schema inference Terraform module with the `confluent_tableflow_topic` resource for a fully automated pipeline.

### Inline Module Approach

Schema inference happens during `terraform plan`. Tableflow is enabled after schema registration.

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
  topics = ["orders", "payments", "users"]
}

# Step 1: Infer schemas from Kafka topics
module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"

  bootstrap_servers = var.bootstrap_servers
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret

  topics       = local.topics
  format       = "json-schema"
  max_messages = 50
}

# Step 2: Register schemas to Schema Registry
resource "confluent_schema" "inferred" {
  for_each     = module.inferred_schemas.schemas
  subject_name = "${each.key}-value"
  format       = "JSON"
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

# Step 3: Enable Tableflow on topics with registered schemas
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

  # Schema must be registered before Tableflow can materialize the topic
  depends_on = [confluent_schema.inferred]

  lifecycle {
    prevent_destroy = true
  }
}
```

### File-Based Approach

Generate schemas separately (with human review), then register and enable Tableflow via Terraform.

```bash
# Generate schema files
schema-infer --config cc-config.yaml infer \
  --topics "orders,payments,users" \
  --format json-schema --output-dir ./schemas/

# Review generated schemas before applying
cat schemas/orders.json | python3 -m json.tool
```

```hcl
locals {
  schema_files = fileset("${path.module}/schemas", "*.json")
  topic_schemas = {
    for f in local.schema_files :
    trimsuffix(f, ".json") => file("${path.module}/schemas/${f}")
  }
}

# Register schemas
resource "confluent_schema" "inferred" {
  for_each     = local.topic_schemas
  subject_name = "${each.key}-value"
  format       = "JSON"
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

# Enable Tableflow
resource "confluent_tableflow_topic" "materialized" {
  for_each = local.topic_schemas

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

  depends_on = [confluent_schema.inferred]

  lifecycle {
    prevent_destroy = true
  }
}
```

### Live Mode with Tableflow

Use `live` mode to continuously detect schema evolution. When schemas change, Terraform updates Schema Registry and Tableflow picks up the new table structure automatically.

```
┌─────────────────────────────────────┐
│  schema-infer live                  │
│  (runs continuously)                │
│                                     │
│  Detects schema changes             │
│  Updates .json files                │
└──────────────┬──────────────────────┘
               │ updated schema files
               ▼
┌─────────────────────────────────────┐
│  CI/CD pipeline                     │
│  terraform apply                    │
│                                     │
│  Updates confluent_schema           │
│  Tableflow picks up new structure   │
└─────────────────────────────────────┘
```

```bash
# Run live mode to track schema evolution
schema-infer --config cc-config.yaml live \
  --topics "orders,payments,users" \
  --format json-schema \
  --output-dir ./schemas/
```

When live mode detects a new field or type change, it overwrites the `.json` file. A CI/CD pipeline triggers `terraform apply`, which updates the schema in Schema Registry. Tableflow automatically reflects the updated table structure.

## Tableflow Configuration Options

The `confluent_tableflow_topic` resource supports:

| Option | Description |
|--------|-------------|
| `table_formats` | `["ICEBERG"]`, `["DELTA"]`, or `["ICEBERG", "DELTA"]` |
| `managed_storage {}` | Use Confluent-managed storage |
| `byob_aws { bucket_name, provider_integration_id }` | Bring your own S3 bucket |
| `azure_data_lake_storage_gen_2 { ... }` | Azure ADLS Gen2 storage |
| `retention_ms` | Max age of snapshots/versions in milliseconds |
| `error_handling { mode, log_target }` | `SUSPEND`, `SKIP`, or `LOG` on bad records |

## Schema Format Considerations

Tableflow supports all three schema formats that schema-infer generates:

| Format | Tableflow support | Best for |
|--------|-------------------|----------|
| **Avro** | Full support | Most Kafka-native use cases, best Schema Registry integration |
| **Protobuf** | Full support | High-performance applications, cross-language compatibility |
| **JSON Schema** | Full support | Unserialized JSON messages, web APIs, human-readable schemas |

Use **JSON Schema** when your topics contain unserialized JSON messages (raw JSON produced without a schema serializer). Since the data is already plain JSON on the wire, JSON Schema is the natural fit -- it describes the structure without requiring producers to change serialization. Avro and Protobuf are better suited for topics where producers already use those serialization formats.

## Verification Checklist

After running the pipeline:

1. **Schema Registry**: Verify subjects exist
   ```bash
   curl -u "$SR_API_KEY:$SR_API_SECRET" \
     "$SR_ENDPOINT/subjects" | python3 -m json.tool
   ```

2. **Tableflow status**: Check in Confluent Cloud UI under the topic's Tableflow tab

3. **Table queryable**: If using managed storage, query the Iceberg table via Confluent Cloud or connected query engines (Spark, Trino, etc.)

4. **Schema evolution**: Produce a message with a new field, verify live mode detects it, and confirm the materialized table reflects the new column

## Troubleshooting

### Tableflow fails to materialize

- Ensure the schema is registered **before** enabling Tableflow (`depends_on` in Terraform)
- Verify the schema subject follows the naming convention (`<topic>-value`)
- Check that the schema format matches what producers are writing

### Schema mismatch with existing data

- Increase `max_messages` to sample more data for better type inference
- Use `--data-format json` to force JSON parsing if auto-detection is wrong
- Review the generated schema and manually adjust if needed before registration

### Tableflow API key errors

Tableflow API keys are separate from Kafka and Schema Registry API keys. Create them in the Confluent Cloud UI under the environment's API keys section.
