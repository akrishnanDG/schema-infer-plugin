# Using Schema Inference with Terraform

This guide covers how to use the Schema Inference Plugin with the [Confluent Terraform Provider](https://registry.terraform.io/providers/confluentinc/confluent/latest) to automatically infer schemas from Confluent Cloud Kafka topics and register them to Confluent Cloud Schema Registry as part of your Infrastructure-as-Code workflow.

> **Note**: Terraform integration is supported with **Confluent Cloud** only.

## How It Works

A reusable Terraform module calls the `schema-infer` CLI via Terraform's built-in `external` data source. The inferred schema feeds directly into `confluent_schema` resources.

```
terraform plan
    |
    +-> external data source calls schema-infer CLI
    |   (connects to Confluent Cloud, samples messages, infers schema)
    |
    +-> confluent_schema compares inferred schema against state
    |   (shows diff if schema evolved)
    |
    +-> terraform apply registers via Confluent provider
```

The module auto-installs `schema-infer` via pip on the first run if it is not already present.

## Prerequisites

- Terraform >= 1.0
- Python 3.9+
- A Confluent Cloud Kafka cluster with data in topics
- Confluent Cloud API keys for Kafka and Schema Registry
- Confluent Terraform Provider configured for Confluent Cloud

## Getting Started

### Option A: Inline Variables (Recommended)

Pass Confluent Cloud credentials directly as Terraform variables. No YAML config file needed. Use `json-schema` for topics with unserialized JSON messages, since it matches the data already on the wire.

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

# Infer schemas -- runs automatically during terraform plan
module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"

  bootstrap_servers = "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret

  topics       = ["orders", "payments", "users"]
  format       = "json-schema"
  max_messages = 50
}

# Register inferred schemas via Confluent provider
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
```

Credentials can come from `terraform.tfvars`, environment variables, Terraform Cloud variable sets, or HashiCorp Vault.

### Option B: YAML Config File

If you already have a Confluent Cloud `cc-config.yaml` for the CLI:

```hcl
module "inferred_schemas" {
  source      = "github.com/akrishnanDG/terraform-schema-infer"
  config_file = "${path.module}/cc-config.yaml"
  topics      = ["orders", "payments", "users"]
  format      = "json-schema"
}
```

## Module Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `topics` | Yes | - | List of Confluent Cloud Kafka topic names to infer schemas from |
| `format` | No | `avro` | Output format: `avro`, `protobuf`, or `json-schema`. Use `json-schema` for unserialized JSON messages |
| `max_messages` | No | `50` | Maximum messages to sample per topic |
| `config_file` | No | `""` | Path to Confluent Cloud YAML config file (Option B) |
| `bootstrap_servers` | No | `""` | Confluent Cloud bootstrap servers (Option A) |
| `kafka_api_key` | No | `""` | Confluent Cloud Kafka API key (Option A, sensitive) |
| `kafka_api_secret` | No | `""` | Confluent Cloud Kafka API secret (Option A, sensitive) |

## Module Outputs

| Output | Type | Description |
|--------|------|-------------|
| `schemas` | `map(string)` | Map of topic name to inferred schema string |

## Examples

### Single Topic

```hcl
module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"

  bootstrap_servers = "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret

  topics = ["orders"]
  format = "json-schema"
}

resource "confluent_schema" "orders" {
  subject_name = "orders-value"
  format       = "JSON"
  schema       = module.inferred_schemas.schemas["orders"]
  # ... cluster config, credentials
}
```

### Multiple Topics with for_each

```hcl
locals {
  topics = ["orders", "payments", "users", "inventory", "shipments"]
}

module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"

  bootstrap_servers = "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret

  topics       = local.topics
  format       = "json-schema"
  max_messages = 50
}

resource "confluent_schema" "inferred" {
  for_each     = module.inferred_schemas.schemas
  subject_name = "${each.key}-value"
  format       = "JSON"
  schema       = each.value
  # ... cluster config, credentials
}

output "registered_subjects" {
  value = [for k, v in confluent_schema.inferred : v.subject_name]
}
```

### Protobuf Schemas

```hcl
module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"

  bootstrap_servers = "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret

  topics = ["sensor-data", "device-events"]
  format = "protobuf"
}

resource "confluent_schema" "inferred" {
  for_each     = module.inferred_schemas.schemas
  subject_name = "${each.key}-value"
  format       = "PROTOBUF"
  schema       = each.value
  # ... cluster config, credentials
}
```

## Lifecycle Behavior

| Action | What happens |
|--------|-------------|
| `terraform plan` | Connects to Confluent Cloud, samples messages, infers schemas, shows diff |
| `terraform apply` | Registers schemas to Confluent Cloud Schema Registry via Confluent provider |
| `terraform plan` (again) | Re-infers; no diff if data unchanged |
| `terraform destroy` | Deletes `confluent_schema` resources from Schema Registry |

## Day-to-Day Operations

- **New topic**: Add it to the `topics` list, run `terraform plan/apply`
- **Schema evolved**: Run `terraform plan` to see the diff, `terraform apply` to register the new version
- **Remove a topic**: Remove from the list, `terraform apply` destroys the schema resource
- **Change format**: Update the `format` variable (e.g., `avro` to `protobuf`), plan/apply

## Security Considerations

- API keys passed as inline variables are marked `sensitive` in Terraform and will not appear in plan output
- When using inline variables, the module generates a temporary config file during inference and deletes it immediately after
- For production use, store credentials in Terraform Cloud variable sets, HashiCorp Vault, or encrypted `.tfvars` files
- The YAML config file (Option B) stores credentials in plaintext -- manage access accordingly

## Limitations

- Supported with **Confluent Cloud only** (not Confluent Platform or open-source Kafka)
- The `external` data source runs on every `terraform plan`, connecting to Confluent Cloud each time
- Requires Python 3.9+ on the machine running Terraform (CI runner, local workstation)
- Each topic inference runs sequentially within a single `terraform plan`
- Only value schemas are inferred (`<topic>-value` subjects); key schemas require separate handling
- Errors from the CLI surface as generic `external` data source errors

## Alternative: Generate Schemas Separately, Register with Terraform

Instead of inferring schemas inline during `terraform plan`, you can generate schema files separately using the CLI and have Terraform read them from disk. This decouples schema generation from Terraform runs and allows human review before registration.

### One-Shot Generation with `infer`

Generate schemas into a directory, review them, then register with Terraform.

**Step 1: Generate schema files**

```bash
# Generate JSON schemas for specific topics
schema-infer --config cc-config.yaml infer \
  --topics "orders,payments,users" \
  --format json-schema \
  --output-dir ./schemas/

# Or generate for all topics matching a pattern
schema-infer --config cc-config.yaml infer \
  --topic-pattern ".*" \
  --format json-schema \
  --output-dir ./schemas/ \
  --exclude-internal
```

This produces files like:

```
schemas/
  orders.json
  payments.json
  users.json
```

**Step 2: Review the generated schemas** (optional but recommended)

```bash
# Inspect a generated schema
cat schemas/orders.json | python3 -m json.tool
```

**Step 3: Register with Terraform**

```hcl
locals {
  # Automatically discover all .json files in the schemas directory
  schema_files = fileset("${path.module}/schemas", "*.json")
  topic_schemas = {
    for f in local.schema_files :
    trimsuffix(f, ".json") => file("${path.module}/schemas/${f}")
  }
}

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
```

**Step 4: Plan and apply**

```bash
terraform plan    # shows schemas to register (no Kafka connection needed)
terraform apply   # registers to Schema Registry
```

### Continuous Detection and Schema Evolution with `live` Mode

Use `live` mode to continuously consume messages, discover new topics matching prefix/pattern filters, detect schema evolution (new fields, type changes), and update schema files. Terraform then registers the updated versions.

```
┌─────────────────────────────────────┐
│  schema-infer live                  │
│  (runs continuously as a service)   │
│                                     │
│  Consumes new messages              │
│  Discovers new topics               │
│  Detects schema changes             │
│  Overwrites .json files on change   │
└──────────────┬──────────────────────┘
               │ updated schema files
               ▼
┌─────────────────────────────────────┐
│  Git diff detects changes           │
│  CI/CD triggers terraform apply     │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  terraform plan / apply             │
│  registers new schema versions      │
└─────────────────────────────────────┘
```

**Run live mode (output only, no direct registration):**

```bash
schema-infer --config cc-config.yaml live \
  --topics "orders,payments,users" \
  --format json-schema \
  --output-dir ./schemas/ \
  --batch-size 200 \
  --batch-timeout 60
```

Or monitor all topics by pattern:

```bash
schema-infer --config cc-config.yaml live \
  --topic-pattern ".*" \
  --format json-schema \
  --output-dir ./schemas/ \
  --exclude-internal
```

Live mode continuously reads new messages, incrementally builds schemas, and detects structural changes (new fields, type changes, nullability changes). When a schema evolves, it overwrites the corresponding `.json` file.

**CI/CD pipeline example (GitHub Actions):**

```yaml
name: Register Schemas
on:
  push:
    paths:
      - 'schemas/*.json'

jobs:
  register:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
      - run: terraform init
      - run: terraform plan -out=plan.tfplan
      - run: terraform apply plan.tfplan
```

### Comparison of Approaches

| Approach | Schema generation | Terraform's role | Human review | Real-time |
|----------|-------------------|------------------|-------------|-----------|
| **Module (inline)** | During `terraform plan` | Infers + registers | No (automatic) | Per plan run |
| **`infer` + file()** | Manual CLI run | Registers only | Yes | No |
| **`live` + file()** | Continuous (evolution + new topic discovery) | Registers only | Yes (via PR) | Real-time |

**When to use which:**

- **Module (inline)**: Simplest setup, good for stable environments with known topics
- **`infer` + file()**: Best for initial schema bootstrap with human review before registration
- **`live` + file()**: Best for production schema governance where data shapes evolve over time and new topics need auto-discovery

## Integration with Confluent Tableflow

Schema inference pairs with [Tableflow](https://docs.confluent.io/cloud/current/topics/tableflow/overview.html) to materialize schemaless Kafka topics as Iceberg/Delta Lake tables. Add `confluent_tableflow_topic` resources alongside your inferred schemas:

```hcl
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

  depends_on = [confluent_schema.inferred]

  lifecycle {
    prevent_destroy = true
  }
}
```

The `depends_on` ensures schemas are registered before Tableflow attempts materialization. For the full guide, see [TABLEFLOW.md](TABLEFLOW.md).

## Troubleshooting

### "schema-infer command not found"

The module auto-installs on first run. If it fails:

```bash
# Install manually
pip install git+https://github.com/akrishnanDG/schema-infer-plugin.git

# Verify
schema-infer --help
```

### "No schema generated for topic"

The topic may be empty or contain binary data:

```bash
# Check if the topic has messages
schema-infer --config cc-config.yaml list-topics --show-metadata

# Try with more messages or explicit format
# Increase max_messages in the module, or use the CLI to debug:
schema-infer --config cc-config.yaml infer --topic my-topic --max-messages 500
```

### Authentication errors

Verify your Confluent Cloud credentials work with the CLI first:

```bash
schema-infer --config cc-config.yaml list-topics
```

If that works, ensure the same credentials are passed to the Terraform module.
