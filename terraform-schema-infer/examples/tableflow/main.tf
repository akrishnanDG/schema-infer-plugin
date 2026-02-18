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
  format       = "avro"
  max_messages = 100
}

# Step 2: Register schemas to Schema Registry
resource "confluent_schema" "inferred" {
  for_each     = module.inferred_schemas.schemas
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

# Step 3: Enable Tableflow -- materializes topics as Iceberg tables
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

# --- Outputs ---

output "registered_schemas" {
  value = [for k, v in confluent_schema.inferred : v.subject_name]
}

output "tableflow_topics" {
  value = [for k, v in confluent_tableflow_topic.materialized : v.display_name]
}

# --- Variables ---

variable "confluent_cloud_api_key" {
  type      = string
  sensitive = true
}

variable "confluent_cloud_api_secret" {
  type      = string
  sensitive = true
}

variable "bootstrap_servers" {
  type = string
}

variable "kafka_api_key" {
  type      = string
  sensitive = true
}

variable "kafka_api_secret" {
  type      = string
  sensitive = true
}

variable "environment_id" {
  type        = string
  description = "Confluent Cloud environment ID (e.g., env-xxxxx)"
}

variable "kafka_cluster_id" {
  type        = string
  description = "Confluent Cloud Kafka cluster ID (e.g., lkc-xxxxx)"
}

variable "sr_cluster_id" {
  type        = string
  description = "Schema Registry cluster ID (e.g., lsrc-xxxxx)"
}

variable "sr_rest_endpoint" {
  type        = string
  description = "Schema Registry REST endpoint URL"
}

variable "sr_api_key" {
  type      = string
  sensitive = true
}

variable "sr_api_secret" {
  type      = string
  sensitive = true
}

variable "tableflow_api_key" {
  type      = string
  sensitive = true
}

variable "tableflow_api_secret" {
  type      = string
  sensitive = true
}
