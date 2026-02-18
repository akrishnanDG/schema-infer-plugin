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

# Infer schemas for all topics -- using inline variables (no YAML file)
module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"

  bootstrap_servers = var.bootstrap_servers
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret

  topics       = local.topics
  format       = "avro"
  max_messages = 100
}

# Register all inferred schemas using for_each
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
  description = "List of registered schema subjects"
  value       = [for k, v in confluent_schema.inferred : v.subject_name]
}

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

variable "sr_cluster_id" {
  type = string
}

variable "sr_rest_endpoint" {
  type = string
}

variable "sr_api_key" {
  type      = string
  sensitive = true
}

variable "sr_api_secret" {
  type      = string
  sensitive = true
}
