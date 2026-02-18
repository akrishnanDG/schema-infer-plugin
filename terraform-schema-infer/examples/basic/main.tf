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

# Infer schema from a single topic -- no YAML config file needed
module "inferred_schemas" {
  source = "github.com/akrishnanDG/terraform-schema-infer"

  bootstrap_servers = var.bootstrap_servers
  kafka_api_key     = var.kafka_api_key
  kafka_api_secret  = var.kafka_api_secret

  topics       = ["orders"]
  format       = "avro"
  max_messages = 100
}

# Register the inferred schema
resource "confluent_schema" "orders" {
  subject_name = "orders-value"
  format       = "AVRO"
  schema       = module.inferred_schemas.schemas["orders"]

  schema_registry_cluster {
    id = var.sr_cluster_id
  }

  rest_endpoint = var.sr_rest_endpoint

  credentials {
    key    = var.sr_api_key
    secret = var.sr_api_secret
  }
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
