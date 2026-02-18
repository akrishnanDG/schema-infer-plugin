terraform {
  required_version = ">= 1.0"
}

locals {
  use_config_file = var.config_file != ""
}

data "external" "schema" {
  for_each = toset(var.topics)
  program  = ["python3", "${path.module}/scripts/infer.py"]

  query = {
    topic        = each.value
    format       = var.format
    max_messages = tostring(var.max_messages)

    # Option A: pass config file path
    config_file = var.config_file

    # Option B: pass connection details inline
    bootstrap_servers = var.bootstrap_servers
    kafka_api_key     = var.kafka_api_key
    kafka_api_secret  = var.kafka_api_secret
    security_protocol = var.security_protocol
    schema_registry_url = var.schema_registry_url
    sr_api_key        = var.sr_api_key
    sr_api_secret     = var.sr_api_secret
  }
}
