# --- Connection (Option A: config file) ---

variable "config_file" {
  description = "Path to schema-infer YAML configuration file. If set, overrides all connection variables below."
  type        = string
  default     = ""
}

# --- Connection (Option B: inline variables) ---

variable "bootstrap_servers" {
  description = "Kafka bootstrap servers (e.g., pkc-xxxxx.us-east-1.aws.confluent.cloud:9092)"
  type        = string
  default     = ""
}

variable "kafka_api_key" {
  description = "Kafka API key (SASL username)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "kafka_api_secret" {
  description = "Kafka API secret (SASL password)"
  type        = string
  default     = ""
  sensitive   = true
}

variable "security_protocol" {
  description = "Kafka security protocol. Auto-detected from bootstrap_servers if not set."
  type        = string
  default     = ""
}

variable "schema_registry_url" {
  description = "Schema Registry URL (only needed if using --register outside Terraform)"
  type        = string
  default     = ""
}

variable "sr_api_key" {
  description = "Schema Registry API key"
  type        = string
  default     = ""
  sensitive   = true
}

variable "sr_api_secret" {
  description = "Schema Registry API secret"
  type        = string
  default     = ""
  sensitive   = true
}

# --- Inference settings ---

variable "topics" {
  description = "List of Kafka topic names to infer schemas from"
  type        = list(string)

  validation {
    condition     = length(var.topics) > 0
    error_message = "At least one topic must be specified."
  }
}

variable "format" {
  description = "Output schema format: avro, protobuf, or json-schema"
  type        = string
  default     = "avro"

  validation {
    condition     = contains(["avro", "protobuf", "json-schema"], var.format)
    error_message = "Format must be one of: avro, protobuf, json-schema."
  }
}

variable "max_messages" {
  description = "Maximum number of messages to sample per topic for schema inference"
  type        = number
  default     = 100

  validation {
    condition     = var.max_messages > 0
    error_message = "max_messages must be a positive number."
  }
}
