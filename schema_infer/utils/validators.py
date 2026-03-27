"""
Validation utilities for Schema Inference Plugin
"""

import re
from typing import List, Optional, Tuple

from .exceptions import ValidationError


def validate_topic_name(topic_name: str) -> bool:
    """Validate Kafka topic name."""

    if not topic_name:
        raise ValidationError("Topic name cannot be empty")

    if len(topic_name) > 249:
        raise ValidationError("Topic name cannot exceed 249 characters")

    # Kafka topic name pattern: alphanumeric, dots, hyphens, underscores
    pattern = r"^[a-zA-Z0-9._-]+$"
    if not re.match(pattern, topic_name):
        raise ValidationError(
            "Topic name can only contain alphanumeric characters, dots, hyphens, and underscores"
        )

    # Cannot start or end with dot
    if topic_name.startswith(".") or topic_name.endswith("."):
        raise ValidationError("Topic name cannot start or end with a dot")

    # Cannot contain consecutive dots
    if ".." in topic_name:
        raise ValidationError("Topic name cannot contain consecutive dots")

    return True


def validate_schema_format(schema_format: str) -> bool:
    """Validate schema format."""

    valid_formats = ["avro", "protobuf", "json-schema"]

    if schema_format not in valid_formats:
        raise ValidationError(
            f"Invalid schema format: {schema_format}. Valid formats: {', '.join(valid_formats)}"
        )

    return True


def validate_data_format(data_format: str) -> bool:
    """Validate data format."""

    valid_formats = ["json", "csv", "key-value", "tsv", "auto"]

    if data_format not in valid_formats:
        raise ValidationError(
            f"Invalid data format: {data_format}. Valid formats: {', '.join(valid_formats)}"
        )

    return True


def validate_topic_list(topics: List[str]) -> bool:
    """Validate list of topic names."""

    if not topics:
        raise ValidationError("Topic list cannot be empty")

    for topic in topics:
        validate_topic_name(topic.strip())

    return True


def validate_max_messages(max_messages: int) -> bool:
    """Validate maximum messages parameter."""

    if max_messages <= 0:
        raise ValidationError("Maximum messages must be greater than 0")

    if max_messages > 1000000:  # 1M limit
        raise ValidationError("Maximum messages cannot exceed 1,000,000")

    return True


def validate_timeout(timeout: int) -> bool:
    """Validate timeout parameter."""

    if timeout <= 0:
        raise ValidationError("Timeout must be greater than 0")

    if timeout > 3600:  # 1 hour limit
        raise ValidationError("Timeout cannot exceed 3600 seconds")

    return True


def validate_bootstrap_servers(servers: str) -> bool:
    """Validate bootstrap servers string."""

    if not servers:
        raise ValidationError("Bootstrap servers cannot be empty")

    # Basic validation - should contain host:port pairs
    server_list = [s.strip() for s in servers.split(",")]

    for server in server_list:
        if ":" not in server:
            raise ValidationError(
                f"Invalid server format: {server}. Expected host:port"
            )

        host, port = server.split(":", 1)

        if not host:
            raise ValidationError("Host cannot be empty")

        try:
            port_num = int(port)
            if not (1 <= port_num <= 65535):
                raise ValidationError(f"Invalid port number: {port_num}")
        except ValueError:
            raise ValidationError(f"Invalid port number: {port}")

    return True


def validate_schema_registry_url(url: str) -> bool:
    """Validate Schema Registry URL."""

    if not url:
        raise ValidationError("Schema Registry URL cannot be empty")

    # Basic URL validation
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ValidationError("Schema Registry URL must start with http:// or https://")

    return True


def validate_generated_schema(
    schema_content: str, schema_format: str
) -> Tuple[bool, str]:
    """
    Validate a generated schema before registration.

    Args:
        schema_content: Schema content as string
        schema_format: Schema format (avro, protobuf, json-schema)

    Returns:
        Tuple of (is_valid, error_message)
    """
    import json

    if schema_format == "avro":
        return _validate_avro_schema(schema_content)
    elif schema_format == "json-schema":
        return _validate_json_schema(schema_content)
    elif schema_format == "protobuf":
        return _validate_protobuf_schema(schema_content)
    else:
        return True, ""


def _validate_avro_schema(schema_content: str) -> Tuple[bool, str]:
    """Validate Avro schema JSON structure."""
    import json

    try:
        schema = json.loads(schema_content)
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {e}"

    if not isinstance(schema, dict):
        return False, "Avro schema must be a JSON object"

    if "type" not in schema:
        return False, "Avro schema missing 'type' field"

    if schema.get("type") == "record":
        if "name" not in schema:
            return False, "Avro record schema missing 'name' field"
        if "fields" not in schema:
            return False, "Avro record schema missing 'fields' field"

        # Validate each field
        for i, field in enumerate(schema["fields"]):
            if "name" not in field:
                return False, f"Avro field {i} missing 'name'"
            if "type" not in field:
                return False, f"Avro field '{field.get('name', i)}' missing 'type'"

            # Check for invalid standalone type strings
            field_type = field["type"]
            if isinstance(field_type, str):
                valid_types = {
                    "null",
                    "boolean",
                    "int",
                    "long",
                    "float",
                    "double",
                    "bytes",
                    "string",
                }
                if field_type not in valid_types:
                    return (
                        False,
                        f"Avro field '{field['name']}' has invalid type '{field_type}'. Must be a primitive type or a {{\"type\": ...}} definition",
                    )

    return True, ""


def _validate_json_schema(schema_content: str) -> Tuple[bool, str]:
    """Validate JSON Schema structure."""
    import json

    try:
        schema = json.loads(schema_content)
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {e}"

    if not isinstance(schema, dict):
        return False, "JSON Schema must be a JSON object"

    return True, ""


def _validate_protobuf_schema(schema_content: str) -> Tuple[bool, str]:
    """Validate basic Protobuf syntax."""
    import re

    if (
        'syntax = "proto3";' not in schema_content
        and "syntax = 'proto3';" not in schema_content
    ):
        return False, "Missing proto3 syntax declaration"

    # Check for invalid characters in identifiers
    lines = schema_content.split("\n")
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        if not stripped or stripped.startswith("//"):
            continue

        # Check message names
        msg_match = re.match(r"message\s+(\S+)\s*\{", stripped)
        if msg_match:
            name = msg_match.group(1)
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
                return False, f"Line {i}: Invalid message name '{name}'"

        # Check field definitions for invalid names
        field_match = re.match(
            r"\s*(?:repeated\s+)?(?:\w+)\s+(\S+)\s*=\s*\d+", stripped
        )
        if field_match:
            name = field_match.group(1)
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
                return False, f"Line {i}: Invalid field name '{name}'"

    return True, ""
