"""
Validation utilities for Schema Inference Plugin
"""

import re
from typing import List, Optional

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
        raise ValidationError(f"Invalid schema format: {schema_format}. Valid formats: {', '.join(valid_formats)}")
    
    return True


def validate_data_format(data_format: str) -> bool:
    """Validate data format."""
    
    valid_formats = ["json", "csv", "key-value", "tsv", "auto"]
    
    if data_format not in valid_formats:
        raise ValidationError(f"Invalid data format: {data_format}. Valid formats: {', '.join(valid_formats)}")
    
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
            raise ValidationError(f"Invalid server format: {server}. Expected host:port")
        
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
