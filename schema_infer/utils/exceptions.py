"""
Custom exceptions for Schema Inference Plugin
"""


class SchemaInferError(Exception):
    """Base exception for schema inference errors."""
    pass


class KafkaError(SchemaInferError):
    """Kafka-related errors."""
    pass


class SchemaRegistryError(SchemaInferError):
    """Schema Registry-related errors."""
    pass


class FormatDetectionError(SchemaInferError):
    """Data format detection errors."""
    pass


class ValidationError(SchemaInferError):
    """Validation errors."""
    pass


class ConfigurationError(SchemaInferError):
    """Configuration errors."""
    pass


class InferenceError(SchemaInferError):
    """Schema inference errors."""
    pass
