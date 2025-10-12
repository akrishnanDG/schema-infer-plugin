# API Reference

Complete API reference for the Schema Inference Schema Inference Plugin.

## Table of Contents

1. [CLI Commands](#cli-commands)
2. [Configuration Schema](#configuration-schema)
3. [Python API](#python-api)
4. [Schema Classes](#schema-classes)
5. [Generator Classes](#generator-classes)
6. [Core Classes](#core-classes)
7. [Utility Classes](#utility-classes)
8. [Error Classes](#error-classes)

---

## CLI Commands

### Global Options

All commands support these global options:

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--config` | `-c` | Path to YAML configuration file | None |
| `--help` | `-h` | Show help message | False |
| `--version` | `-v` | Show version information | False |

### `infer` Command

Generate schemas from Kafka topics.

```bash
schema-infer infer [OPTIONS]
```

#### Options

| Option | Short | Type | Description | Default |
|--------|-------|------|-------------|---------|
| `--topic` | `-t` | String | Single topic name | None |
| `--topics` | | String | Comma-separated list of topics | None |
| `--topic-prefix` | | String | Process topics with specific prefix | None |
| `--topic-pattern` | | String | Regex pattern for topic selection | None |
| `--output` | `-o` | String | Output file path (single topic) | None |
| `--output-dir` | `-d` | String | Output directory (multiple topics) | None |
| `--format` | `-f` | String | Schema format (json-schema, avro, protobuf) | json-schema |
| `--register` | `-r` | Boolean | Register schema in Schema Registry | False |
| `--max-messages` | `-m` | Integer | Maximum messages to read | 1000 |
| `--timeout` | `-T` | Integer | Timeout in seconds | 30 |
| `--exclude-internal` | | Boolean | Exclude internal topics | True |
| `--internal-prefix` | | String | Custom internal topic prefix | "_" |
| `--additional-exclude-prefixes` | | String | Additional prefixes to exclude | None |
| `--include-patterns` | | String | Patterns to include | None |

#### Examples

```bash
# Single topic
schema-infer infer --topic user-events --output user-schema.json --format json-schema

# Multiple topics
schema-infer infer --topics "user-events,order-events" --output-dir schemas/ --format avro

# Prefix matching
schema-infer infer --topic-prefix "prod-" --output-dir schemas/ --format protobuf

# Register in Schema Registry
schema-infer infer --topic user-events --format avro --register
```

### `list-topics` Command

List available topics with filtering options.

```bash
schema-infer list-topics [OPTIONS]
```

#### Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `--exclude-internal` | Boolean | Exclude internal topics | True |
| `--internal-prefix` | String | Custom internal topic prefix | "_" |
| `--additional-exclude-prefixes` | String | Additional prefixes to exclude | None |
| `--include-patterns` | String | Patterns to include | None |
| `--show-metadata` | Boolean | Show topic metadata | False |

#### Examples

```bash
# List all topics
schema-infer list-topics

# Exclude internal topics
schema-infer list-topics --exclude-internal

# Custom filtering
schema-infer list-topics --internal-prefix "internal-" --additional-exclude-prefixes "temp-,backup-"
```

### `validate-topics` Command

Validate topics for schema inference.

```bash
schema-infer validate-topics [OPTIONS]
```

#### Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `--topics` | String | Comma-separated list of topics to validate | None |
| `--check-connectivity` | Boolean | Check Kafka connectivity | False |
| `--check-schema-registry` | Boolean | Check Schema Registry connectivity | False |

#### Examples

```bash
# Validate specific topics
schema-infer validate-topics --topics "user-events,order-events"

# Full validation
schema-infer validate-topics --topics "user-events" --check-connectivity --check-schema-registry
```

### `version` Command

Show version and build information.

```bash
schema-infer version
```

---

## Configuration Schema

### Root Configuration

```yaml
kafka: KafkaConfig
schema_registry: SchemaRegistryConfig
inference: InferenceConfig
performance: PerformanceConfig
topic_filter: TopicFilterConfig
```

### KafkaConfig

```yaml
kafka:
  bootstrap_servers: string                    # Required: Kafka bootstrap servers
  auto_offset_reset: string                    # Default: "latest"
  session_timeout_ms: integer                  # Default: 30000
  heartbeat_interval_ms: integer               # Default: 10000
  security_protocol: string                    # Default: "PLAINTEXT"
  sasl_mechanism: string                       # Default: "PLAIN"
  sasl_username: string                        # Optional
  sasl_password: string                        # Optional
  ssl_ca_location: string                      # Optional
  ssl_certificate_location: string             # Optional
  ssl_key_location: string                     # Optional
  cloud_api_key: string                        # Optional: Schema Inference Cloud API key
  cloud_api_secret: string                     # Optional: Schema Inference Cloud API secret
```

### SchemaRegistryConfig

```yaml
schema_registry:
  url: string                                  # Required: Schema Registry URL
  verify_ssl: boolean                          # Default: true
  auth: object                                 # Optional: Authentication config
  compatibility: string                        # Default: "NONE"
  subject_name_strategy: string                # Default: "TopicNameStrategy"
  cloud_api_key: string                        # Optional: Schema Inference Cloud API key
  cloud_api_secret: string                     # Optional: Schema Inference Cloud API secret
```

### InferenceConfig

```yaml
inference:
  max_messages: integer                        # Default: 50
  timeout: integer                             # Default: 30
  max_depth: integer                           # Default: 5
  confidence_threshold: float                  # Default: 0.8
  auto_detect_format: boolean                  # Default: true
  forced_data_format: string                   # Optional: Force specific format
```

### PerformanceConfig

```yaml
performance:
  background: boolean                          # Default: false
  max_workers: integer                         # Default: 4
  batch_size: integer                          # Default: 100
  memory_limit_mb: integer                     # Default: 512
  enable_caching: boolean                      # Default: true
  cache_ttl: integer                           # Default: 3600
  show_progress: boolean                       # Default: true
  verbose_logging: boolean                     # Default: false
```

### TopicFilterConfig

```yaml
topic_filter:
  internal_prefix: string                      # Default: "_"
  exclude_internal: boolean                    # Default: true
  additional_exclude_prefixes: array           # Default: []
  include_patterns: array                      # Default: []
```

---

## Python API

### Core Classes

#### SchemaInferrer

Main class for schema inference operations.

```python
class SchemaInferrer:
    def __init__(
        self,
        max_depth: int = 5,
        confidence_threshold: float = 0.8,
        array_handling: str = "union",
        null_handling: str = "optional"
    ):
        """
        Initialize schema inferrer.
        
        Args:
            max_depth: Maximum nesting depth for analysis
            confidence_threshold: Minimum confidence for type detection
            array_handling: Strategy for array type detection
            null_handling: Strategy for null value handling
        """
    
    def infer_schema(
        self,
        data: List[Dict[str, Any]],
        name: str
    ) -> InferredSchema:
        """
        Infer schema from data.
        
        Args:
            data: List of data records
            name: Schema name
            
        Returns:
            InferredSchema object
        """
    
    def process_topics_parallel(
        self,
        topic_messages: Dict[str, List[Tuple[Optional[bytes], bytes]]],
        output_format: str,
        output_path: Optional[str] = None,
        output_dir: Optional[str] = None,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """
        Process multiple topics in parallel.
        
        Args:
            topic_messages: Dictionary mapping topic names to messages
            output_format: Output format (avro, protobuf, json)
            output_path: Single output file path
            output_dir: Output directory for multiple topics
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary with processing results
        """
```

#### TopicDiscovery

Class for discovering and filtering Kafka topics.

```python
class TopicDiscovery:
    def __init__(self, config: Config):
        """
        Initialize topic discovery.
        
        Args:
            config: Configuration object
        """
    
    def discover_topics(
        self,
        topic_name: Optional[str] = None,
        topics: Optional[List[str]] = None,
        topic_prefix: Optional[str] = None,
        topic_pattern: Optional[str] = None,
        exclude_internal: Optional[bool] = None,
        internal_prefix: Optional[str] = None,
        additional_exclude_prefixes: Optional[List[str]] = None,
        include_patterns: Optional[List[str]] = None,
        show_metadata: bool = False
    ) -> List[str]:
        """
        Discover topics based on criteria.
        
        Args:
            topic_name: Single topic name
            topics: List of topic names
            topic_prefix: Topic prefix to match
            topic_pattern: Regex pattern for topics
            exclude_internal: Exclude internal topics
            internal_prefix: Custom internal topic prefix
            additional_exclude_prefixes: Additional prefixes to exclude
            include_patterns: Patterns to include
            show_metadata: Show topic metadata
            
        Returns:
            List of discovered topic names
        """
```

#### KafkaConsumer

Class for Kafka consumer operations.

```python
class KafkaConsumer:
    def __init__(self, config: Config):
        """
        Initialize Kafka consumer.
        
        Args:
            config: Configuration object
        """
    
    def list_topics(self, timeout: float = 10.0) -> Dict[str, Any]:
        """
        List available topics.
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            Dictionary of topic metadata
        """
    
    def get_watermark_offsets(
        self,
        partition: Any,
        timeout: float = 10.0
    ) -> Tuple[int, int]:
        """
        Get watermark offsets for partition.
        
        Args:
            partition: Topic partition
            timeout: Timeout in seconds
            
        Returns:
            Tuple of (low, high) offsets
        """
    
    def close(self) -> None:
        """Close consumer connection."""
```

#### SchemaRegistry

Class for Schema Registry operations.

```python
class SchemaRegistry:
    def __init__(self, config: Config):
        """
        Initialize Schema Registry client.
        
        Args:
            config: Configuration object
        """
    
    def register_schema(
        self,
        subject: str,
        schema_content: str,
        schema_type: str
    ) -> Dict[str, Any]:
        """
        Register schema in Schema Registry.
        
        Args:
            subject: Subject name
            schema_content: Schema content
            schema_type: Schema type (avro, protobuf, json)
            
        Returns:
            Registration response
        """
    
    def get_schema(self, subject: str, version: int = -1) -> Dict[str, Any]:
        """
        Get schema from Schema Registry.
        
        Args:
            subject: Subject name
            version: Schema version (-1 for latest)
            
        Returns:
            Schema information
        """
```

---

## Schema Classes

### InferredSchema

Represents an inferred schema.

```python
class InferredSchema:
    def __init__(
        self,
        name: str,
        fields: List[SchemaField],
        namespace: Optional[str] = None,
        description: Optional[str] = None
    ):
        """
        Initialize inferred schema.
        
        Args:
            name: Schema name
            fields: List of schema fields
            namespace: Schema namespace
            description: Schema description
        """
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary."""
    
    def get_field(self, name: str) -> Optional[SchemaField]:
        """Get field by name."""
    
    def add_field(self, field: SchemaField) -> None:
        """Add field to schema."""
```

### SchemaField

Represents a schema field.

```python
class SchemaField:
    def __init__(
        self,
        name: str,
        field_type: FieldType,
        required: bool = True,
        description: Optional[str] = None,
        examples: Optional[Set[Any]] = None,
        default_value: Optional[Any] = None
    ):
        """
        Initialize schema field.
        
        Args:
            name: Field name
            field_type: Field type
            required: Whether field is required
            description: Field description
            examples: Example values
            default_value: Default value
        """
```

### FieldType

Represents a field type.

```python
class FieldType:
    def __init__(
        self,
        name: str,
        nullable: bool = False,
        array: bool = False
    ):
        """
        Initialize field type.
        
        Args:
            name: Type name
            nullable: Whether type is nullable
            array: Whether type is an array
        """
    
    def __eq__(self, other: 'FieldType') -> bool:
        """Check equality with another field type."""
    
    def __hash__(self) -> int:
        """Get hash of field type."""
```

---

## Generator Classes

### BaseSchemaGenerator

Base class for schema generators.

```python
class BaseSchemaGenerator(ABC):
    @abstractmethod
    def generate(self, schema: InferredSchema) -> str:
        """Generate schema content."""
    
    @abstractmethod
    def get_file_extension(self) -> str:
        """Get file extension for schema format."""
```

### JSONSchemaGenerator

Generates JSON Schema format.

```python
class JSONSchemaGenerator(BaseSchemaGenerator):
    def generate(self, schema: InferredSchema) -> str:
        """Generate JSON Schema."""
    
    def get_file_extension(self) -> str:
        """Get file extension: 'json'."""
    
    def _build_nested_properties(self, fields: List[SchemaField]) -> Dict[str, Any]:
        """Build nested object structure."""
    
    def _build_nested_structure(self, nested_fields: List[tuple]) -> Dict[str, Any]:
        """Build nested structure from field paths."""
```

### AvroGenerator

Generates Avro schema format.

```python
class AvroGenerator(BaseSchemaGenerator):
    def generate(self, schema: InferredSchema) -> str:
        """Generate Avro schema."""
    
    def get_file_extension(self) -> str:
        """Get file extension: 'avsc'."""
    
    def _build_nested_avro_fields(self, fields: List[SchemaField]) -> List[Dict[str, Any]]:
        """Build nested record structure."""
    
    def _sanitize_avro_name(self, name: str) -> str:
        """Sanitize name for Avro compatibility."""
```

### ProtobufGenerator

Generates Protobuf schema format.

```python
class ProtobufGenerator(BaseSchemaGenerator):
    def generate(self, schema: InferredSchema) -> str:
        """Generate Protobuf schema."""
    
    def get_file_extension(self) -> str:
        """Get file extension: 'proto'."""
    
    def _build_nested_protobuf_structure(self, fields: List[SchemaField]) -> Dict[str, Any]:
        """Build nested message structure."""
    
    def _sanitize_protobuf_name(self, name: str) -> str:
        """Sanitize name for Protobuf compatibility."""
```

### SchemaGeneratorFactory

Factory for creating schema generators.

```python
class SchemaGeneratorFactory:
    @staticmethod
    def create_generator(format_name: str) -> BaseSchemaGenerator:
        """
        Create schema generator for format.
        
        Args:
            format_name: Format name (json-schema, avro, protobuf)
            
        Returns:
            Schema generator instance
            
        Raises:
            ValueError: If format is not supported
        """
```

---

## Utility Classes

### FormatDetector

Detects data format from messages.

```python
class FormatDetector:
    def __init__(self):
        """Initialize format detector."""
    
    def detect_format(
        self,
        messages: List[Tuple[Optional[bytes], bytes]]
    ) -> Tuple[str, float]:
        """
        Detect format from messages.
        
        Args:
            messages: List of (key, value) message tuples
            
        Returns:
            Tuple of (format_name, confidence)
        """
```

### ParserFactory

Factory for creating format parsers.

```python
class ParserFactory:
    @staticmethod
    def create_parser(format_name: str) -> BaseParser:
        """
        Create parser for format.
        
        Args:
            format_name: Format name (json, csv, key-value, raw-text)
            
        Returns:
            Parser instance
        """
```

### AuthenticationManager

Manages authentication for Kafka and Schema Registry.

```python
class AuthenticationManager:
    def __init__(self, config: Config):
        """Initialize authentication manager."""
    
    def configure_kafka_auth(self) -> Dict[str, Any]:
        """Configure Kafka authentication."""
    
    def configure_schema_registry_auth(self) -> Dict[str, Any]:
        """Configure Schema Registry authentication."""
```

---

## Error Classes

### SchemaInferenceError

Base exception for schema inference errors.

```python
class SchemaInferenceError(Exception):
    """Base exception for schema inference errors."""
```

### ConfigurationError

Exception for configuration-related errors.

```python
class ConfigurationError(SchemaInferenceError):
    """Exception for configuration errors."""
```

### KafkaConnectionError

Exception for Kafka connection errors.

```python
class KafkaConnectionError(SchemaInferenceError):
    """Exception for Kafka connection errors."""
```

### SchemaRegistryError

Exception for Schema Registry errors.

```python
class SchemaRegistryError(SchemaInferenceError):
    """Exception for Schema Registry errors."""
```

### FormatDetectionError

Exception for format detection errors.

```python
class FormatDetectionError(SchemaInferenceError):
    """Exception for format detection errors."""
```

### SchemaGenerationError

Exception for schema generation errors.

```python
class SchemaGenerationError(SchemaInferenceError):
    """Exception for schema generation errors."""
```

---

## Usage Examples

### Programmatic Usage

```python
from schema_infer.schemas.inference import SchemaInferrer
from schema_infer.schemas.generators import JSONSchemaGenerator
from schema_infer.config import Config

# Load configuration
config = Config()

# Initialize inferrer
inferrer = SchemaInferrer(max_depth=5)

# Sample data
data = [
    {"userId": "123", "name": "John", "age": 30},
    {"userId": "456", "name": "Jane", "age": 25}
]

# Infer schema
schema = inferrer.infer_schema(data, "user_schema")

# Generate JSON Schema
generator = JSONSchemaGenerator()
json_schema = generator.generate(schema)

print(json_schema)
```

### Custom Configuration

```python
from schema_infer.config import Config, KafkaConfig, SchemaRegistryConfig

# Create custom configuration
config = Config()
config.kafka = KafkaConfig(
    bootstrap_servers="localhost:9092",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username="kafka",
    sasl_password="secret"
)
config.schema_registry = SchemaRegistryConfig(
    url="http://localhost:8081",
    verify_ssl=False
)

# Use configuration
from schema_infer.core.discovery import TopicDiscovery
discovery = TopicDiscovery(config)
topics = discovery.discover_topics(topic_prefix="prod-")
```

### Error Handling

```python
from schema_infer.utils.exceptions import (
    SchemaInferenceError,
    KafkaConnectionError,
    SchemaRegistryError
)

try:
    # Schema inference operations
    schema = inferrer.infer_schema(data, "test_schema")
except KafkaConnectionError as e:
    print(f"Kafka connection failed: {e}")
except SchemaRegistryError as e:
    print(f"Schema Registry error: {e}")
except SchemaInferenceError as e:
    print(f"Schema inference error: {e}")
```

---

This API reference provides comprehensive documentation for all classes, methods, and configuration options available in the Schema Inference Schema Inference Plugin. For more detailed examples and usage patterns, see the [DOCUMENTATION.md](DOCUMENTATION.md) file.
