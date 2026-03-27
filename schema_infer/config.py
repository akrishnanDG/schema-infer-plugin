"""
Configuration management for Schema Inference Plugin
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator


class KafkaConfig(BaseModel):
    """Kafka connection configuration."""

    bootstrap_servers: str = Field(
        default="localhost:9092", description="Kafka bootstrap servers"
    )
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol")
    sasl_mechanism: Optional[str] = Field(default=None, description="SASL mechanism")
    sasl_username: Optional[str] = Field(default=None, description="SASL username")
    sasl_password: Optional[str] = Field(default=None, description="SASL password")
    ssl_ca_location: Optional[str] = Field(
        default=None, description="SSL CA certificate location"
    )
    ssl_certificate_location: Optional[str] = Field(
        default=None, description="SSL certificate location"
    )
    ssl_key_location: Optional[str] = Field(
        default=None, description="SSL key location"
    )
    ssl_key_password: Optional[str] = Field(
        default=None, description="SSL key password"
    )

    # Schema Inference Cloud API Key/Secret
    cloud_api_key: Optional[str] = Field(
        default=None, description="Schema Inference Cloud API key"
    )
    cloud_api_secret: Optional[str] = Field(
        default=None, description="Schema Inference Cloud API secret"
    )

    consumer_group: str = Field(
        default="schema-infer-consumer", description="Consumer group ID"
    )
    auto_offset_reset: str = Field(
        default="earliest", description="Auto offset reset policy"
    )
    enable_auto_commit: bool = Field(default=False, description="Enable auto commit")
    session_timeout_ms: int = Field(
        default=30000, description="Session timeout in milliseconds"
    )
    heartbeat_interval_ms: int = Field(
        default=10000, description="Heartbeat interval in milliseconds"
    )


class SchemaRegistryConfig(BaseModel):
    """Schema Registry configuration."""

    url: str = Field(default="http://localhost:8081", description="Schema Registry URL")
    username: Optional[str] = Field(
        default=None, description="Schema Registry username"
    )
    password: Optional[str] = Field(
        default=None, description="Schema Registry password"
    )
    ssl_ca_location: Optional[str] = Field(
        default=None, description="SSL CA certificate location"
    )
    ssl_certificate_location: Optional[str] = Field(
        default=None, description="SSL certificate location"
    )
    ssl_key_location: Optional[str] = Field(
        default=None, description="SSL key location"
    )
    ssl_key_password: Optional[str] = Field(
        default=None, description="SSL key password"
    )
    basic_auth_credentials_source: str = Field(
        default="USER_INFO", description="Basic auth credentials source"
    )

    # Schema Inference Cloud API Key/Secret
    cloud_api_key: Optional[str] = Field(
        default=None, description="Schema Inference Cloud API key"
    )
    cloud_api_secret: Optional[str] = Field(
        default=None, description="Schema Inference Cloud API secret"
    )

    # Schema compatibility settings
    compatibility: str = Field(
        default="BACKWARD",
        description="Schema compatibility level (NONE, BACKWARD, FORWARD, FULL, BACKWARD_TRANSITIVE, FORWARD_TRANSITIVE, FULL_TRANSITIVE)",
    )

    # Subject name strategy
    subject_name_strategy: str = Field(
        default="TopicNameStrategy",
        description="Subject name strategy (TopicNameStrategy, RecordNameStrategy, TopicRecordNameStrategy)",
    )

    # Schema Registry context
    context: Optional[str] = Field(
        default=None,
        description="Schema Registry context for subject name prefixing (e.g., 'my-context' produces ':.my-context:subject-name')",
    )

    @field_validator("compatibility")
    @classmethod
    def validate_compatibility(cls, v):
        valid_compatibility_levels = {
            "NONE",
            "BACKWARD",
            "FORWARD",
            "FULL",
            "BACKWARD_TRANSITIVE",
            "FORWARD_TRANSITIVE",
            "FULL_TRANSITIVE",
        }
        if v.upper() not in valid_compatibility_levels:
            raise ValueError(
                f"Invalid compatibility level: {v}. Must be one of: {', '.join(valid_compatibility_levels)}"
            )
        return v.upper()

    @field_validator("subject_name_strategy")
    @classmethod
    def validate_subject_name_strategy(cls, v):
        valid_strategies = {
            "TopicNameStrategy",
            "RecordNameStrategy",
            "TopicRecordNameStrategy",
        }
        if v not in valid_strategies:
            raise ValueError(
                f"Invalid subject name strategy: {v}. Must be one of: {', '.join(valid_strategies)}"
            )
        return v


class InferenceConfig(BaseModel):
    """Schema inference configuration."""

    max_messages: int = Field(
        default=50, ge=1, description="Maximum messages to sample"
    )
    timeout: int = Field(default=20, ge=1, description="Consumer timeout in seconds")
    auto_detect_format: bool = Field(
        default=True, description="Auto-detect data format"
    )
    forced_data_format: Optional[str] = Field(
        default=None, description="Force specific data format"
    )
    confidence_threshold: float = Field(
        default=0.8,
        ge=0.0,
        le=1.0,
        description="Confidence threshold for format detection",
    )
    sample_size: int = Field(
        default=100, ge=1, description="Sample size for format detection"
    )
    enable_nested_objects: bool = Field(
        default=True, description="Enable nested object inference"
    )
    max_depth: int = Field(default=10, ge=1, description="Maximum nesting depth")
    array_handling: str = Field(
        default="union", description="Array handling strategy: union, first, all"
    )
    null_handling: str = Field(
        default="optional",
        description="Null handling strategy: optional, required, ignore",
    )

    @field_validator("array_handling")
    @classmethod
    def validate_array_handling(cls, v):
        valid = {"union", "first", "all"}
        if v not in valid:
            raise ValueError(f"array_handling must be one of {valid}, got '{v}'")
        return v

    @field_validator("null_handling")
    @classmethod
    def validate_null_handling(cls, v):
        valid = {"optional", "required", "ignore"}
        if v not in valid:
            raise ValueError(f"null_handling must be one of {valid}, got '{v}'")
        return v


class PerformanceConfig(BaseModel):
    """Performance and optimization configuration."""

    background: bool = Field(default=False, description="Run in background mode")
    max_workers: int = Field(
        default=4, ge=1, description="Maximum number of worker threads"
    )
    batch_size: int = Field(default=100, ge=1, description="Batch size for processing")
    memory_limit_mb: int = Field(default=512, ge=1, description="Memory limit in MB")
    enable_caching: bool = Field(default=True, description="Enable result caching")
    cache_ttl: int = Field(default=3600, description="Cache TTL in seconds")
    show_progress: bool = Field(
        default=True, description="Show progress bars during processing"
    )
    verbose_logging: bool = Field(
        default=False, description="Enable verbose logging for debugging"
    )


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = Field(default="INFO", description="Logging level")
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format",
    )
    file: Optional[str] = Field(default=None, description="Log file path")
    max_size_mb: int = Field(default=10, description="Maximum log file size in MB")
    backup_count: int = Field(default=5, description="Number of backup log files")


class TopicFilterConfig(BaseModel):
    """Topic filtering configuration."""

    internal_prefix: str = Field(
        default="__", description="Prefix for internal topics to exclude"
    )
    exclude_internal: bool = Field(
        default=True, description="Exclude internal topics by default"
    )
    additional_exclude_prefixes: List[str] = Field(
        default_factory=list, description="Additional prefixes to exclude"
    )
    include_patterns: List[str] = Field(
        default_factory=list, description="Patterns to include (overrides exclusions)"
    )


class LiveConfig(BaseModel):
    """Live consumer mode configuration."""

    consumer_group: str = Field(
        default="schema-infer-live",
        description="Stable consumer group ID for offset tracking",
    )
    batch_size: int = Field(
        default=100,
        description="Number of messages to accumulate before re-inferring schema",
    )
    batch_timeout_seconds: float = Field(
        default=60.0,
        description="Maximum seconds to wait for batch_size messages before processing",
    )
    state_dir: Optional[str] = Field(
        default=None,
        description="Directory for persisting incremental schema state (default: ~/.schema-infer/state/)",
    )
    persist_state: bool = Field(
        default=True,
        description="Whether to persist schema state to disk for resume-on-restart",
    )
    initial_offset: str = Field(
        default="latest",
        description="Where to start consuming if no committed offsets exist (earliest/latest)",
    )
    min_records_before_register: int = Field(
        default=10,
        description="Minimum records to process before first schema registration",
    )
    idle_evict_seconds: int = Field(
        default=3600,
        description="Evict idle topic state from memory after this many seconds",
    )
    max_concurrent_registrations: int = Field(
        default=5, description="Max parallel schema registrations (rate-limiting)"
    )
    summary_interval_seconds: int = Field(
        default=60,
        description="Interval for periodic status summary (useful for many topics)",
    )
    topic_discovery_interval_seconds: int = Field(
        default=300,
        description="Re-discover topics matching prefix/pattern every N seconds (0 to disable)",
    )
    on_incompatible: str = Field(
        default="skip",
        description="Behavior when schema is incompatible: skip, log, force, fail",
    )

    @field_validator("initial_offset")
    @classmethod
    def validate_initial_offset(cls, v):
        if v not in ("earliest", "latest"):
            raise ValueError(
                f"initial_offset must be 'earliest' or 'latest', got '{v}'"
            )
        return v

    @field_validator("on_incompatible")
    @classmethod
    def validate_on_incompatible(cls, v):
        valid = {"skip", "log", "force", "fail"}
        if v not in valid:
            raise ValueError(f"on_incompatible must be one of {valid}, got '{v}'")
        return v


class Config(BaseModel):
    """Main configuration class."""

    kafka: KafkaConfig = Field(
        default_factory=KafkaConfig, description="Kafka configuration"
    )
    schema_registry: SchemaRegistryConfig = Field(
        default_factory=SchemaRegistryConfig,
        description="Schema Registry configuration",
    )
    inference: InferenceConfig = Field(
        default_factory=InferenceConfig, description="Inference configuration"
    )
    performance: PerformanceConfig = Field(
        default_factory=PerformanceConfig, description="Performance configuration"
    )
    logging: LoggingConfig = Field(
        default_factory=LoggingConfig, description="Logging configuration"
    )
    topic_filter: TopicFilterConfig = Field(
        default_factory=TopicFilterConfig, description="Topic filtering configuration"
    )
    live: LiveConfig = Field(
        default_factory=LiveConfig, description="Live consumer mode configuration"
    )

    # Convenience fields for backward compatibility and CLI overrides.
    # These are NOT synced via validators (which had side-effect bugs).
    # Instead, load_config() syncs them after construction.
    bootstrap_servers: str = Field(default="localhost:9092")
    schema_registry_url: str = Field(default="http://localhost:8081")
    log_level: str = Field(default="INFO")
    max_messages: int = Field(default=50)
    timeout: int = Field(default=20)
    auto_detect_format: bool = Field(default=True)
    forced_data_format: Optional[str] = Field(default=None)
    background: bool = Field(default=False)

    model_config = {"validate_assignment": True}

    def sync_nested_to_convenience(self) -> None:
        """Sync nested config values up to convenience fields.

        Call after constructing nested configs to keep convenience fields
        consistent. Nested configs are the source of truth.
        """
        self.bootstrap_servers = self.kafka.bootstrap_servers
        self.schema_registry_url = self.schema_registry.url
        self.log_level = self.logging.level
        self.max_messages = self.inference.max_messages
        self.timeout = self.inference.timeout
        self.auto_detect_format = self.inference.auto_detect_format
        self.forced_data_format = self.inference.forced_data_format
        self.background = self.performance.background

    def sync_convenience_to_nested(self) -> None:
        """Sync convenience fields down to nested config objects.

        Call after modifying convenience fields (e.g., from CLI overrides)
        to propagate changes into the nested configs.
        """
        self.kafka.bootstrap_servers = self.bootstrap_servers
        self.schema_registry.url = self.schema_registry_url
        self.logging.level = self.log_level
        self.inference.max_messages = self.max_messages
        self.inference.timeout = self.timeout
        self.inference.auto_detect_format = self.auto_detect_format
        self.inference.forced_data_format = self.forced_data_format
        self.performance.background = self.background


def load_config(config_path: Optional[Path] = None) -> Config:
    """Load configuration from file or environment variables."""

    config_data = {}

    # Load from file if provided
    if config_path and config_path.exists():
        with open(config_path, "r") as f:
            if config_path.suffix.lower() in [".yaml", ".yml"]:
                config_data = yaml.safe_load(f)
            elif config_path.suffix.lower() == ".json":
                import json

                config_data = json.load(f)
            else:
                raise ValueError(
                    f"Unsupported config file format: {config_path.suffix}"
                )

        # Validate top-level config keys
        valid_sections = {
            "kafka",
            "schema_registry",
            "inference",
            "performance",
            "logging",
            "topic_filter",
            "live",
            "bootstrap_servers",
            "schema_registry_url",
            "log_level",
            "max_messages",
            "timeout",
            "auto_detect_format",
            "forced_data_format",
            "background",
        }
        unknown_keys = set(config_data.keys()) - valid_sections
        if unknown_keys:
            import warnings

            warnings.warn(
                f"Unknown config keys will be ignored: {', '.join(sorted(unknown_keys))}"
            )

    # Load from environment variables
    env_config = {}
    for key, value in os.environ.items():
        if key.startswith("SCHEMA_INFER_"):
            # Convert SCHEMA_INFER_KAFKA_BOOTSTRAP_SERVERS to nested dict
            parts = key[13:].lower().split("_", 1)
            if len(parts) == 2:
                section = parts[0]
                field = parts[1].replace("__", ".")  # double underscore for nested
                if section not in env_config:
                    env_config[section] = {}
                if isinstance(env_config[section], dict):
                    env_config[section][field] = value
            else:
                env_config[parts[0]] = value

    # Merge configurations (env takes precedence over file)
    merged_config = {**config_data, **env_config}

    # Create Config object with proper structure
    config = Config()

    # Update nested configurations if they exist in the loaded data
    if "kafka" in merged_config:
        config.kafka = KafkaConfig(**merged_config["kafka"])
    if "schema_registry" in merged_config:
        config.schema_registry = SchemaRegistryConfig(
            **merged_config["schema_registry"]
        )
    if "inference" in merged_config:
        config.inference = InferenceConfig(**merged_config["inference"])
    if "performance" in merged_config:
        config.performance = PerformanceConfig(**merged_config["performance"])
    if "logging" in merged_config:
        config.logging = LoggingConfig(**merged_config["logging"])
    if "topic_filter" in merged_config:
        config.topic_filter = TopicFilterConfig(**merged_config["topic_filter"])
    if "live" in merged_config:
        config.live = LiveConfig(**merged_config["live"])

    # First: sync nested configs up to convenience fields so they reflect
    # whatever was loaded from the YAML (nested configs are source of truth)
    config.sync_nested_to_convenience()

    # Then: apply any top-level convenience overrides from the config file
    # (these take precedence over nested values when explicitly set)
    has_convenience_override = False
    if "bootstrap_servers" in merged_config:
        config.bootstrap_servers = merged_config["bootstrap_servers"]
        has_convenience_override = True
    if "schema_registry_url" in merged_config:
        config.schema_registry_url = merged_config["schema_registry_url"]
        has_convenience_override = True
    if "log_level" in merged_config:
        config.log_level = merged_config["log_level"]
        has_convenience_override = True
    if "max_messages" in merged_config:
        config.max_messages = merged_config["max_messages"]
        has_convenience_override = True
    if "timeout" in merged_config:
        config.timeout = merged_config["timeout"]
        has_convenience_override = True
    if "auto_detect_format" in merged_config:
        config.auto_detect_format = merged_config["auto_detect_format"]
        has_convenience_override = True
    if "forced_data_format" in merged_config:
        config.forced_data_format = merged_config["forced_data_format"]
        has_convenience_override = True
    if "background" in merged_config:
        config.background = merged_config["background"]
        has_convenience_override = True

    # Only sync convenience → nested if convenience overrides were explicitly set
    if has_convenience_override:
        config.sync_convenience_to_nested()

    return config


def save_config(config: Config, config_path: Path) -> None:
    """Save configuration to file (secrets are redacted)."""

    config_path.parent.mkdir(parents=True, exist_ok=True)

    config_dict = config.model_dump()

    # Redact sensitive fields
    sensitive_fields = ["password", "secret", "ssl_key_password"]

    def redact(d):
        for key, value in d.items():
            if isinstance(value, dict):
                redact(value)
            elif any(s in key.lower() for s in sensitive_fields) and value is not None:
                d[key] = "***REDACTED***"

    redact(config_dict)

    with open(config_path, "w") as f:
        if config_path.suffix.lower() in [".yaml", ".yml"]:
            yaml.dump(config_dict, f, default_flow_style=False, indent=2)
        elif config_path.suffix.lower() == ".json":
            import json

            json.dump(config_dict, f, indent=2)
        else:
            raise ValueError(f"Unsupported config file format: {config_path.suffix}")


def get_default_config_path() -> Path:
    """Get default configuration file path."""

    # Try different locations in order of preference
    locations = [
        Path.cwd() / "schema-infer.yaml",
        Path.cwd() / "schema-infer.yml",
        Path.cwd() / "schema-infer.json",
        Path.home() / ".config" / "schema-infer" / "config.yaml",
        Path.home() / ".schema-infer.yaml",
    ]

    for location in locations:
        if location.exists():
            return location

    # Return the first location as default
    return locations[0]
