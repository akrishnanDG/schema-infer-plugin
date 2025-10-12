"""
Configuration management for Schema Inference Plugin
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, validator


class KafkaConfig(BaseModel):
    """Kafka connection configuration."""
    
    bootstrap_servers: str = Field(default="localhost:9092", description="Kafka bootstrap servers")
    security_protocol: str = Field(default="PLAINTEXT", description="Security protocol")
    sasl_mechanism: Optional[str] = Field(default=None, description="SASL mechanism")
    sasl_username: Optional[str] = Field(default=None, description="SASL username")
    sasl_password: Optional[str] = Field(default=None, description="SASL password")
    ssl_ca_location: Optional[str] = Field(default=None, description="SSL CA certificate location")
    ssl_certificate_location: Optional[str] = Field(default=None, description="SSL certificate location")
    ssl_key_location: Optional[str] = Field(default=None, description="SSL key location")
    ssl_key_password: Optional[str] = Field(default=None, description="SSL key password")
    
    # Schema Inference Cloud API Key/Secret
    cloud_api_key: Optional[str] = Field(default=None, description="Schema Inference Cloud API key")
    cloud_api_secret: Optional[str] = Field(default=None, description="Schema Inference Cloud API secret")
    
    consumer_group: str = Field(default="schema-infer-consumer", description="Consumer group ID")
    auto_offset_reset: str = Field(default="earliest", description="Auto offset reset policy")
    enable_auto_commit: bool = Field(default=True, description="Enable auto commit")
    session_timeout_ms: int = Field(default=30000, description="Session timeout in milliseconds")
    heartbeat_interval_ms: int = Field(default=10000, description="Heartbeat interval in milliseconds")


class SchemaRegistryConfig(BaseModel):
    """Schema Registry configuration."""
    
    url: str = Field(default="http://localhost:8081", description="Schema Registry URL")
    username: Optional[str] = Field(default=None, description="Schema Registry username")
    password: Optional[str] = Field(default=None, description="Schema Registry password")
    ssl_ca_location: Optional[str] = Field(default=None, description="SSL CA certificate location")
    ssl_certificate_location: Optional[str] = Field(default=None, description="SSL certificate location")
    ssl_key_location: Optional[str] = Field(default=None, description="SSL key location")
    ssl_key_password: Optional[str] = Field(default=None, description="SSL key password")
    basic_auth_credentials_source: str = Field(default="USER_INFO", description="Basic auth credentials source")
    
    # Schema Inference Cloud API Key/Secret
    cloud_api_key: Optional[str] = Field(default=None, description="Schema Inference Cloud API key")
    cloud_api_secret: Optional[str] = Field(default=None, description="Schema Inference Cloud API secret")
    
    # Schema compatibility settings
    compatibility: str = Field(default="NONE", description="Schema compatibility level (NONE, BACKWARD, FORWARD, FULL, BACKWARD_TRANSITIVE, FORWARD_TRANSITIVE, FULL_TRANSITIVE)")
    
    # Subject name strategy
    subject_name_strategy: str = Field(default="TopicNameStrategy", description="Subject name strategy (TopicNameStrategy, RecordNameStrategy, TopicRecordNameStrategy)")
    
    @validator('compatibility')
    def validate_compatibility(cls, v):
        valid_compatibility_levels = {
            'NONE', 'BACKWARD', 'FORWARD', 'FULL', 
            'BACKWARD_TRANSITIVE', 'FORWARD_TRANSITIVE', 'FULL_TRANSITIVE'
        }
        if v.upper() not in valid_compatibility_levels:
            raise ValueError(f"Invalid compatibility level: {v}. Must be one of: {', '.join(valid_compatibility_levels)}")
        return v.upper()
    
    @validator('subject_name_strategy')
    def validate_subject_name_strategy(cls, v):
        valid_strategies = {
            'TopicNameStrategy', 'RecordNameStrategy', 'TopicRecordNameStrategy'
        }
        if v not in valid_strategies:
            raise ValueError(f"Invalid subject name strategy: {v}. Must be one of: {', '.join(valid_strategies)}")
        return v


class InferenceConfig(BaseModel):
    """Schema inference configuration."""
    
    max_messages: int = Field(default=50, description="Maximum messages to sample")
    timeout: int = Field(default=20, description="Consumer timeout in seconds")
    auto_detect_format: bool = Field(default=True, description="Auto-detect data format")
    forced_data_format: Optional[str] = Field(default=None, description="Force specific data format")
    confidence_threshold: float = Field(default=0.8, description="Confidence threshold for format detection")
    sample_size: int = Field(default=100, description="Sample size for format detection")
    enable_nested_objects: bool = Field(default=True, description="Enable nested object inference")
    max_depth: int = Field(default=10, description="Maximum nesting depth")
    array_handling: str = Field(default="union", description="Array handling strategy: union, first, all")
    null_handling: str = Field(default="optional", description="Null handling strategy: optional, required, ignore")


class PerformanceConfig(BaseModel):
    """Performance and optimization configuration."""
    
    background: bool = Field(default=False, description="Run in background mode")
    max_workers: int = Field(default=4, description="Maximum number of worker threads")
    batch_size: int = Field(default=100, description="Batch size for processing")
    memory_limit_mb: int = Field(default=512, description="Memory limit in MB")
    enable_caching: bool = Field(default=True, description="Enable result caching")
    cache_ttl: int = Field(default=3600, description="Cache TTL in seconds")
    show_progress: bool = Field(default=True, description="Show progress bars during processing")
    verbose_logging: bool = Field(default=False, description="Enable verbose logging for debugging")


class LoggingConfig(BaseModel):
    """Logging configuration."""
    
    level: str = Field(default="INFO", description="Logging level")
    format: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s", description="Log format")
    file: Optional[str] = Field(default=None, description="Log file path")
    max_size_mb: int = Field(default=10, description="Maximum log file size in MB")
    backup_count: int = Field(default=5, description="Number of backup log files")


class TopicFilterConfig(BaseModel):
    """Topic filtering configuration."""
    
    internal_prefix: str = Field(default="__", description="Prefix for internal topics to exclude")
    exclude_internal: bool = Field(default=True, description="Exclude internal topics by default")
    additional_exclude_prefixes: List[str] = Field(default_factory=list, description="Additional prefixes to exclude")
    include_patterns: List[str] = Field(default_factory=list, description="Patterns to include (overrides exclusions)")


class Config(BaseModel):
    """Main configuration class."""
    
    kafka: KafkaConfig = Field(default_factory=KafkaConfig, description="Kafka configuration")
    schema_registry: SchemaRegistryConfig = Field(default_factory=SchemaRegistryConfig, description="Schema Registry configuration")
    inference: InferenceConfig = Field(default_factory=InferenceConfig, description="Inference configuration")
    performance: PerformanceConfig = Field(default_factory=PerformanceConfig, description="Performance configuration")
    logging: LoggingConfig = Field(default_factory=LoggingConfig, description="Logging configuration")
    topic_filter: TopicFilterConfig = Field(default_factory=TopicFilterConfig, description="Topic filtering configuration")
    
    # Convenience properties for backward compatibility
    bootstrap_servers: str = Field(default="localhost:9092")
    schema_registry_url: str = Field(default="http://localhost:8081")
    log_level: str = Field(default="INFO")
    max_messages: int = Field(default=50)
    timeout: int = Field(default=20)
    auto_detect_format: bool = Field(default=True)
    forced_data_format: Optional[str] = Field(default=None)
    background: bool = Field(default=False)
    
    @validator("bootstrap_servers", always=True)
    def sync_bootstrap_servers(cls, v, values):
        """Sync bootstrap_servers with kafka config."""
        if "kafka" in values:
            values["kafka"].bootstrap_servers = v
        return v
    
    @validator("schema_registry_url", always=True)
    def sync_schema_registry_url(cls, v, values):
        """Sync schema_registry_url with schema_registry config."""
        if "schema_registry" in values:
            values["schema_registry"].url = v
        return v
    
    @validator("log_level", always=True)
    def sync_log_level(cls, v, values):
        """Sync log_level with logging config."""
        if "logging" in values:
            values["logging"].level = v
        return v
    
    @validator("max_messages", always=True)
    def sync_max_messages(cls, v, values):
        """Sync max_messages with inference config."""
        if "inference" in values:
            values["inference"].max_messages = v
        return v
    
    @validator("timeout", always=True)
    def sync_timeout(cls, v, values):
        """Sync timeout with inference config."""
        if "inference" in values:
            values["inference"].timeout = v
        return v
    
    @validator("auto_detect_format", always=True)
    def sync_auto_detect_format(cls, v, values):
        """Sync auto_detect_format with inference config."""
        if "inference" in values:
            values["inference"].auto_detect_format = v
        return v
    
    @validator("forced_data_format", always=True)
    def sync_forced_data_format(cls, v, values):
        """Sync forced_data_format with inference config."""
        if "inference" in values:
            values["inference"].forced_data_format = v
        return v
    
    @validator("background", always=True)
    def sync_background(cls, v, values):
        """Sync background with performance config."""
        if "performance" in values:
            values["performance"].background = v
        return v
    
    class Config:
        """Pydantic configuration."""
        env_prefix = "SCHEMA_INFER_"
        case_sensitive = False
        validate_assignment = True


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
                raise ValueError(f"Unsupported config file format: {config_path.suffix}")
    
    # Load from environment variables
    env_config = {}
    for key, value in os.environ.items():
        if key.startswith("SCHEMA_INFER_"):
            # Convert SCHEMA_INFER_KAFKA_BOOTSTRAP_SERVERS to kafka.bootstrap_servers
            config_key = key[13:].lower().replace("_", ".")
            env_config[config_key] = value
    
    # Merge configurations (file takes precedence over env)
    merged_config = {**env_config, **config_data}
    
    # Create Config object with proper structure
    config = Config()
    
    # Update nested configurations if they exist in the loaded data
    if "kafka" in merged_config:
        config.kafka = KafkaConfig(**merged_config["kafka"])
    if "schema_registry" in merged_config:
        config.schema_registry = SchemaRegistryConfig(**merged_config["schema_registry"])
    if "inference" in merged_config:
        config.inference = InferenceConfig(**merged_config["inference"])
    if "performance" in merged_config:
        config.performance = PerformanceConfig(**merged_config["performance"])
    if "logging" in merged_config:
        config.logging = LoggingConfig(**merged_config["logging"])
    if "topic_filter" in merged_config:
        config.topic_filter = TopicFilterConfig(**merged_config["topic_filter"])
    
    # Update convenience properties
    if "bootstrap_servers" in merged_config:
        config.bootstrap_servers = merged_config["bootstrap_servers"]
    if "schema_registry_url" in merged_config:
        config.schema_registry_url = merged_config["schema_registry_url"]
    if "log_level" in merged_config:
        config.log_level = merged_config["log_level"]
    if "max_messages" in merged_config:
        config.max_messages = merged_config["max_messages"]
    if "timeout" in merged_config:
        config.timeout = merged_config["timeout"]
    if "auto_detect_format" in merged_config:
        config.auto_detect_format = merged_config["auto_detect_format"]
    if "forced_data_format" in merged_config:
        config.forced_data_format = merged_config["forced_data_format"]
    if "background" in merged_config:
        config.background = merged_config["background"]
    
    return config


def save_config(config: Config, config_path: Path) -> None:
    """Save configuration to file."""
    
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(config_path, "w") as f:
        if config_path.suffix.lower() in [".yaml", ".yml"]:
            yaml.dump(config.dict(), f, default_flow_style=False, indent=2)
        elif config_path.suffix.lower() == ".json":
            import json
            json.dump(config.dict(), f, indent=2)
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
