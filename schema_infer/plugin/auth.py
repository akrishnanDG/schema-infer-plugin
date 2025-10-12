"""
Authentication management for Schema Inference Platform and Cloud
"""

import os
from typing import Any, Dict, Optional

from ..config import Config
from ..utils.logger import get_logger


class AuthenticationManager:
    """Manages authentication for Kafka and Schema Registry across Schema Inference Platform and Cloud."""
    
    def __init__(self, config: Config):
        """
        Initialize authentication manager.
        
        Args:
            config: Configuration object
        """
        
        self.config = config
        self.logger = get_logger(__name__)
    
    def detect_environment(self) -> str:
        """
        Detect if we're connecting to Schema Inference Cloud or Platform.
        
        Returns:
            'cloud' or 'platform'
        """
        
        bootstrap_servers = self.config.kafka.bootstrap_servers.lower()
        schema_registry_url = self.config.schema_registry.url.lower()
        
        # Check for Schema Inference Cloud indicators
        cloud_indicators = [
            'schema-infer.cloud',
            'pkc-',
            'psrc-',
            'lkc-',
            'lsrc-',
            'gcp.schema-infer.cloud',
            'aws.schema-infer.cloud',
            'azure.schema-infer.cloud'
        ]
        
        for indicator in cloud_indicators:
            if indicator in bootstrap_servers or indicator in schema_registry_url:
                return 'cloud'
        
        return 'platform'
    
    def configure_kafka_auth(self) -> Dict[str, Any]:
        """
        Configure Kafka authentication based on environment.
        
        Returns:
            Kafka configuration dictionary
        """
        
        environment = self.detect_environment()
        kafka_config = {}
        
        if environment == 'cloud':
            kafka_config.update(self._configure_cloud_kafka_auth())
        else:
            kafka_config.update(self._configure_platform_kafka_auth())
        
        return kafka_config
    
    def configure_schema_registry_auth(self) -> Dict[str, Any]:
        """
        Configure Schema Registry authentication based on environment.
        
        Returns:
            Schema Registry configuration dictionary
        """
        
        environment = self.detect_environment()
        sr_config = {}
        
        if environment == 'cloud':
            sr_config.update(self._configure_cloud_sr_auth())
        else:
            sr_config.update(self._configure_platform_sr_auth())
        
        return sr_config
    
    def _configure_cloud_kafka_auth(self) -> Dict[str, Any]:
        """Configure authentication for Schema Inference Cloud Kafka."""
        
        config = {}
        
        # Schema Inference Cloud always uses SASL_SSL
        config['security.protocol'] = 'SASL_SSL'
        config['sasl.mechanism'] = 'PLAIN'
        
        # Get credentials from environment or config
        api_key = self._get_cloud_api_key()
        api_secret = self._get_cloud_api_secret()
        
        if api_key and api_secret:
            config['sasl.username'] = api_key
            config['sasl.password'] = api_secret
        else:
            self.logger.warning("Schema Inference Cloud API key/secret not found in YAML config file")
        
        return config
    
    def _configure_platform_kafka_auth(self) -> Dict[str, Any]:
        """Configure authentication for Schema Inference Platform Kafka."""
        
        config = {}
        
        # Use configured security protocol or default to PLAINTEXT
        security_protocol = self.config.kafka.security_protocol or 'PLAINTEXT'
        config['security.protocol'] = security_protocol
        
        if security_protocol == 'SASL_SSL':
            config['sasl.mechanism'] = self.config.kafka.sasl_mechanism or 'PLAIN'
            config['sasl.username'] = self.config.kafka.sasl_username
            config['sasl.password'] = self.config.kafka.sasl_password
            
            # SSL configuration
            if self.config.kafka.ssl_ca_location:
                config['ssl.ca.location'] = self.config.kafka.ssl_ca_location
            if self.config.kafka.ssl_certificate_location:
                config['ssl.certificate.location'] = self.config.kafka.ssl_certificate_location
            if self.config.kafka.ssl_key_location:
                config['ssl.key.location'] = self.config.kafka.ssl_key_location
        
        elif security_protocol == 'SASL_PLAINTEXT':
            config['sasl.mechanism'] = self.config.kafka.sasl_mechanism or 'PLAIN'
            config['sasl.username'] = self.config.kafka.sasl_username
            config['sasl.password'] = self.config.kafka.sasl_password
        
        elif security_protocol == 'SSL':
            if self.config.kafka.ssl_ca_location:
                config['ssl.ca.location'] = self.config.kafka.ssl_ca_location
            if self.config.kafka.ssl_certificate_location:
                config['ssl.certificate.location'] = self.config.kafka.ssl_certificate_location
            if self.config.kafka.ssl_key_location:
                config['ssl.key.location'] = self.config.kafka.ssl_key_location
        
        return config
    
    def _configure_cloud_sr_auth(self) -> Dict[str, Any]:
        """Configure authentication for Schema Inference Cloud Schema Registry."""
        
        config = {}
        
        # Get credentials from environment or config
        api_key = self._get_cloud_api_key()
        api_secret = self._get_cloud_api_secret()
        
        if api_key and api_secret:
            config['username'] = api_key
            config['password'] = api_secret
        else:
            self.logger.warning("Schema Inference Cloud API key/secret not found for Schema Registry")
        
        return config
    
    def _configure_platform_sr_auth(self) -> Dict[str, Any]:
        """Configure authentication for Schema Inference Platform Schema Registry."""
        
        config = {}
        
        # Use configured credentials if available
        if self.config.schema_registry.username and self.config.schema_registry.password:
            config['username'] = self.config.schema_registry.username
            config['password'] = self.config.schema_registry.password
        
        # SSL configuration
        if self.config.schema_registry.ssl_ca_location:
            config['ssl_ca_location'] = self.config.schema_registry.ssl_ca_location
        if self.config.schema_registry.ssl_certificate_location:
            config['ssl_certificate_location'] = self.config.schema_registry.ssl_certificate_location
        if self.config.schema_registry.ssl_key_location:
            config['ssl_key_location'] = self.config.schema_registry.ssl_key_location
        
        return config
    
    def _get_cloud_api_key(self) -> Optional[str]:
        """Get Schema Inference Cloud API key from YAML config only."""
        
        # Check Kafka config first
        if hasattr(self.config.kafka, 'cloud_api_key') and self.config.kafka.cloud_api_key:
            return self.config.kafka.cloud_api_key
        
        # Check Schema Registry config as fallback
        if hasattr(self.config.schema_registry, 'cloud_api_key') and self.config.schema_registry.cloud_api_key:
            return self.config.schema_registry.cloud_api_key
        
        return None
    
    def _get_cloud_api_secret(self) -> Optional[str]:
        """Get Schema Inference Cloud API secret from YAML config only."""
        
        # Check Kafka config first
        if hasattr(self.config.kafka, 'cloud_api_secret') and self.config.kafka.cloud_api_secret:
            return self.config.kafka.cloud_api_secret
        
        # Check Schema Registry config as fallback
        if hasattr(self.config.schema_registry, 'cloud_api_secret') and self.config.schema_registry.cloud_api_secret:
            return self.config.schema_registry.cloud_api_secret
        
        return None
    
    def get_authentication_info(self) -> Dict[str, Any]:
        """
        Get authentication information for debugging.
        
        Returns:
            Dictionary with authentication details (without secrets)
        """
        
        environment = self.detect_environment()
        
        info = {
            'environment': environment,
            'kafka_bootstrap_servers': self.config.kafka.bootstrap_servers,
            'schema_registry_url': self.config.schema_registry.url,
        }
        
        if environment == 'cloud':
            api_key = self._get_cloud_api_key()
            info['has_api_key'] = bool(api_key)
            info['api_key_prefix'] = api_key[:8] + '...' if api_key else None
        else:
            info['security_protocol'] = self.config.kafka.security_protocol
            info['sasl_mechanism'] = self.config.kafka.sasl_mechanism
            info['has_username'] = bool(self.config.kafka.sasl_username)
            info['has_ssl_ca'] = bool(self.config.kafka.ssl_ca_location)
        
        return info
