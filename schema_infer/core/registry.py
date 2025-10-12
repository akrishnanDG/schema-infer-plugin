"""
Schema Registry integration for Schema Inference Plugin
"""

import json
import requests
from typing import Any, Dict, List, Optional

from ..config import Config
from ..utils.exceptions import SchemaRegistryError
from ..utils.logger import get_logger
from ..utils.validators import validate_schema_registry_url


class SchemaRegistry:
    """Client for Schema Inference Schema Registry."""
    
    def __init__(self, config: Config):
        """
        Initialize Schema Registry client.
        
        Args:
            config: Configuration object
        """
        
        self.config = config
        self.logger = get_logger(__name__)
        self.base_url = config.schema_registry.url.rstrip('/')
        
        # Validate URL
        validate_schema_registry_url(self.base_url)
        
        # Setup authentication
        self.auth = None
        if config.schema_registry.username and config.schema_registry.password:
            self.auth = (config.schema_registry.username, config.schema_registry.password)
        
        # Setup SSL configuration
        self.verify_ssl = True
        self.cert = None
        
        if config.schema_registry.ssl_certificate_location and config.schema_registry.ssl_key_location:
            self.cert = (config.schema_registry.ssl_certificate_location, config.schema_registry.ssl_key_location)
        
        if config.schema_registry.ssl_ca_location:
            self.verify_ssl = config.schema_registry.ssl_ca_location
        
        self.logger.info(f"Initialized Schema Registry client for {self.base_url}")
        
        # Test connection on initialization
        self._test_connection()
    
    def register_schema(
        self, 
        topic_name: str, 
        schema_content: str, 
        schema_format: str,
        schema_type: str = "AVRO"
    ) -> int:
        """
        Register a schema in the Schema Registry using topic name strategy.
        
        Args:
            topic_name: Kafka topic name
            schema_content: Schema content as string
            schema_format: Schema format (avro, protobuf, json-schema)
            schema_type: Schema type for registry (AVRO, PROTOBUF, JSON)
            
        Returns:
            Schema ID
        """
        
        try:
            # Generate subject name based on strategy
            subject_name = self._generate_subject_name(topic_name, schema_format)
            
            # Map format to registry type
            registry_type = self._map_format_to_registry_type(schema_format)
            
            # Prepare schema data
            schema_data = {
                "schema": schema_content,
                "schemaType": registry_type
            }
            
            # Set compatibility level if not NONE
            if self.config.schema_registry.compatibility != "NONE":
                self._set_subject_compatibility(subject_name, self.config.schema_registry.compatibility)
            
            # Register schema
            url = f"{self.base_url}/subjects/{subject_name}/versions"
            
            self.logger.info(f"Registering schema for topic '{topic_name}' with subject '{subject_name}' using {self.config.schema_registry.subject_name_strategy} strategy")
            
            response = requests.post(
                url,
                json=schema_data,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
            )
            
            response.raise_for_status()
            
            result = response.json()
            schema_id = result.get("id")
            
            if schema_id is None:
                raise SchemaRegistryError("No schema ID returned from registry")
            
            self.logger.info(f"Successfully registered schema with ID: {schema_id}")
            return schema_id
            
        except requests.exceptions.RequestException as e:
            error_msg = str(e)
            if "nodename nor servname provided" in error_msg or "NXDOMAIN" in error_msg:
                self.logger.error(f"Schema Registry connection failed - DNS resolution error. Please check your Schema Registry URL: {self.base_url}")
                raise SchemaRegistryError(f"Schema Registry URL not accessible: {self.base_url}. Please verify the URL in your configuration.")
            elif hasattr(e, 'response') and e.response is not None:
                try:
                    error_details = e.response.json()
                    self.logger.error(f"Schema Registry error details: {error_details}")
                    raise SchemaRegistryError(f"Schema Registry error: {error_details}")
                except:
                    self.logger.error(f"Failed to register schema: {e}")
                    raise SchemaRegistryError(f"Failed to register schema: {e}")
            else:
                self.logger.error(f"Failed to register schema: {e}")
                raise SchemaRegistryError(f"Failed to register schema: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error registering schema: {e}")
            raise SchemaRegistryError(f"Unexpected error: {e}")
    
    def get_schema(self, schema_id: int) -> Dict[str, Any]:
        """
        Get a schema by ID.
        
        Args:
            schema_id: Schema ID
            
        Returns:
            Schema information
        """
        
        try:
            url = f"{self.base_url}/schemas/ids/{schema_id}"
            
            response = requests.get(
                url,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get schema {schema_id}: {e}")
            raise SchemaRegistryError(f"Failed to get schema: {e}")
    
    def get_subject_versions(self, subject: str) -> List[Dict[str, Any]]:
        """
        Get all versions of a subject.
        
        Args:
            subject: Subject name
            
        Returns:
            List of version information
        """
        
        try:
            url = f"{self.base_url}/subjects/{subject}/versions"
            
            response = requests.get(
                url,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get subject versions for {subject}: {e}")
            raise SchemaRegistryError(f"Failed to get subject versions: {e}")
    
    def get_latest_schema(self, subject: str) -> Dict[str, Any]:
        """
        Get the latest schema for a subject.
        
        Args:
            subject: Subject name
            
        Returns:
            Latest schema information
        """
        
        try:
            url = f"{self.base_url}/subjects/{subject}/versions/latest"
            
            response = requests.get(
                url,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get latest schema for {subject}: {e}")
            raise SchemaRegistryError(f"Failed to get latest schema: {e}")
    
    def list_subjects(self) -> List[str]:
        """
        List all subjects in the registry.
        
        Returns:
            List of subject names
        """
        
        try:
            url = f"{self.base_url}/subjects"
            
            response = requests.get(
                url,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to list subjects: {e}")
            raise SchemaRegistryError(f"Failed to list subjects: {e}")
    
    def delete_subject(self, subject: str, permanent: bool = False) -> List[int]:
        """
        Delete a subject and all its versions.
        
        Args:
            subject: Subject name
            permanent: Whether to permanently delete (soft delete by default)
            
        Returns:
            List of deleted version IDs
        """
        
        try:
            url = f"{self.base_url}/subjects/{subject}"
            if permanent:
                url += "?permanent=true"
            
            response = requests.delete(
                url,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to delete subject {subject}: {e}")
            raise SchemaRegistryError(f"Failed to delete subject: {e}")
    
    def check_compatibility(
        self, 
        subject: str, 
        schema_content: str, 
        version: Optional[str] = None
    ) -> bool:
        """
        Check if a schema is compatible with existing versions.
        
        Args:
            subject: Subject name
            schema_content: Schema content
            version: Version to check against (latest if None)
            
        Returns:
            True if compatible
        """
        
        try:
            if version is None:
                url = f"{self.base_url}/compatibility/subjects/{subject}/versions/latest"
            else:
                url = f"{self.base_url}/compatibility/subjects/{subject}/versions/{version}"
            
            schema_data = {
                "schema": schema_content
            }
            
            response = requests.post(
                url,
                json=schema_data,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
            )
            
            response.raise_for_status()
            result = response.json()
            
            return result.get("is_compatible", False)
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to check compatibility: {e}")
            raise SchemaRegistryError(f"Failed to check compatibility: {e}")
    
    def get_config(self, subject: Optional[str] = None) -> Dict[str, Any]:
        """
        Get configuration for a subject or global configuration.
        
        Args:
            subject: Subject name (None for global config)
            
        Returns:
            Configuration information
        """
        
        try:
            if subject:
                url = f"{self.base_url}/config/{subject}"
            else:
                url = f"{self.base_url}/config"
            
            response = requests.get(
                url,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get config: {e}")
            raise SchemaRegistryError(f"Failed to get config: {e}")
    
    def set_config(self, config_data: Dict[str, Any], subject: Optional[str] = None) -> None:
        """
        Set configuration for a subject or global configuration.
        
        Args:
            config_data: Configuration data
            subject: Subject name (None for global config)
        """
        
        try:
            if subject:
                url = f"{self.base_url}/config/{subject}"
            else:
                url = f"{self.base_url}/config"
            
            response = requests.put(
                url,
                json=config_data,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
            )
            
            response.raise_for_status()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to set config: {e}")
            raise SchemaRegistryError(f"Failed to set config: {e}")
    
    def test_connection(self) -> bool:
        """
        Test connection to Schema Registry.
        
        Returns:
            True if connection is successful
        """
        
        try:
            url = f"{self.base_url}/subjects"
            
            response = requests.get(
                url,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl,
                timeout=10
            )
            
            response.raise_for_status()
            self.logger.info("Schema Registry connection test successful")
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Schema Registry connection test failed: {e}")
            return False
    
    def _map_format_to_registry_type(self, schema_format: str) -> str:
        """
        Map schema format to Schema Registry type.
        
        Args:
            schema_format: Schema format name
            
        Returns:
            Schema Registry type
        """
        
        mapping = {
            "avro": "AVRO",
            "protobuf": "PROTOBUF", 
            "json-schema": "JSON"
        }
        
        return mapping.get(schema_format.lower(), "AVRO")
    
    def _set_subject_compatibility(self, subject: str, compatibility: str) -> None:
        """
        Set compatibility level for a subject.
        
        Args:
            subject: Subject name
            compatibility: Compatibility level (NONE, BACKWARD, FORWARD, FULL, etc.)
        """
        try:
            url = f"{self.base_url}/config/{subject}"
            
            compatibility_data = {
                "compatibility": compatibility
            }
            
            self.logger.info(f"Setting compatibility level for subject '{subject}' to: {compatibility}")
            
            response = requests.put(
                url,
                json=compatibility_data,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}
            )
            
            response.raise_for_status()
            
            self.logger.info(f"Successfully set compatibility level for subject '{subject}' to: {compatibility}")
            
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"Failed to set compatibility level for subject '{subject}': {e}")
            # Don't raise exception - compatibility setting is optional
        except Exception as e:
            self.logger.warning(f"Unexpected error setting compatibility level for subject '{subject}': {e}")
            # Don't raise exception - compatibility setting is optional
    
    def _test_connection(self) -> None:
        """Test Schema Registry connection on initialization."""
        try:
            # Try to get the Schema Registry version/info
            url = f"{self.base_url}/subjects"
            response = requests.get(
                url,
                auth=self.auth,
                cert=self.cert,
                verify=self.verify_ssl,
                timeout=10
            )
            response.raise_for_status()
            self.logger.info("Schema Registry connection test successful")
        except requests.exceptions.RequestException as e:
            # Only log critical connection errors, suppress routine connection failures
            error_msg = str(e)
            if "nodename nor servname provided" in error_msg or "NXDOMAIN" in error_msg:
                # Only show this if verbose logging is enabled
                if hasattr(self, 'config') and hasattr(self.config, 'performance') and self.config.performance.verbose_logging:
                    self.logger.warning(f"Schema Registry connection test failed - DNS resolution error for {self.base_url}")
                    self.logger.warning("Please verify your Schema Registry URL in the configuration")
            # Suppress other connection test failures to keep output clean
        except Exception as e:
            # Suppress unexpected errors during connection test
            pass
    
    def _generate_subject_name(self, topic_name: str, schema_format: str) -> str:
        """
        Generate subject name based on the configured strategy.
        
        Args:
            topic_name: Kafka topic name
            schema_format: Schema format (avro, protobuf, json-schema)
            
        Returns:
            Subject name for Schema Registry
        """
        strategy = self.config.schema_registry.subject_name_strategy
        
        if strategy == "TopicNameStrategy":
            # TopicNameStrategy: <topic-name>-value (for message values)
            # This is the most common strategy for Kafka topics
            return f"{topic_name}-value"
            
        elif strategy == "RecordNameStrategy":
            # RecordNameStrategy: Use the record name from the schema
            # For now, we'll use topic name as fallback since we don't have record name
            self.logger.warning("RecordNameStrategy requires record name from schema - using topic name as fallback")
            return topic_name
            
        elif strategy == "TopicRecordNameStrategy":
            # TopicRecordNameStrategy: <topic-name>-<record-name>
            # For now, we'll use topic name as fallback since we don't have record name
            self.logger.warning("TopicRecordNameStrategy requires record name from schema - using topic name as fallback")
            return topic_name
            
        else:
            # Fallback to TopicNameStrategy
            self.logger.warning(f"Unknown strategy '{strategy}' - falling back to TopicNameStrategy")
            return f"{topic_name}-value"
