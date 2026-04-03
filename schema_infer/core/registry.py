"""
Schema Registry integration for Schema Inference Plugin
"""

import json
import time
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

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
        self.base_url = config.schema_registry.url.rstrip("/")

        # Validate URL
        validate_schema_registry_url(self.base_url)

        # Setup authentication using AuthenticationManager
        self.auth = None
        from ..plugin.auth import AuthenticationManager

        auth_manager = AuthenticationManager(config)
        sr_auth = auth_manager.configure_schema_registry_auth()
        if sr_auth.get("username") and sr_auth.get("password"):
            self.auth = (sr_auth["username"], sr_auth["password"])
        elif config.schema_registry.username and config.schema_registry.password:
            self.auth = (
                config.schema_registry.username,
                config.schema_registry.password,
            )

        # Setup SSL configuration
        self.verify_ssl = True
        self.cert = None

        if (
            config.schema_registry.ssl_certificate_location
            and config.schema_registry.ssl_key_location
        ):
            self.cert = (
                config.schema_registry.ssl_certificate_location,
                config.schema_registry.ssl_key_location,
            )

        if config.schema_registry.ssl_ca_location:
            self.verify_ssl = config.schema_registry.ssl_ca_location

        # Persistent session for connection pooling (reuses TCP/SSL connections)
        self._session = requests.Session()
        if self.auth:
            self._session.auth = self.auth
        if self.cert:
            self._session.cert = self.cert
        self._session.verify = self.verify_ssl

        self.logger.info(f"Initialized Schema Registry client for {self.base_url}")

        # Test connection on initialization
        self._test_connection()

    def _request_with_retry(
        self, method: str, url: str, max_retries: int = 3, **kwargs
    ) -> requests.Response:
        """Execute an HTTP request with retry on transient errors.

        Retries on ConnectionError and Timeout. Non-retryable errors
        (4xx, 5xx HTTP responses) are raised immediately.

        Args:
            method: HTTP method (get, post, put, delete)
            url: Request URL
            max_retries: Number of retry attempts
            **kwargs: Passed to requests.request()

        Returns:
            Response object
        """
        kwargs.setdefault("timeout", (5, 30))

        last_exception = None
        for attempt in range(max_retries):
            try:
                response = self._session.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                last_exception = e
                if attempt < max_retries - 1:
                    wait = 2**attempt
                    self.logger.warning(
                        f"Request failed ({type(e).__name__}), retrying in {wait}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait)
                else:
                    raise
        raise last_exception  # type: ignore[misc]

    def register_schema(
        self,
        topic_name: str,
        schema_content: str,
        schema_format: str,
        schema_type: str = "AVRO",
        references: Optional[List[Dict[str, Any]]] = None,
        subject_override: Optional[str] = None,
        skip_compatibility_set: bool = False,
    ) -> int:
        """
        Register a schema in the Schema Registry using topic name strategy.

        Args:
            topic_name: Kafka topic name
            schema_content: Schema content as string
            schema_format: Schema format (avro, protobuf, json-schema)
            schema_type: Schema type for registry (AVRO, PROTOBUF, JSON)
            references: Optional list of schema references for composition
            subject_override: Optional subject name override (bypasses strategy)
            skip_compatibility_set: If True, skip automatic subject compatibility
                setting (used when caller manages compatibility directly)

        Returns:
            Schema ID
        """

        try:
            subject_name = subject_override or self._generate_subject_name(
                topic_name, schema_format
            )
            registry_type = self._map_format_to_registry_type(schema_format)
            schema_data = {"schema": schema_content, "schemaType": registry_type}
            if references:
                schema_data["references"] = references

            if (
                not skip_compatibility_set
                and self.config.schema_registry.compatibility != "NONE"
            ):
                self._set_subject_compatibility(
                    subject_name, self.config.schema_registry.compatibility
                )

            url = f"{self.base_url}/subjects/{self._encode_subject(subject_name)}/versions"
            self.logger.info(
                f"Registering schema for topic '{topic_name}' with subject '{subject_name}'"
            )

            response = self._request_with_retry(
                "post",
                url,
                json=schema_data,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )

            result = response.json()
            schema_id = result.get("id")

            if schema_id is None:
                raise SchemaRegistryError("No schema ID returned from registry")

            self.logger.info(f"Successfully registered schema with ID: {schema_id}")
            return schema_id

        except requests.exceptions.RequestException as e:
            error_msg = str(e)
            if "nodename nor servname provided" in error_msg or "NXDOMAIN" in error_msg:
                self.logger.error(
                    f"Schema Registry connection failed - DNS resolution error. Please check your Schema Registry URL: {self.base_url}"
                )
                raise SchemaRegistryError(
                    f"Schema Registry URL not accessible: {self.base_url}. Please verify the URL in your configuration."
                )
            elif hasattr(e, "response") and e.response is not None:
                status = e.response.status_code
                try:
                    error_details = e.response.json()
                    error_code = error_details.get("error_code", status)
                    error_message = error_details.get("message", "Unknown error")
                except (ValueError, KeyError):
                    error_code = status
                    error_message = f"HTTP {status}"

                # Differentiated error messages by status code
                if status == 409 or error_code == 40901:
                    self.logger.error(
                        f"Schema incompatible with existing version: {error_message}"
                    )
                    raise SchemaRegistryError(
                        f"Schema incompatible (error {error_code}): {error_message}"
                    )
                elif status == 422 or error_code == 42201:
                    self.logger.error(
                        f"Invalid schema rejected by registry: {error_message}"
                    )
                    raise SchemaRegistryError(
                        f"Invalid schema (error {error_code}): {error_message}"
                    )
                elif status in (401, 403):
                    self.logger.error(
                        f"Authentication/authorization failed: {error_message}"
                    )
                    raise SchemaRegistryError(
                        f"Auth failed (HTTP {status}): {error_message}. Check your API key and secret."
                    )
                else:
                    self.logger.error(
                        f"Schema Registry error {error_code}: {error_message}"
                    )
                    raise SchemaRegistryError(
                        f"Schema Registry error {error_code}: {error_message}"
                    )
            else:
                self.logger.error(f"Failed to register schema: {e}")
                raise SchemaRegistryError(f"Failed to register schema: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error registering schema: {e}")
            raise SchemaRegistryError(f"Unexpected error: {e}")

    def register_multi_event_schemas(
        self,
        topic_name: str,
        schema_contents: Dict[str, str],
        main_schema_content: str,
        schema_format: str,
        skip_compatibility_set: bool = False,
    ) -> Dict[str, int]:
        """
        Register multi-event schemas with references.

        Registers each event type sub-schema as its own subject, then
        registers the main topic schema with references to all sub-schemas.

        Args:
            topic_name: Kafka topic name
            schema_contents: Dict mapping event type to schema JSON string
            main_schema_content: Main oneOf schema JSON string
            schema_format: Schema format (json-schema, avro, protobuf)
            skip_compatibility_set: If True, skip automatic subject compatibility
                setting (used when caller manages compatibility directly)

        Returns:
            Dict mapping subject name to schema ID
        """
        result = {}
        references = []

        # Register sub-schemas first. If a later sub-schema or the main schema
        # fails, already-registered sub-schemas remain in SR (no rollback).
        # This is logged so operators can identify orphaned schemas.
        for event_type, schema_content in sorted(schema_contents.items()):
            subject = f"{topic_name}-{event_type}"
            try:
                schema_id = self.register_schema(
                    topic_name,
                    schema_content,
                    schema_format,
                    subject_override=subject,
                    skip_compatibility_set=skip_compatibility_set,
                )
            except Exception as e:
                if result:
                    self.logger.warning(
                        f"Partial multi-event registration: {len(result)} sub-schemas "
                        f"already registered ({list(result.keys())}), failed on '{subject}': {e}"
                    )
                raise
            result[subject] = schema_id
            # Get actual version after registration for accurate references
            try:
                latest = self.get_latest_schema(subject)
                version = latest.get("version", 1)
            except Exception:
                version = 1
            references.append(
                {
                    "name": subject,
                    "subject": subject,
                    "version": version,
                }
            )
            self.logger.info(
                f"Registered sub-schema '{subject}' with ID {schema_id}, version {version}"
            )

        # Register main schema with references
        main_schema_id = self.register_schema(
            topic_name,
            main_schema_content,
            schema_format,
            references=references,
            skip_compatibility_set=skip_compatibility_set,
        )
        main_subject = self._generate_subject_name(topic_name, schema_format)
        result[main_subject] = main_schema_id
        self.logger.info(
            f"Registered main schema '{main_subject}' with ID {main_schema_id} "
            f"referencing {len(references)} sub-schemas"
        )

        return result

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

            response = self._session.get(url, timeout=(5, 30))

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
            url = f"{self.base_url}/subjects/{self._encode_subject(subject)}/versions"
            response = self._request_with_retry("get", url)
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
            url = f"{self.base_url}/subjects/{self._encode_subject(subject)}/versions/latest"
            response = self._request_with_retry("get", url)
            return response.json()

        except requests.exceptions.RequestException as e:
            # 404 is expected when no schema exists yet — log at debug, not error
            if (
                hasattr(e, "response")
                and e.response is not None
                and e.response.status_code == 404
            ):
                self.logger.debug(f"No existing schema for {subject}")
            else:
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

            response = self._session.get(url, timeout=(5, 30))

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
            url = f"{self.base_url}/subjects/{self._encode_subject(subject)}"
            if permanent:
                url += "?permanent=true"

            response = self._session.delete(url, timeout=(5, 30))

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to delete subject {subject}: {e}")
            raise SchemaRegistryError(f"Failed to delete subject: {e}")

    def check_compatibility(
        self,
        subject: str,
        schema_content: str,
        version: Optional[str] = None,
        schema_format: Optional[str] = None,
    ) -> bool:
        """
        Check if a schema is compatible with existing versions.

        Args:
            subject: Subject name
            schema_content: Schema content
            version: Version to check against (latest if None)
            schema_format: Schema format (avro, protobuf, json-schema)

        Returns:
            True if compatible
        """

        try:
            if version is None:
                url = f"{self.base_url}/compatibility/subjects/{self._encode_subject(subject)}/versions/latest"
            else:
                url = f"{self.base_url}/compatibility/subjects/{self._encode_subject(subject)}/versions/{version}"

            schema_data = {"schema": schema_content}

            # Include schemaType so SR doesn't default to Avro
            if schema_format:
                registry_type = self._map_format_to_registry_type(schema_format)
                schema_data["schemaType"] = registry_type

            response = self._request_with_retry(
                "post",
                url,
                json=schema_data,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )
            result = response.json()

            return result.get("is_compatible", False)

        except requests.exceptions.RequestException as e:
            # 422 means incompatible — return False instead of raising
            if (
                hasattr(e, "response")
                and e.response is not None
                and e.response.status_code == 422
            ):
                self.logger.debug(f"Schema incompatible with {subject}")
                return False
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
                url = f"{self.base_url}/config/{self._encode_subject(subject)}"
            else:
                url = f"{self.base_url}/config"

            response = self._session.get(url, timeout=(5, 30))

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get config: {e}")
            raise SchemaRegistryError(f"Failed to get config: {e}")

    def set_config(
        self, config_data: Dict[str, Any], subject: Optional[str] = None
    ) -> None:
        """
        Set configuration for a subject or global configuration.

        Args:
            config_data: Configuration data
            subject: Subject name (None for global config)
        """

        try:
            if subject:
                url = f"{self.base_url}/config/{self._encode_subject(subject)}"
            else:
                url = f"{self.base_url}/config"

            response = self._session.put(
                url,
                json=config_data,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                timeout=(5, 30),
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

            response = self._session.get(url, timeout=10)

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

        mapping = {"avro": "AVRO", "protobuf": "PROTOBUF", "json-schema": "JSON"}

        result = mapping.get(schema_format.lower())
        if result is None:
            self.logger.warning(
                f"Unknown schema format '{schema_format}', defaulting to AVRO. "
                f"Valid formats: {', '.join(mapping.keys())}"
            )
            return "AVRO"
        return result

    def _set_subject_compatibility(self, subject: str, compatibility: str) -> None:
        """
        Set compatibility level for a subject.

        Args:
            subject: Subject name
            compatibility: Compatibility level (NONE, BACKWARD, FORWARD, FULL, etc.)
        """
        try:
            url = f"{self.base_url}/config/{self._encode_subject(subject)}"

            compatibility_data = {"compatibility": compatibility}

            self.logger.info(
                f"Setting compatibility level for subject '{subject}' to: {compatibility}"
            )

            response = self._session.put(
                url,
                json=compatibility_data,
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                timeout=(5, 30),
            )

            response.raise_for_status()

            self.logger.info(
                f"Successfully set compatibility level for subject '{subject}' to: {compatibility}"
            )

        except requests.exceptions.RequestException as e:
            self.logger.warning(
                f"Failed to set compatibility level for subject '{subject}': {e}"
            )
            # Don't raise exception - compatibility setting is optional
        except Exception as e:
            self.logger.warning(
                f"Unexpected error setting compatibility level for subject '{subject}': {e}"
            )
            # Don't raise exception - compatibility setting is optional

    def _test_connection(self) -> None:
        """Test Schema Registry connection on initialization."""
        try:
            # Try to get the Schema Registry version/info
            url = f"{self.base_url}/subjects"
            response = self._session.get(url, timeout=10)
            response.raise_for_status()
            self.logger.info("Schema Registry connection test successful")
        except requests.exceptions.RequestException as e:
            # Only log critical connection errors, suppress routine connection failures
            error_msg = str(e)
            if "nodename nor servname provided" in error_msg or "NXDOMAIN" in error_msg:
                # Only show this if verbose logging is enabled
                if (
                    hasattr(self, "config")
                    and hasattr(self.config, "performance")
                    and self.config.performance.verbose_logging
                ):
                    self.logger.warning(
                        f"Schema Registry connection test failed - DNS resolution error for {self.base_url}"
                    )
                    self.logger.warning(
                        "Please verify your Schema Registry URL in the configuration"
                    )
            # Suppress other connection test failures to keep output clean
        except Exception as e:
            self.logger.debug(
                f"Schema Registry connection test encountered unexpected error: {e}"
            )

    @staticmethod
    def _encode_subject(subject: str) -> str:
        """URL-encode a subject name for safe use in URL paths."""
        return quote(subject, safe="")

    def _generate_subject_name(self, topic_name: str, schema_format: str) -> str:
        """
        Generate subject name based on the configured strategy and optional context.

        Args:
            topic_name: Kafka topic name
            schema_format: Schema format (avro, protobuf, json-schema)

        Returns:
            Subject name for Schema Registry
        """
        strategy = self.config.schema_registry.subject_name_strategy

        if strategy == "TopicNameStrategy":
            subject = f"{topic_name}-value"
        elif strategy == "RecordNameStrategy":
            self.logger.warning(
                "RecordNameStrategy requires record name from schema - using topic name as fallback"
            )
            subject = topic_name
        elif strategy == "TopicRecordNameStrategy":
            self.logger.warning(
                "TopicRecordNameStrategy requires record name from schema - using topic name as fallback"
            )
            subject = topic_name
        else:
            self.logger.warning(
                f"Unknown strategy '{strategy}' - falling back to TopicNameStrategy"
            )
            subject = f"{topic_name}-value"

        # Apply context prefix if configured
        context = self.config.schema_registry.context
        if context:
            subject = f":.{context}:{subject}"

        return subject
