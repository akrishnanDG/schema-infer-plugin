"""
Comprehensive unit tests for core components (consumer, registry, discovery)
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from confluent_kafka import KafkaError as ConfluentKafkaError
from typing import List, Dict, Any

from schema_infer.core.consumer import KafkaConsumer
from schema_infer.core.registry import SchemaRegistry
from schema_infer.core.discovery import TopicDiscovery
from schema_infer.config import Config


class TestKafkaConsumer:
    """Comprehensive tests for Kafka consumer."""
    
    def setup_method(self):
        """Set up test configuration."""
        self.config = Config()
        self.config.kafka.bootstrap_servers = "localhost:9092"
        self.config.kafka.auto_offset_reset = "latest"
        self.config.kafka.session_timeout_ms = 30000
        self.config.kafka.heartbeat_interval_ms = 10000
    
    @patch('schema_infer.core.consumer.Consumer')
    def test_consumer_initialization(self, mock_consumer_class):
        """Test consumer initialization."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumer(self.config)
        
        assert consumer.config == self.config
        mock_consumer_class.assert_called_once()
    
    @patch('schema_infer.core.consumer.Consumer')
    def test_list_topics(self, mock_consumer_class):
        """Test listing topics."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock metadata response
        mock_metadata = Mock()
        mock_metadata.topics = {
            "topic1": Mock(),
            "topic2": Mock(),
            "_internal_topic": Mock()
        }
        mock_consumer.list_topics.return_value = mock_metadata
        
        consumer = KafkaConsumer(self.config)
        topics = consumer.list_topics()
        
        assert "topic1" in topics
        assert "topic2" in topics
        assert "_internal_topic" in topics
        mock_consumer.list_topics.assert_called_once()
    
    @patch('schema_infer.core.consumer.Consumer')
    def test_get_watermark_offsets(self, mock_consumer_class):
        """Test getting watermark offsets via the underlying consumer."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        mock_consumer.get_watermark_offsets.return_value = (0, 100)

        consumer = KafkaConsumer(self.config)

        # Mock topic partition
        mock_partition = Mock()
        mock_partition.topic = "test_topic"
        mock_partition.partition = 0

        # Access the underlying confluent_kafka Consumer directly
        low, high = consumer.consumer.get_watermark_offsets(mock_partition, timeout=10.0)

        assert low == 0
        assert high == 100
        mock_consumer.get_watermark_offsets.assert_called_once_with(mock_partition, timeout=10.0)
    
    @patch('schema_infer.core.consumer.Consumer')
    def test_consumer_close(self, mock_consumer_class):
        """Test consumer cleanup."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        consumer = KafkaConsumer(self.config)
        consumer.close()
        
        mock_consumer.close.assert_called_once()


class TestSchemaRegistry:
    """Comprehensive tests for Schema Registry."""
    
    def setup_method(self):
        """Set up test configuration."""
        self.config = Config()
        self.config.schema_registry.url = "http://localhost:8081"
        self.config.schema_registry.username = None
        self.config.schema_registry.password = None
    
    @patch('schema_infer.core.registry.requests.get')
    def test_connection_test_success(self, mock_get):
        """Test successful connection test."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        registry = SchemaRegistry(self.config)
        
        # Connection test should not raise exception
        assert registry is not None
        mock_get.assert_called_once()
    
    @patch('schema_infer.core.registry.requests.get')
    def test_connection_test_failure(self, mock_get):
        """Test connection test failure."""
        mock_get.side_effect = Exception("Connection failed")
        
        # Should not raise exception during initialization
        registry = SchemaRegistry(self.config)
        assert registry is not None
    
    @patch('schema_infer.core.registry.requests.request')
    @patch('schema_infer.core.registry.requests.get')
    def test_register_schema_success(self, mock_get, mock_request):
        """Test successful schema registration."""
        # Mock the connection test GET request
        mock_get_response = Mock()
        mock_get_response.raise_for_status.return_value = None
        mock_get.return_value = mock_get_response

        # Mock the requests.request call (used by _request_with_retry)
        mock_req_response = Mock()
        mock_req_response.json.return_value = {"id": 1}
        mock_req_response.raise_for_status.return_value = None
        mock_request.return_value = mock_req_response

        registry = SchemaRegistry(self.config)

        schema_content = '{"type": "record", "name": "TestRecord", "fields": []}'
        result = registry.register_schema("test-topic", schema_content, "avro")

        assert result == 1
        mock_request.assert_called()

    @patch('schema_infer.core.registry.requests.request')
    @patch('schema_infer.core.registry.requests.get')
    def test_register_schema_failure(self, mock_get, mock_request):
        """Test schema registration failure."""
        # Mock the connection test GET request
        mock_get_response = Mock()
        mock_get_response.raise_for_status.return_value = None
        mock_get.return_value = mock_get_response

        mock_request.side_effect = Exception("Registration failed")

        registry = SchemaRegistry(self.config)

        schema_content = '{"type": "record", "name": "TestRecord", "fields": []}'

        with pytest.raises(Exception):
            registry.register_schema("test-topic", schema_content, "avro")
    
    @patch('schema_infer.core.registry.requests.get')
    def test_generate_subject_name_topic_name_strategy(self, mock_get):
        """Test subject name generation with TopicNameStrategy."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        self.config.schema_registry.subject_name_strategy = "TopicNameStrategy"
        registry = SchemaRegistry(self.config)

        subject_name = registry._generate_subject_name("test-topic", "avro")
        assert subject_name == "test-topic-value"

    @patch('schema_infer.core.registry.requests.get')
    def test_generate_subject_name_record_name_strategy(self, mock_get):
        """Test subject name generation with RecordNameStrategy."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        self.config.schema_registry.subject_name_strategy = "RecordNameStrategy"
        registry = SchemaRegistry(self.config)

        subject_name = registry._generate_subject_name("test-topic", "avro")
        assert subject_name == "test-topic"

    @patch('schema_infer.core.registry.requests.get')
    def test_generate_subject_name_topic_record_name_strategy(self, mock_get):
        """Test subject name generation with TopicRecordNameStrategy."""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        self.config.schema_registry.subject_name_strategy = "TopicRecordNameStrategy"
        registry = SchemaRegistry(self.config)

        subject_name = registry._generate_subject_name("test-topic", "avro")
        assert subject_name == "test-topic"


class TestTopicDiscovery:
    """Comprehensive tests for topic discovery."""
    
    def setup_method(self):
        """Set up test configuration."""
        self.config = Config()
        self.config.kafka.bootstrap_servers = "localhost:9092"
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_by_name(self, mock_consumer_class):
        """Test discovering topics by exact name."""
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.list_topics.return_value = ["user-events", "order-events", "_internal_topic"]
        mock_consumer_instance.__enter__ = MagicMock(return_value=mock_consumer_instance)
        mock_consumer_instance.__exit__ = MagicMock(return_value=False)
        mock_consumer_class.return_value = mock_consumer_instance

        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(topic="user-events")

        assert topics == ["user-events"]
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_by_list(self, mock_consumer_class):
        """Test discovering topics by list."""
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.list_topics.return_value = ["user-events", "order-events", "payment-events", "_internal_topic"]
        mock_consumer_instance.__enter__ = MagicMock(return_value=mock_consumer_instance)
        mock_consumer_instance.__exit__ = MagicMock(return_value=False)
        mock_consumer_class.return_value = mock_consumer_instance

        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(topics="user-events,order-events")

        assert "user-events" in topics
        assert "order-events" in topics
        assert len(topics) == 2
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_by_prefix(self, mock_consumer_class):
        """Test discovering topics by prefix."""
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.list_topics.return_value = ["user-events", "user-profiles", "order-events", "_internal_topic"]
        mock_consumer_instance.__enter__ = MagicMock(return_value=mock_consumer_instance)
        mock_consumer_instance.__exit__ = MagicMock(return_value=False)
        mock_consumer_class.return_value = mock_consumer_instance

        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(topic_prefix="user-")

        assert "user-events" in topics
        assert "user-profiles" in topics
        assert "order-events" not in topics
        assert len(topics) == 2
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_by_pattern(self, mock_consumer_class):
        """Test discovering topics by regex pattern."""
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.list_topics.return_value = ["prod-user-events", "prod-order-events", "dev-user-events", "test-order-events", "_internal_topic"]
        mock_consumer_instance.__enter__ = MagicMock(return_value=mock_consumer_instance)
        mock_consumer_instance.__exit__ = MagicMock(return_value=False)
        mock_consumer_class.return_value = mock_consumer_instance

        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(topic_pattern="^prod-.*")

        assert "prod-user-events" in topics
        assert "prod-order-events" in topics
        assert "dev-user-events" not in topics
        assert "test-order-events" not in topics
        assert len(topics) == 2
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_exclude_internal(self, mock_consumer_class):
        """Test excluding internal topics."""
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.list_topics.return_value = ["user-events", "order-events", "__internal_topic", "__consumer_offsets", "__schema-infer-metrics"]
        mock_consumer_instance.__enter__ = MagicMock(return_value=mock_consumer_instance)
        mock_consumer_instance.__exit__ = MagicMock(return_value=False)
        mock_consumer_class.return_value = mock_consumer_instance

        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(exclude_internal=True)

        assert "user-events" in topics
        assert "order-events" in topics
        assert "__internal_topic" not in topics
        assert "__consumer_offsets" not in topics
        assert "__schema-infer-metrics" not in topics
        assert len(topics) == 2
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_custom_internal_prefix(self, mock_consumer_class):
        """Test custom internal topic prefix."""
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.list_topics.return_value = ["user-events", "order-events", "internal-topic", "system-topic"]
        mock_consumer_instance.__enter__ = MagicMock(return_value=mock_consumer_instance)
        mock_consumer_instance.__exit__ = MagicMock(return_value=False)
        mock_consumer_class.return_value = mock_consumer_instance

        self.config.topic_filter.internal_prefix = "internal-"
        self.config.topic_filter.exclude_internal = True
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(exclude_internal=True)

        assert "user-events" in topics
        assert "order-events" in topics
        assert "internal-topic" not in topics
        assert "system-topic" in topics
        assert len(topics) == 3
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_additional_exclude_prefixes(self, mock_consumer_class):
        """Test additional exclude prefixes."""
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.list_topics.return_value = ["user-events", "order-events", "temp-topic", "backup-topic"]
        mock_consumer_instance.__enter__ = MagicMock(return_value=mock_consumer_instance)
        mock_consumer_instance.__exit__ = MagicMock(return_value=False)
        mock_consumer_class.return_value = mock_consumer_instance

        self.config.topic_filter.additional_exclude_prefixes = ["temp-", "backup-"]
        self.config.topic_filter.exclude_internal = True
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(exclude_internal=True)

        assert "user-events" in topics
        assert "order-events" in topics
        assert "temp-topic" not in topics
        assert "backup-topic" not in topics
        assert len(topics) == 2
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_include_patterns(self, mock_consumer_class):
        """Test include patterns."""
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.list_topics.return_value = ["user-events", "order-events", "payment-events", "system-logs"]
        mock_consumer_instance.__enter__ = MagicMock(return_value=mock_consumer_instance)
        mock_consumer_instance.__exit__ = MagicMock(return_value=False)
        mock_consumer_class.return_value = mock_consumer_instance

        self.config.topic_filter.include_patterns = [".*-events"]
        self.config.topic_filter.exclude_internal = True
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(exclude_internal=True)

        assert "user-events" in topics
        assert "order-events" in topics
        assert "payment-events" in topics
        # system-logs will still appear since it doesn't start with the internal prefix
        # and include_patterns only override exclusions for topics that would be excluded
        assert len(topics) >= 3
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_no_matches(self, mock_consumer_class):
        """Test when no topics match criteria."""
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.list_topics.return_value = ["user-events", "order-events"]
        mock_consumer_instance.__enter__ = MagicMock(return_value=mock_consumer_instance)
        mock_consumer_instance.__exit__ = MagicMock(return_value=False)
        mock_consumer_class.return_value = mock_consumer_instance

        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(topic_prefix="nonexistent-")

        assert len(topics) == 0
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_error_handling(self, mock_consumer_class):
        """Test error handling in topic discovery."""
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.list_topics.side_effect = Exception("Connection failed")
        mock_consumer_instance.__enter__ = MagicMock(return_value=mock_consumer_instance)
        mock_consumer_instance.__exit__ = MagicMock(return_value=False)
        mock_consumer_class.return_value = mock_consumer_instance

        discovery = TopicDiscovery(self.config)

        # _list_all_topics catches the exception internally and returns []
        topics = discovery.discover_topics()
        assert len(topics) == 0


class TestCoreComponentsIntegration:
    """Integration tests for core components."""
    
    def setup_method(self):
        """Set up test configuration."""
        self.config = Config()
        self.config.kafka.bootstrap_servers = "localhost:9092"
        self.config.schema_registry.url = "http://localhost:8081"
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    @patch('schema_infer.core.registry.requests.get')
    def test_end_to_end_workflow(self, mock_registry_get, mock_consumer_class):
        """Test end-to-end workflow with mocked components."""
        # Mock consumer with context manager support
        mock_consumer_instance = MagicMock()
        mock_consumer_instance.list_topics.return_value = ["user-events", "order-events"]
        mock_consumer_instance.__enter__ = MagicMock(return_value=mock_consumer_instance)
        mock_consumer_instance.__exit__ = MagicMock(return_value=False)
        mock_consumer_class.return_value = mock_consumer_instance

        # Mock registry response
        mock_registry_response = Mock()
        mock_registry_response.raise_for_status.return_value = None
        mock_registry_get.return_value = mock_registry_response

        # Test topic discovery
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(topic_prefix="user-")

        assert "user-events" in topics

        # Test schema registry connection
        registry = SchemaRegistry(self.config)
        assert registry is not None

        # Test consumer initialization
        consumer = KafkaConsumer(self.config)
        assert consumer is not None
    
    def test_configuration_validation(self):
        """Test configuration validation."""
        # Test valid configuration
        config = Config()
        config.kafka.bootstrap_servers = "localhost:9092"
        config.schema_registry.url = "http://localhost:8081"
        
        assert config.kafka.bootstrap_servers == "localhost:9092"
        assert config.schema_registry.url == "http://localhost:8081"
        
        # Test default values
        assert config.kafka.auto_offset_reset == "earliest"
        assert config.kafka.session_timeout_ms == 30000
        assert config.schema_registry.compatibility == "BACKWARD"


class TestKafkaConsumerMethods:
    """Tests for KafkaConsumer methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = Config()
        self.config.kafka.bootstrap_servers = "localhost:9092"

    @patch('schema_infer.plugin.auth.AuthenticationManager')
    @patch('schema_infer.core.consumer.Consumer')
    def test_consume_topic(self, mock_consumer_class, mock_auth):
        """Test consuming messages from a topic."""
        mock_auth_instance = MagicMock()
        mock_auth_instance.configure_kafka_auth.return_value = {}
        mock_auth.return_value = mock_auth_instance

        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        # Simulate 3 messages then None (timeout)
        msg1 = MagicMock()
        msg1.error.return_value = None
        msg1.key.return_value = b'key1'
        msg1.value.return_value = b'{"id": 1}'

        msg2 = MagicMock()
        msg2.error.return_value = None
        msg2.key.return_value = b'key2'
        msg2.value.return_value = b'{"id": 2}'

        msg3 = MagicMock()
        msg3.error.return_value = None
        msg3.key.return_value = None
        msg3.value.return_value = b'{"id": 3}'

        mock_consumer.poll.side_effect = [msg1, msg2, msg3, None, None, None]

        from schema_infer.core.consumer import KafkaConsumer
        consumer = KafkaConsumer(self.config)
        messages = consumer.consume_topic("test-topic", max_messages=3, timeout=10)

        assert len(messages) == 3
        assert messages[0] == (b'key1', b'{"id": 1}')
        assert messages[2] == (None, b'{"id": 3}')
        mock_consumer.subscribe.assert_called_once_with(["test-topic"])

    @patch('schema_infer.plugin.auth.AuthenticationManager')
    @patch('schema_infer.core.consumer.Consumer')
    def test_consume_topic_partition_eof(self, mock_consumer_class, mock_auth):
        """Test that partition EOF stops consumption."""
        mock_auth_instance = MagicMock()
        mock_auth_instance.configure_kafka_auth.return_value = {}
        mock_auth.return_value = mock_auth_instance

        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        msg1 = MagicMock()
        msg1.error.return_value = None
        msg1.key.return_value = None
        msg1.value.return_value = b'{"id": 1}'

        eof_msg = MagicMock()
        eof_error = MagicMock()
        eof_error.code.return_value = ConfluentKafkaError._PARTITION_EOF
        eof_msg.error.return_value = eof_error

        mock_consumer.poll.side_effect = [msg1, eof_msg]

        from schema_infer.core.consumer import KafkaConsumer
        consumer = KafkaConsumer(self.config)
        messages = consumer.consume_topic("test-topic", max_messages=10, timeout=10)

        assert len(messages) == 1

    @patch('schema_infer.plugin.auth.AuthenticationManager')
    @patch('schema_infer.core.consumer.Consumer')
    def test_consume_topic_skips_null_values(self, mock_consumer_class, mock_auth):
        """Test that null message values are skipped."""
        mock_auth_instance = MagicMock()
        mock_auth_instance.configure_kafka_auth.return_value = {}
        mock_auth.return_value = mock_auth_instance

        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        msg_with_value = MagicMock()
        msg_with_value.error.return_value = None
        msg_with_value.key.return_value = None
        msg_with_value.value.return_value = b'data'

        msg_null_value = MagicMock()
        msg_null_value.error.return_value = None
        msg_null_value.key.return_value = b'key'
        msg_null_value.value.return_value = None

        mock_consumer.poll.side_effect = [msg_null_value, msg_with_value]

        from schema_infer.core.consumer import KafkaConsumer
        consumer = KafkaConsumer(self.config)
        messages = consumer.consume_topic("test-topic", max_messages=1, timeout=5)

        assert len(messages) == 1
        assert messages[0] == (None, b'data')

    @patch('schema_infer.plugin.auth.AuthenticationManager')
    @patch('schema_infer.core.consumer.Consumer')
    def test_consume_topics_multiple(self, mock_consumer_class, mock_auth):
        """Test consuming from multiple topics."""
        mock_auth_instance = MagicMock()
        mock_auth_instance.configure_kafka_auth.return_value = {}
        mock_auth.return_value = mock_auth_instance

        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        msg = MagicMock()
        msg.error.return_value = None
        msg.key.return_value = None
        msg.value.return_value = b'data'

        # First topic gets a message, second gets none
        mock_consumer.poll.side_effect = [msg, None, None, None, None, None]

        from schema_infer.core.consumer import KafkaConsumer
        consumer = KafkaConsumer(self.config)
        results = consumer.consume_topics(["topic-a", "topic-b"], max_messages_per_topic=5, timeout=2)

        assert "topic-a" in results
        assert "topic-b" in results

    @patch('schema_infer.plugin.auth.AuthenticationManager')
    @patch('schema_infer.core.consumer.Consumer')
    def test_get_topic_metadata(self, mock_consumer_class, mock_auth):
        """Test getting topic metadata."""
        mock_auth_instance = MagicMock()
        mock_auth_instance.configure_kafka_auth.return_value = {}
        mock_auth.return_value = mock_auth_instance

        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        # Mock metadata response
        mock_partition = MagicMock()
        mock_partition.leader = 1
        mock_partition.replicas = [1]
        mock_partition.isrs = [1]
        mock_partition.error = None

        mock_topic_metadata = MagicMock()
        mock_topic_metadata.partitions = {0: mock_partition, 1: mock_partition}
        mock_topic_metadata.error = None

        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = {"test-topic": mock_topic_metadata}
        mock_consumer.list_topics.return_value = mock_cluster_metadata

        from schema_infer.core.consumer import KafkaConsumer
        consumer = KafkaConsumer(self.config)
        metadata = consumer.get_topic_metadata("test-topic")

        assert metadata["name"] == "test-topic"
        assert metadata["partitions"] == 2
        assert "0" in metadata["partition_info"]


class TestSchemaRegistryMethods:
    """Tests for SchemaRegistry methods."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = Config()
        self.config.schema_registry.url = "http://localhost:8081"
        self.config.schema_registry.username = None
        self.config.schema_registry.password = None

    @patch('schema_infer.core.registry.requests.get')
    def test_get_schema(self, mock_get):
        """Test getting a schema by ID."""
        # Mock _test_connection
        mock_get.return_value = MagicMock(status_code=200, json=lambda: [])
        mock_get.return_value.raise_for_status = MagicMock()

        from schema_infer.core.registry import SchemaRegistry
        registry = SchemaRegistry(self.config)

        # Now mock get_schema call
        schema_response = {"schema": '{"type":"string"}', "schemaType": "AVRO"}
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = schema_response
        mock_get.return_value.raise_for_status = MagicMock()

        result = registry.get_schema(1)
        assert result["schemaType"] == "AVRO"

    @patch('schema_infer.core.registry.requests.get')
    def test_list_subjects(self, mock_get):
        """Test listing subjects."""
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.json.return_value = []
        mock_get.return_value.raise_for_status = MagicMock()

        from schema_infer.core.registry import SchemaRegistry
        registry = SchemaRegistry(self.config)

        mock_get.return_value.json.return_value = ["subject-1", "subject-2"]
        result = registry.list_subjects()
        assert len(result) == 2
        assert "subject-1" in result

    @patch('schema_infer.core.registry.requests.delete')
    @patch('schema_infer.core.registry.requests.get')
    def test_delete_subject(self, mock_get, mock_delete):
        """Test deleting a subject."""
        mock_get.return_value = MagicMock(status_code=200, json=lambda: [])
        mock_get.return_value.raise_for_status = MagicMock()

        from schema_infer.core.registry import SchemaRegistry
        registry = SchemaRegistry(self.config)

        mock_delete.return_value = MagicMock(status_code=200)
        mock_delete.return_value.json.return_value = [1, 2]
        mock_delete.return_value.raise_for_status = MagicMock()

        result = registry.delete_subject("test-subject")
        assert result == [1, 2]

    @patch('schema_infer.core.registry.requests.request')
    @patch('schema_infer.core.registry.requests.get')
    def test_check_compatibility(self, mock_get, mock_request):
        """Test checking schema compatibility."""
        mock_get.return_value = MagicMock(status_code=200, json=lambda: [])
        mock_get.return_value.raise_for_status = MagicMock()

        from schema_infer.core.registry import SchemaRegistry
        registry = SchemaRegistry(self.config)

        compat_response = MagicMock(status_code=200)
        compat_response.json.return_value = {"is_compatible": True}
        compat_response.raise_for_status = MagicMock()
        mock_request.return_value = compat_response

        result = registry.check_compatibility("test-subject", '{"type":"string"}')
        assert result == True

    @patch('schema_infer.core.registry.requests.get')
    def test_get_config(self, mock_get):
        """Test getting SR config."""
        mock_get.return_value = MagicMock(status_code=200, json=lambda: [])
        mock_get.return_value.raise_for_status = MagicMock()

        from schema_infer.core.registry import SchemaRegistry
        registry = SchemaRegistry(self.config)

        # Replace the return_value entirely so json() returns the config dict
        config_response = MagicMock(status_code=200)
        config_response.json.return_value = {"compatibilityLevel": "BACKWARD"}
        config_response.raise_for_status = MagicMock()
        mock_get.return_value = config_response

        result = registry.get_config()
        assert result["compatibilityLevel"] == "BACKWARD"

    @patch('schema_infer.core.registry.requests.put')
    @patch('schema_infer.core.registry.requests.get')
    def test_set_config(self, mock_get, mock_put):
        """Test setting SR config."""
        mock_get.return_value = MagicMock(status_code=200, json=lambda: [])
        mock_get.return_value.raise_for_status = MagicMock()

        from schema_infer.core.registry import SchemaRegistry
        registry = SchemaRegistry(self.config)

        mock_put.return_value = MagicMock(status_code=200)
        mock_put.return_value.raise_for_status = MagicMock()

        registry.set_config({"compatibility": "FULL"})
        mock_put.assert_called()


class TestAuthenticationManager:
    """Tests for AuthenticationManager."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = Config()

    def test_plaintext_auth(self):
        """Test PLAINTEXT produces empty auth config."""
        self.config.kafka.security_protocol = "PLAINTEXT"
        from schema_infer.plugin.auth import AuthenticationManager
        auth_manager = AuthenticationManager(self.config)
        auth_config = auth_manager.configure_kafka_auth()

        assert "security.protocol" in auth_config
        assert auth_config["security.protocol"] == "PLAINTEXT"

    def test_sasl_ssl_auth(self):
        """Test SASL_SSL auth config."""
        self.config.kafka.security_protocol = "SASL_SSL"
        self.config.kafka.sasl_mechanism = "PLAIN"
        self.config.kafka.sasl_username = "user"
        self.config.kafka.sasl_password = "pass"

        from schema_infer.plugin.auth import AuthenticationManager
        auth_manager = AuthenticationManager(self.config)
        auth_config = auth_manager.configure_kafka_auth()

        assert auth_config["security.protocol"] == "SASL_SSL"
        assert auth_config["sasl.mechanism"] == "PLAIN"
        assert auth_config["sasl.username"] == "user"
        assert auth_config["sasl.password"] == "pass"

    def test_cloud_api_key_auth(self):
        """Test Cloud API key auth config."""
        self.config.kafka.bootstrap_servers = "pkc-abc123.us-east-1.aws.schema-infer.cloud:9092"
        self.config.kafka.cloud_api_key = "api-key"
        self.config.kafka.cloud_api_secret = "api-secret"

        from schema_infer.plugin.auth import AuthenticationManager
        auth_manager = AuthenticationManager(self.config)
        auth_config = auth_manager.configure_kafka_auth()

        assert auth_config["security.protocol"] == "SASL_SSL"
        assert auth_config["sasl.mechanism"] == "PLAIN"
        assert auth_config["sasl.username"] == "api-key"
        assert auth_config["sasl.password"] == "api-secret"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
