"""
Comprehensive unit tests for core components (consumer, registry, discovery)
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
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
        """Test getting watermark offsets."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        mock_consumer.get_watermark_offsets.return_value = (0, 100)
        
        consumer = KafkaConsumer(self.config)
        
        # Mock topic partition
        mock_partition = Mock()
        mock_partition.topic = "test_topic"
        mock_partition.partition = 0
        
        low, high = consumer.get_watermark_offsets(mock_partition)
        
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
        self.config.schema_registry.auth = None
        self.config.schema_registry.verify_ssl = True
    
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
    
    @patch('schema_infer.core.registry.requests.post')
    def test_register_schema_success(self, mock_post):
        """Test successful schema registration."""
        mock_response = Mock()
        mock_response.json.return_value = {"id": 1}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response
        
        registry = SchemaRegistry(self.config)
        
        schema_content = '{"type": "record", "name": "TestRecord", "fields": []}'
        result = registry.register_schema("test-topic", schema_content, "avro")
        
        assert result == {"id": 1}
        mock_post.assert_called_once()
    
    @patch('schema_infer.core.registry.requests.post')
    def test_register_schema_failure(self, mock_post):
        """Test schema registration failure."""
        mock_post.side_effect = Exception("Registration failed")
        
        registry = SchemaRegistry(self.config)
        
        schema_content = '{"type": "record", "name": "TestRecord", "fields": []}'
        
        with pytest.raises(Exception):
            registry.register_schema("test-topic", schema_content, "avro")
    
    def test_generate_subject_name_topic_name_strategy(self):
        """Test subject name generation with TopicNameStrategy."""
        registry = SchemaRegistry(self.config)
        
        subject_name = registry._generate_subject_name("test-topic", "TopicNameStrategy")
        assert subject_name == "test-topic-value"
    
    def test_generate_subject_name_record_name_strategy(self):
        """Test subject name generation with RecordNameStrategy."""
        registry = SchemaRegistry(self.config)
        
        subject_name = registry._generate_subject_name("test-topic", "RecordNameStrategy")
        assert subject_name == "com.schema-infer.schema.infer.TestRecord"
    
    def test_generate_subject_name_topic_record_name_strategy(self):
        """Test subject name generation with TopicRecordNameStrategy."""
        registry = SchemaRegistry(self.config)
        
        subject_name = registry._generate_subject_name("test-topic", "TopicRecordNameStrategy")
        assert subject_name == "test-topic-com.schema-infer.schema.infer.TestRecord"


class TestTopicDiscovery:
    """Comprehensive tests for topic discovery."""
    
    def setup_method(self):
        """Set up test configuration."""
        self.config = Config()
        self.config.kafka.bootstrap_servers = "localhost:9092"
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_by_name(self, mock_consumer_class):
        """Test discovering topics by exact name."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock metadata response
        mock_metadata = Mock()
        mock_metadata.topics = {
            "user-events": Mock(),
            "order-events": Mock(),
            "_internal_topic": Mock()
        }
        mock_consumer.list_topics.return_value = mock_metadata
        
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(topic_name="user-events")
        
        assert topics == ["user-events"]
        mock_consumer.list_topics.assert_called_once()
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_by_list(self, mock_consumer_class):
        """Test discovering topics by list."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock metadata response
        mock_metadata = Mock()
        mock_metadata.topics = {
            "user-events": Mock(),
            "order-events": Mock(),
            "payment-events": Mock(),
            "_internal_topic": Mock()
        }
        mock_consumer.list_topics.return_value = mock_metadata
        
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(topics=["user-events", "order-events"])
        
        assert "user-events" in topics
        assert "order-events" in topics
        assert len(topics) == 2
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_by_prefix(self, mock_consumer_class):
        """Test discovering topics by prefix."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock metadata response
        mock_metadata = Mock()
        mock_metadata.topics = {
            "user-events": Mock(),
            "user-profiles": Mock(),
            "order-events": Mock(),
            "_internal_topic": Mock()
        }
        mock_consumer.list_topics.return_value = mock_metadata
        
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(topic_prefix="user-")
        
        assert "user-events" in topics
        assert "user-profiles" in topics
        assert "order-events" not in topics
        assert len(topics) == 2
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_by_pattern(self, mock_consumer_class):
        """Test discovering topics by regex pattern."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock metadata response
        mock_metadata = Mock()
        mock_metadata.topics = {
            "prod-user-events": Mock(),
            "prod-order-events": Mock(),
            "dev-user-events": Mock(),
            "test-order-events": Mock(),
            "_internal_topic": Mock()
        }
        mock_consumer.list_topics.return_value = mock_metadata
        
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
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock metadata response
        mock_metadata = Mock()
        mock_metadata.topics = {
            "user-events": Mock(),
            "order-events": Mock(),
            "_internal_topic": Mock(),
            "__consumer_offsets": Mock(),
            "_schema-infer-metrics": Mock()
        }
        mock_consumer.list_topics.return_value = mock_metadata
        
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(exclude_internal=True)
        
        assert "user-events" in topics
        assert "order-events" in topics
        assert "_internal_topic" not in topics
        assert "__consumer_offsets" not in topics
        assert "_schema-infer-metrics" not in topics
        assert len(topics) == 2
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_custom_internal_prefix(self, mock_consumer_class):
        """Test custom internal topic prefix."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock metadata response
        mock_metadata = Mock()
        mock_metadata.topics = {
            "user-events": Mock(),
            "order-events": Mock(),
            "internal-topic": Mock(),
            "system-topic": Mock()
        }
        mock_consumer.list_topics.return_value = mock_metadata
        
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(internal_prefix="internal-")
        
        assert "user-events" in topics
        assert "order-events" in topics
        assert "internal-topic" not in topics
        assert "system-topic" in topics
        assert len(topics) == 3
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_additional_exclude_prefixes(self, mock_consumer_class):
        """Test additional exclude prefixes."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock metadata response
        mock_metadata = Mock()
        mock_metadata.topics = {
            "user-events": Mock(),
            "order-events": Mock(),
            "temp-topic": Mock(),
            "backup-topic": Mock()
        }
        mock_consumer.list_topics.return_value = mock_metadata
        
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(additional_exclude_prefixes=["temp-", "backup-"])
        
        assert "user-events" in topics
        assert "order-events" in topics
        assert "temp-topic" not in topics
        assert "backup-topic" not in topics
        assert len(topics) == 2
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_include_patterns(self, mock_consumer_class):
        """Test include patterns."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock metadata response
        mock_metadata = Mock()
        mock_metadata.topics = {
            "user-events": Mock(),
            "order-events": Mock(),
            "payment-events": Mock(),
            "system-logs": Mock()
        }
        mock_consumer.list_topics.return_value = mock_metadata
        
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(include_patterns=[".*-events"])
        
        assert "user-events" in topics
        assert "order-events" in topics
        assert "payment-events" in topics
        assert "system-logs" not in topics
        assert len(topics) == 3
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_no_matches(self, mock_consumer_class):
        """Test when no topics match criteria."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock metadata response
        mock_metadata = Mock()
        mock_metadata.topics = {
            "user-events": Mock(),
            "order-events": Mock()
        }
        mock_consumer.list_topics.return_value = mock_metadata
        
        discovery = TopicDiscovery(self.config)
        topics = discovery.discover_topics(topic_prefix="nonexistent-")
        
        assert len(topics) == 0
    
    @patch('schema_infer.core.discovery.KafkaConsumer')
    def test_discover_topics_error_handling(self, mock_consumer_class):
        """Test error handling in topic discovery."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        mock_consumer.list_topics.side_effect = Exception("Connection failed")
        
        discovery = TopicDiscovery(self.config)
        
        with pytest.raises(Exception, match="Connection failed"):
            discovery.discover_topics()


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
        # Mock consumer
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock metadata response
        mock_metadata = Mock()
        mock_metadata.topics = {
            "user-events": Mock(),
            "order-events": Mock()
        }
        mock_consumer.list_topics.return_value = mock_metadata
        
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
        assert config.kafka.auto_offset_reset == "latest"
        assert config.kafka.session_timeout_ms == 30000
        assert config.schema_registry.verify_ssl == True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
