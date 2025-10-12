"""
Kafka Consumer for Schema Inference Plugin
Version: 1.2.0
Build: 2025-10-12-10:55:00
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError as ConfluentKafkaError, KafkaException

from ..config import Config
from ..utils.exceptions import KafkaError
from ..utils.logger import get_logger
from ..utils.validators import validate_topic_name, validate_max_messages, validate_timeout


class KafkaConsumer:
    """Kafka consumer for reading messages from topics."""
    
    def __init__(self, config: Config):
        """Initialize Kafka consumer."""
        
        self.config = config
        self.logger = get_logger(__name__)
        self.consumer: Optional[Consumer] = None
        self._initialize_consumer()
    
    def _initialize_consumer(self) -> None:
        """Initialize the Kafka consumer with configuration."""
        
        try:
            # Build consumer configuration
            consumer_config = {
                "bootstrap.servers": self.config.kafka.bootstrap_servers,
                "group.id": self.config.kafka.consumer_group,
                "auto.offset.reset": self.config.kafka.auto_offset_reset,
                "enable.auto.commit": self.config.kafka.enable_auto_commit,
                "session.timeout.ms": self.config.kafka.session_timeout_ms,
                "heartbeat.interval.ms": self.config.kafka.heartbeat_interval_ms,
                # Reduce verbose logging from Kafka client
                "log_level": "7",  # Only show critical messages
                "log.connection.close": "false",
                "log.thread.name": "false",
            }
            
            # Get authentication configuration from authentication manager
            from ..plugin.auth import AuthenticationManager
            auth_manager = AuthenticationManager(self.config)
            auth_config = auth_manager.configure_kafka_auth()
            consumer_config.update(auth_config)
            
            # Create consumer
            self.consumer = Consumer(consumer_config)
            
            self.logger.info(f"Initialized Kafka consumer with servers: {self.config.kafka.bootstrap_servers}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise KafkaError(f"Failed to initialize Kafka consumer: {e}")
    
    def consume_topic(
        self, 
        topic_name: str, 
        max_messages: int, 
        timeout: int
    ) -> List[Tuple[Optional[bytes], bytes]]:
        """
        Consume messages from a topic.
        
        Args:
            topic_name: Name of the topic to consume from
            max_messages: Maximum number of messages to consume
            timeout: Timeout in seconds
            
        Returns:
            List of (key, value) tuples
        """
        
        # Validate inputs
        validate_topic_name(topic_name)
        validate_max_messages(max_messages)
        validate_timeout(timeout)
        
        if not self.consumer:
            raise KafkaError("Consumer not initialized")
        
        messages = []
        start_time = time.time()
        
        try:
            # Subscribe to topic
            self.consumer.subscribe([topic_name])
            self.logger.info(f"Subscribed to topic: {topic_name}")
            
            message_count = 0
            
            while message_count < max_messages:
                # Check timeout
                if time.time() - start_time > timeout:
                    self.logger.warning(f"Timeout reached after {timeout} seconds")
                    break
                
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == ConfluentKafkaError._PARTITION_EOF:
                        # End of partition reached
                        self.logger.info("Reached end of partition")
                        break
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        raise KafkaError(f"Consumer error: {msg.error()}")
                
                # Extract message
                key = msg.key()
                value = msg.value()
                
                if value is not None:  # Only process non-null values
                    messages.append((key, value))
                    message_count += 1
                    
                    if message_count % 100 == 0:
                        self.logger.debug(f"Consumed {message_count} messages from {topic_name}")
            
            self.logger.info(f"Consumed {len(messages)} messages from topic {topic_name}")
            
        except KafkaException as e:
            self.logger.error(f"Kafka exception while consuming from {topic_name}: {e}")
            raise KafkaError(f"Kafka exception: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error while consuming from {topic_name}: {e}")
            raise KafkaError(f"Unexpected error: {e}")
        finally:
            # Unsubscribe from topic
            try:
                self.consumer.unsubscribe()
            except Exception as e:
                self.logger.warning(f"Error unsubscribing from topic: {e}")
        
        return messages
    
    def consume_topics(
        self, 
        topic_names: List[str], 
        max_messages_per_topic: int, 
        timeout: int
    ) -> Dict[str, List[Tuple[Optional[bytes], bytes]]]:
        """
        Consume messages from multiple topics.
        
        Args:
            topic_names: List of topic names to consume from
            max_messages_per_topic: Maximum number of messages per topic
            timeout: Timeout in seconds per topic
            
        Returns:
            Dictionary mapping topic names to message lists
        """
        
        results = {}
        
        for topic_name in topic_names:
            try:
                self.logger.info(f"Consuming from topic: {topic_name}")
                messages = self.consume_topic(topic_name, max_messages_per_topic, timeout)
                results[topic_name] = messages
            except Exception as e:
                self.logger.error(f"Failed to consume from topic {topic_name}: {e}")
                results[topic_name] = []
        
        return results
    
    def get_topic_metadata(self, topic_name: str) -> Dict[str, Any]:
        """
        Get metadata for a topic.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Topic metadata dictionary
        """
        
        if not self.consumer:
            raise KafkaError("Consumer not initialized")
        
        try:
            # Get cluster metadata
            metadata = self.consumer.list_topics(timeout=10)
            
            if topic_name not in metadata.topics:
                raise KafkaError(f"Topic {topic_name} not found")
            
            topic_metadata = metadata.topics[topic_name]
            
            return {
                "name": topic_name,
                "partitions": len(topic_metadata.partitions),
                "error": topic_metadata.error,
                "partition_info": {
                    str(pid): {
                        "id": pid,
                        "leader": partition.leader,
                        "replicas": partition.replicas,
                        "isrs": partition.isrs,
                        "error": partition.error,
                    }
                    for pid, partition in topic_metadata.partitions.items()
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get metadata for topic {topic_name}: {e}")
            raise KafkaError(f"Failed to get topic metadata: {e}")
    
    def list_topics(self, prefix: Optional[str] = None) -> List[str]:
        """
        List available topics.
        
        Args:
            prefix: Optional prefix to filter topics
            
        Returns:
            List of topic names
        """
        
        if not self.consumer:
            raise KafkaError("Consumer not initialized")
        
        try:
            # Get cluster metadata with longer timeout
            self.logger.info("Requesting cluster metadata...")
            metadata = self.consumer.list_topics(timeout=30)
            
            topics = list(metadata.topics.keys())
            self.logger.info(f"Retrieved {len(topics)} topics from cluster metadata")
            
            # Filter by prefix if provided
            if prefix:
                topics = [t for t in topics if t.startswith(prefix)]
            
            return sorted(topics)
            
        except Exception as e:
            self.logger.error(f"Failed to list topics: {e}")
            raise KafkaError(f"Failed to list topics: {e}")
    
    def close(self) -> None:
        """Close the consumer."""
        
        if self.consumer:
            try:
                self.consumer.close()
                self.logger.info("Kafka consumer closed")
            except Exception as e:
                self.logger.warning(f"Error closing consumer: {e}")
            finally:
                self.consumer = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
