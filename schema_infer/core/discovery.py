"""
Topic discovery functionality for Schema Inference Plugin
"""

import re
from typing import List, Optional, Set

from ..config import Config
from ..core.consumer import KafkaConsumer
from ..utils.exceptions import ValidationError
from ..utils.logger import get_logger
from ..utils.validators import validate_topic_name


class TopicDiscovery:
    """Handles topic discovery and filtering."""
    
    def __init__(self, config: Config):
        """
        Initialize topic discovery.
        
        Args:
            config: Configuration object
        """
        
        self.config = config
        self.logger = get_logger(__name__)
    
    def _should_exclude_topic(self, topic_name: str, exclude_internal: Optional[bool] = None) -> bool:
        """
        Check if a topic should be excluded based on filtering criteria.
        
        Args:
            topic_name: Name of the topic to check
            exclude_internal: Whether to exclude internal topics (uses config default if None)
            
        Returns:
            True if topic should be excluded, False otherwise
        """
        # Use config default if not specified
        if exclude_internal is None:
            exclude_internal = self.config.topic_filter.exclude_internal
        
        if not exclude_internal:
            return False
        
        # Check internal prefix
        if topic_name.startswith(self.config.topic_filter.internal_prefix):
            return True
        
        # Check additional exclude prefixes
        for prefix in self.config.topic_filter.additional_exclude_prefixes:
            if topic_name.startswith(prefix):
                return True
        
        # Check include patterns (override exclusions)
        for pattern in self.config.topic_filter.include_patterns:
            if re.match(pattern, topic_name):
                return False
        
        return False
    
    def discover_topics(
        self,
        topic: Optional[str] = None,
        topics: Optional[str] = None,
        topic_prefix: Optional[str] = None,
        topic_pattern: Optional[str] = None,
        exclude_internal: Optional[bool] = None,
        show_metadata: bool = False
    ) -> List[str]:
        """
        Discover topics based on various criteria.
        
        Args:
            topic: Single topic name
            topics: Comma-separated list of topic names
            topic_prefix: Prefix to match topics
            topic_pattern: Regex pattern to match topics
            exclude_internal: Whether to exclude internal topics (uses config default if None)
            show_metadata: Whether to show topic metadata
            
        Returns:
            List of discovered topic names
        """
        
        discovered_topics = set()
        
        # Single topic
        if topic:
            validate_topic_name(topic)
            discovered_topics.add(topic)
        
        # Comma-separated topics
        if topics:
            topic_list = [t.strip() for t in topics.split(",")]
            for t in topic_list:
                if t:  # Skip empty strings
                    validate_topic_name(t)
                    discovered_topics.add(t)
        
        # Prefix-based discovery
        if topic_prefix:
            prefix_topics = self._discover_by_prefix(topic_prefix, exclude_internal)
            discovered_topics.update(prefix_topics)
        
        # Pattern-based discovery
        if topic_pattern:
            pattern_topics = self._discover_by_pattern(topic_pattern, exclude_internal)
            discovered_topics.update(pattern_topics)
        
        # If no specific criteria provided, list all topics
        if not any([topic, topics, topic_prefix, topic_pattern]):
            all_topics = self._list_all_topics(exclude_internal)
            discovered_topics.update(all_topics)
        
        # Apply filtering to all discovered topics
        filtered_topics = []
        for topic_name in discovered_topics:
            if not self._should_exclude_topic(topic_name, exclude_internal):
                filtered_topics.append(topic_name)
        
        discovered_topics = set(filtered_topics)
        
        result = sorted(list(discovered_topics))
        
        self.logger.info(f"Discovered {len(result)} topics")
        return result
    
    def _discover_by_prefix(self, prefix: str, exclude_internal: Optional[bool] = None) -> List[str]:
        """
        Discover topics by prefix.
        
        Args:
            prefix: Topic name prefix
            exclude_internal: Whether to exclude internal topics (uses config default if None)
            
        Returns:
            List of matching topic names
        """
        
        try:
            with KafkaConsumer(self.config) as consumer:
                all_topics = consumer.list_topics()
                
                # Filter by prefix
                matching_topics = [t for t in all_topics if t.startswith(prefix)]
                
                # Apply topic filtering
                filtered_topics = []
                for topic_name in matching_topics:
                    if not self._should_exclude_topic(topic_name, exclude_internal):
                        filtered_topics.append(topic_name)
                
                self.logger.info(f"Found {len(filtered_topics)} topics with prefix '{prefix}'")
                return filtered_topics
                
        except Exception as e:
            self.logger.error(f"Failed to discover topics by prefix '{prefix}': {e}")
            return []
    
    def _discover_by_pattern(self, pattern: str, exclude_internal: Optional[bool] = None) -> List[str]:
        """
        Discover topics by regex pattern.
        
        Args:
            pattern: Regex pattern to match
            exclude_internal: Whether to exclude internal topics (uses config default if None)
            
        Returns:
            List of matching topic names
        """
        
        try:
            # Compile regex pattern
            regex = re.compile(pattern)
            
            with KafkaConsumer(self.config) as consumer:
                all_topics = consumer.list_topics()
                
                # Filter by pattern
                matching_topics = [t for t in all_topics if regex.match(t)]
                
                # Apply topic filtering
                filtered_topics = []
                for topic_name in matching_topics:
                    if not self._should_exclude_topic(topic_name, exclude_internal):
                        filtered_topics.append(topic_name)
                
                self.logger.info(f"Found {len(filtered_topics)} topics matching pattern '{pattern}'")
                return filtered_topics
                
        except re.error as e:
            self.logger.error(f"Invalid regex pattern '{pattern}': {e}")
            raise ValidationError(f"Invalid regex pattern: {e}")
        except Exception as e:
            self.logger.error(f"Failed to discover topics by pattern '{pattern}': {e}")
            return []
    
    def _list_all_topics(self, exclude_internal: Optional[bool] = None) -> List[str]:
        """
        List all available topics.
        
        Args:
            exclude_internal: Whether to exclude internal topics (uses config default if None)
            
        Returns:
            List of all topic names
        """
        
        try:
            with KafkaConsumer(self.config) as consumer:
                all_topics = consumer.list_topics()
                
                # Apply topic filtering
                filtered_topics = []
                for topic_name in all_topics:
                    if not self._should_exclude_topic(topic_name, exclude_internal):
                        filtered_topics.append(topic_name)
                
                self.logger.info(f"Found {len(filtered_topics)} total topics")
                return filtered_topics
                
        except Exception as e:
            self.logger.error(f"Failed to list topics: {e}")
            return []
    
    def get_topic_metadata(self, topic_names: List[str]) -> dict:
        """
        Get metadata for multiple topics.
        
        Args:
            topic_names: List of topic names
            
        Returns:
            Dictionary mapping topic names to metadata
        """
        
        metadata = {}
        
        try:
            with KafkaConsumer(self.config) as consumer:
                for topic_name in topic_names:
                    try:
                        topic_metadata = consumer.get_topic_metadata(topic_name)
                        metadata[topic_name] = topic_metadata
                    except Exception as e:
                        self.logger.warning(f"Failed to get metadata for topic {topic_name}: {e}")
                        metadata[topic_name] = {"error": str(e)}
                        
        except Exception as e:
            self.logger.error(f"Failed to get topic metadata: {e}")
        
        return metadata
    
    def filter_topics_by_criteria(
        self,
        topics: List[str],
        min_partitions: Optional[int] = None,
        max_partitions: Optional[int] = None,
        exclude_empty: bool = False,
        exclude_system: bool = True
    ) -> List[str]:
        """
        Filter topics by various criteria.
        
        Args:
            topics: List of topic names to filter
            min_partitions: Minimum number of partitions
            max_partitions: Maximum number of partitions
            exclude_empty: Whether to exclude topics with no messages
            exclude_system: Whether to exclude system topics
            
        Returns:
            Filtered list of topic names
        """
        
        filtered_topics = []
        
        try:
            with KafkaConsumer(self.config) as consumer:
                for topic_name in topics:
                    # Exclude system topics
                    if exclude_system and topic_name.startswith("__"):
                        continue
                    
                    try:
                        # Get topic metadata
                        metadata = consumer.get_topic_metadata(topic_name)
                        
                        # Check partition count
                        partition_count = len(metadata.get("partition_info", {}))
                        
                        if min_partitions and partition_count < min_partitions:
                            continue
                        
                        if max_partitions and partition_count > max_partitions:
                            continue
                        
                        # Check for errors
                        if metadata.get("error"):
                            self.logger.warning(f"Topic {topic_name} has errors: {metadata['error']}")
                            continue
                        
                        filtered_topics.append(topic_name)
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to filter topic {topic_name}: {e}")
                        continue
                        
        except Exception as e:
            self.logger.error(f"Failed to filter topics: {e}")
            return topics  # Return original list if filtering fails
        
        self.logger.info(f"Filtered {len(topics)} topics down to {len(filtered_topics)}")
        return filtered_topics
    
    def validate_topics(self, topic_names: List[str]) -> dict:
        """
        Validate a list of topic names.
        
        Args:
            topic_names: List of topic names to validate
            
        Returns:
            Dictionary with validation results
        """
        
        results = {
            "valid": [],
            "invalid": [],
            "not_found": [],
            "accessible": [],
            "inaccessible": []
        }
        
        try:
            with KafkaConsumer(self.config) as consumer:
                available_topics = set(consumer.list_topics())
                
                for topic_name in topic_names:
                    # Validate topic name format
                    try:
                        validate_topic_name(topic_name)
                        results["valid"].append(topic_name)
                    except ValidationError as e:
                        results["invalid"].append({"topic": topic_name, "error": str(e)})
                        continue
                    
                    # Check if topic exists
                    if topic_name not in available_topics:
                        results["not_found"].append(topic_name)
                        continue
                    
                    # Check if topic is accessible
                    try:
                        metadata = consumer.get_topic_metadata(topic_name)
                        if metadata.get("error"):
                            results["inaccessible"].append({"topic": topic_name, "error": metadata["error"]})
                        else:
                            results["accessible"].append(topic_name)
                    except Exception as e:
                        results["inaccessible"].append({"topic": topic_name, "error": str(e)})
                        
        except Exception as e:
            self.logger.error(f"Failed to validate topics: {e}")
            # Mark all topics as inaccessible
            for topic_name in topic_names:
                results["inaccessible"].append({"topic": topic_name, "error": str(e)})
        
        return results
