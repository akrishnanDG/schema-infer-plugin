"""
Optimistic message processing for schema inference
"""

import os
import sys
import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

# Suppress librdkafka telemetry messages
os.environ['KAFKA_LOG_LEVEL'] = '7'
os.environ['RDKAFKA_LOG_LEVEL'] = '7'

import confluent_kafka
from confluent_kafka import Consumer, KafkaError as ConfluentKafkaError, KafkaException
from tqdm import tqdm

from ..config import Config
from ..utils.exceptions import KafkaError
from ..utils.logger import get_logger


class SuppressTelemetry:
    """Context manager to suppress librdkafka telemetry messages."""

    def __enter__(self):
        os.environ['KAFKA_LOG_LEVEL'] = '7'
        os.environ['RDKAFKA_LOG_LEVEL'] = '7'
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class OptimisticProcessor:
    """Optimistic message processor that tries multiple strategies to read messages."""
    
    def __init__(self, config: Config):
        """
        Initialize optimistic processor.
        
        Args:
            config: Configuration object
        """
        
        self.config = config
        self.logger = get_logger(__name__)
        self.performance_stats = {
            'total_processed': 0,
            'total_time': 0.0,
            'avg_time_per_topic': 0.0,
            'fastest_processing': float('inf'),
            'slowest_processing': 0.0
        }
        
        # Shared consumer for connection reuse
        self._shared_consumer = None
        self._consumer_lock = threading.Lock()
    
    def _create_consumer(self, consumer_config: Dict[str, Any]) -> Consumer:
        """Create a consumer with suppressed librdkafka logging."""

        # Set log level to suppress most messages
        consumer_config.setdefault('log_level', '7')
        consumer_config.setdefault('log.connection.close', 'false')
        consumer_config.setdefault('log.thread.name', 'false')
        consumer_config.setdefault('broker.address.family', 'v4')
        consumer_config.setdefault('enable.metrics.push', False)
        consumer_config.setdefault('log.queue', 'false')
        consumer_config.setdefault('statistics.interval.ms', '0')
        consumer_config.setdefault('enable.partition.eof', 'false')

        return Consumer(consumer_config)
    
    def _get_shared_consumer(self) -> Consumer:
        """Get or create a shared consumer for connection reuse."""
        with self._consumer_lock:
            if self._shared_consumer is None:
                # Create consumer config
                consumer_config = {
                    'bootstrap.servers': self.config.kafka.bootstrap_servers,
                    'group.id': f'schema-infer-shared-{uuid.uuid4().hex[:12]}',
                    'auto.offset.reset': self.config.kafka.auto_offset_reset,
                    'session.timeout.ms': 15000,
                    'heartbeat.interval.ms': 5000,
                    'log_level': '7',
                    'log.connection.close': 'false',
                    'log.thread.name': 'false',
                    'broker.address.family': 'v4',
                    'log.queue': 'false',
                    'statistics.interval.ms': '0',
                    'enable.auto.commit': 'false',
                    'enable.partition.eof': 'false',
                    'fetch.max.bytes': 104857600,
                    'max.partition.fetch.bytes': 20971520,
                    'fetch.wait.max.ms': 50,
                    'fetch.min.bytes': 20480,
                    'metadata.max.age.ms': 2000,
                    'reconnect.backoff.ms': 25,
                    'reconnect.backoff.max.ms': 250,
                    'socket.timeout.ms': 25000,
                    'api.version.request.timeout.ms': 5000,
                    'queued.min.messages': 5000,
                    'queued.max.messages.kbytes': 131072,
                    'check.crcs': 'false',
                }
                
                # Add API version settings from config
                if hasattr(self.config.kafka, 'api_version_request'):
                    consumer_config['api.version.request'] = self.config.kafka.api_version_request
                if hasattr(self.config.kafka, 'api_version_fallback_ms'):
                    consumer_config['api.version.fallback.ms'] = self.config.kafka.api_version_fallback_ms
                
                # Add authentication if configured
                from ..plugin.auth import AuthenticationManager
                auth_manager = AuthenticationManager(self.config)
                auth_config = auth_manager.configure_kafka_auth()
                consumer_config.update(auth_config)
                
                self._shared_consumer = self._create_consumer(consumer_config)
            return self._shared_consumer
    
    def _close_shared_consumer(self):
        """Close the shared consumer."""
        with self._consumer_lock:
            if self._shared_consumer is not None:
                try:
                    self._shared_consumer.close()
                except Exception as e:
                    self.logger.debug(f"Error closing shared consumer: {e}")
                finally:
                    self._shared_consumer = None
    
    def read_topics_parallel(
        self,
        topic_list: List[str],
        max_messages: int,
        timeout: int,
        max_workers: int = 8,
        max_readers: int = 5,
        progress_callback=None,
    ) -> Dict[str, List[Tuple[Optional[bytes], bytes]]]:
        """
        Read messages from multiple topics using a small reader pool.

        Uses a small, fixed number of consumer connections (max_readers) to
        avoid broker connection saturation, independent of the processing
        worker count (max_workers). Each reader creates one Kafka consumer
        and processes a chunk of topics sequentially.

        Args:
            topic_list: Topics to read from.
            max_messages: Max messages per topic.
            timeout: Timeout per topic in seconds.
            max_workers: Ignored for reading (kept for backward compatibility).
                         Use max_readers to control consumer connections.
            max_readers: Number of consumer connections (default 5, capped at 10).
            progress_callback: Called with (completed_count, total) after each topic.

        Returns:
            Dict mapping topic name to list of (key, value) tuples.
        """
        if not topic_list:
            return {}

        # Cap readers to avoid broker connection saturation
        num_readers = min(max_readers, len(topic_list), 10)
        topic_messages: Dict[str, List[Tuple[Optional[bytes], bytes]]] = {}
        lock = threading.Lock()
        completed = [0]

        def _reader_worker(topics_chunk: List[str]) -> None:
            """Reader: create a single consumer and read from assigned topics."""
            consumer_config = {
                'bootstrap.servers': self.config.kafka.bootstrap_servers,
                'group.id': f'schema-infer-parallel-{uuid.uuid4().hex[:8]}',
                'auto.offset.reset': self.config.kafka.auto_offset_reset,
                'session.timeout.ms': 15000,
                'heartbeat.interval.ms': 5000,
                'enable.auto.commit': 'false',
                'enable.partition.eof': 'false',
                'fetch.max.bytes': 52428800,
                'max.partition.fetch.bytes': 10485760,
                'fetch.wait.max.ms': 50,
                'fetch.min.bytes': 10240,
                'queued.min.messages': 2000,
                'queued.max.messages.kbytes': 65536,
                'log_level': '7',
                'log.connection.close': 'false',
                'log.thread.name': 'false',
                'broker.address.family': 'v4',
                'enable.metrics.push': False,
                'log.queue': 'false',
                'statistics.interval.ms': '0',
                'check.crcs': 'false',
            }

            from ..plugin.auth import AuthenticationManager
            auth_manager = AuthenticationManager(self.config)
            auth_config = auth_manager.configure_kafka_auth()
            consumer_config.update(auth_config)

            consumer = self._create_consumer(consumer_config)
            try:
                for topic_name in topics_chunk:
                    try:
                        msgs = self._read_single_topic(consumer, topic_name, max_messages, timeout)
                        if msgs:
                            with lock:
                                topic_messages[topic_name] = msgs
                    except Exception as e:
                        self.logger.debug(f"Reader failed to read {topic_name}: {e}")
                    finally:
                        with lock:
                            completed[0] += 1
                        if progress_callback:
                            progress_callback(completed[0], len(topic_list))
            finally:
                try:
                    consumer.close()
                except Exception:
                    pass

        # Split topics across readers (not workers)
        chunks = [[] for _ in range(num_readers)]
        for i, topic in enumerate(topic_list):
            chunks[i % num_readers].append(topic)

        # Run readers in parallel — small pool of consumers
        with ThreadPoolExecutor(max_workers=num_readers) as executor:
            futures = [executor.submit(_reader_worker, chunk) for chunk in chunks if chunk]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.warning(f"Reader thread failed: {e}")

        return topic_messages

    def _read_single_topic(
        self,
        consumer: Consumer,
        topic_name: str,
        max_messages: int,
        timeout: int,
    ) -> List[Tuple[Optional[bytes], bytes]]:
        """Read messages from a single topic using the given consumer."""
        metadata = consumer.list_topics(topic_name, timeout=5.0)
        topic_metadata = metadata.topics.get(topic_name)

        if not topic_metadata or not topic_metadata.partitions:
            return []

        partitions = [
            confluent_kafka.TopicPartition(topic_name, p)
            for p in topic_metadata.partitions.keys()
        ]
        consumer.assign(partitions)

        # Seek to recent messages
        for partition in partitions:
            try:
                low, high = consumer.get_watermark_offsets(partition, timeout=5.0)
                if high > low:
                    target_offset = max(low, high - max_messages)
                    partition.offset = target_offset
                    consumer.seek(partition)
            except Exception:
                continue

        # Poll messages
        messages = []
        poll_start = time.time()
        consecutive_empty = 0
        got_first_message = False

        while len(messages) < max_messages and time.time() - poll_start < timeout:
            msg = consumer.poll(0.1)
            if msg is None:
                consecutive_empty += 1
                # Be patient before first message (broker fetch prep on Confluent Cloud)
                limit = 20 if got_first_message else 50
                if consecutive_empty >= limit:
                    break
                continue

            consecutive_empty = 0
            got_first_message = True

            if msg.error():
                if msg.error().code() in (
                    ConfluentKafkaError._PARTITION_EOF,
                    ConfluentKafkaError._UNKNOWN_TOPIC_OR_PART,
                ):
                    break
                continue

            try:
                msg.value().decode('utf-8', errors='ignore')
                messages.append((msg.key(), msg.value()))
            except Exception:
                continue

        return messages[:max_messages]

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close shared consumer."""
        self._close_shared_consumer()
    
    def _read_messages_from_assigned_partitions(self, consumer, partitions, max_messages: int, timeout: int, topic_name: str) -> List[Tuple[Optional[bytes], bytes]]:
        """Read messages from manually assigned partitions."""
        
        messages = []
        self.logger.info(f"Reading messages from {len(partitions)} manually assigned partitions")
        
        # Try to read from end offset (most recent messages)
        for partition in partitions:
            try:
                # Get watermark offsets
                self.logger.info(f"Getting watermark offsets for partition {partition.partition}")
                low, high = consumer.get_watermark_offsets(partition, timeout=10.0)
                self.logger.info(f"Partition {partition.partition} offsets: low={low}, high={high}")
                
                if high > low:
                    # Calculate smart offset - read from end but not too far back
                    target_offset = max(low, high - min(max_messages * 2, 10000))
                    self.logger.info(f"Seeking to offset {target_offset} on partition {partition.partition}")
                    consumer.seek(confluent_kafka.TopicPartition(topic_name, partition.partition, target_offset))
                    
                    # Batch poll for messages
                    self.logger.info(f"Starting batch poll for partition {partition.partition}")
                    partition_messages = self._batch_poll_messages(consumer, max_messages, timeout, topic_name)
                    self.logger.info(f"Read {len(partition_messages)} messages from partition {partition.partition}")
                    if partition_messages:
                        messages.extend(partition_messages)
                        if len(messages) >= max_messages:
                            break
                else:
                    # Suppress this warning - not useful for users
                    # if hasattr(self.config, 'performance') and hasattr(self.config.performance, 'verbose_logging') and self.config.performance.verbose_logging:
                    #     self.logger.warning(f"Partition {partition.partition} appears to be empty (high={high}, low={low})")
                    pass
            except Exception as e:
                # Suppress these warnings - not useful for users
                # if hasattr(self.config, 'performance') and hasattr(self.config.performance, 'verbose_logging') and self.config.performance.verbose_logging:
                #     self.logger.warning(f"Error reading from partition {partition.partition}: {e}")
                #     import traceback
                #     self.logger.warning(f"Full traceback: {traceback.format_exc()}")
                pass
        
        self.logger.info(f"Total messages read from manually assigned partitions: {len(messages)}")
        return messages[:max_messages]
    
    def _update_performance_stats(self, processing_time: float):
        """Update performance statistics."""
        with self._consumer_lock:
            self.performance_stats['total_processed'] += 1
            self.performance_stats['total_time'] += processing_time
            self.performance_stats['avg_time_per_topic'] = (
                self.performance_stats['total_time'] / self.performance_stats['total_processed']
            )
            self.performance_stats['fastest_processing'] = min(
                self.performance_stats['fastest_processing'], processing_time
            )
            self.performance_stats['slowest_processing'] = max(
                self.performance_stats['slowest_processing'], processing_time
            )
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get current performance statistics."""
        return self.performance_stats.copy()
    
    def _read_messages_from_partitions_parallel(self, consumer, topic_name: str, max_messages: int, timeout: int) -> List[Tuple[Optional[bytes], bytes]]:
        """Read messages from multiple partitions sequentially (consumer is not thread-safe)."""

        try:
            assignment = consumer.assignment()
            if not assignment:
                return []

            all_messages = []
            for partition in assignment:
                try:
                    low, high = consumer.get_watermark_offsets(partition, timeout=10.0)
                    if high > low:
                        start_offset = max(low, high - max_messages)
                        consumer.seek(confluent_kafka.TopicPartition(topic_name, partition.partition, start_offset))

                        while len(all_messages) < max_messages:
                            msg = consumer.poll(timeout=0.1)
                            if msg is None:
                                break
                            if msg.error():
                                if msg.error().code() == ConfluentKafkaError._PARTITION_EOF:
                                    break
                                continue
                            if msg.value() is not None:
                                all_messages.append((msg.key(), msg.value()))
                except Exception as e:
                    self.logger.debug(f"Failed to read from partition {partition}: {e}")

                if len(all_messages) >= max_messages:
                    break

            return all_messages[:max_messages]

        except Exception as e:
            self.logger.debug(f"Partition reading failed: {e}")
            return []
    
    def _quick_topic_check(self, topic_name: str) -> Tuple[bool, str]:
        """Quick check if topic has any messages without reading them."""
        return self._do_quick_topic_check(topic_name)
    
    def _do_quick_topic_check(self, topic_name: str) -> Tuple[bool, str]:
        """Internal method for quick topic check - simplified version."""
        
        try:
            consumer_config = {
                'bootstrap.servers': self.config.kafka.bootstrap_servers,
                'group.id': f'schema-infer-check-{uuid.uuid4().hex[:12]}',
                'auto.offset.reset': 'latest',
                'enable.auto.commit': False,
                'session.timeout.ms': 3000,  # Shorter timeout
                'heartbeat.interval.ms': 1000,
                'broker.address.family': 'v4',
                'log_level': '7',  # Suppress logs
            }

            # Get authentication configuration
            from ..plugin.auth import AuthenticationManager
            auth_manager = AuthenticationManager(self.config)
            auth_config = auth_manager.configure_kafka_auth()
            consumer_config.update(auth_config)

            consumer = self._create_consumer(consumer_config)

            try:
                # Subscribe to topic
                consumer.subscribe([topic_name])

                # Try to get one message with very short timeout
                start_time = time.time()
                while time.time() - start_time < 3:  # 3 second max
                    msg = consumer.poll(timeout=0.5)
                    if msg is None:
                        continue
                    if msg.error():
                        error_str = str(msg.error()).lower()
                        if "offset" in error_str and "out of range" in error_str:
                            return False, "Topic has messages but they may have expired due to retention policy"
                        elif "resolve" in error_str:
                            return False, "Network connectivity issue with topic"
                        else:
                            return True, "Topic is accessible"
                    else:
                        return True, "Topic has accessible messages"
                
                # If we get here, no messages were found in the quick check
                # But this doesn't mean the topic is empty - let the full strategies handle it
                return True, "Quick check inconclusive - proceeding with full processing"
                
            finally:
                consumer.close()
                
        except Exception as e:
            self.logger.debug(f"Quick topic check failed: {e}")
            return True, "Unable to check topic state - proceeding with full processing"  # Assume topic has messages if check fails
    
    def _check_topic_offsets(self, topic_name: str) -> Tuple[bool, str]:
        """
        Check if topic is truly empty by comparing beginning and end offsets for all partitions.
        Returns (is_empty, reason)
        """
        
        try:
            consumer_config = {
                'bootstrap.servers': self.config.kafka.bootstrap_servers,
                'group.id': f'schema-infer-offset-check-{uuid.uuid4().hex[:12]}',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'session.timeout.ms': 3000,  # Shorter timeout
                'heartbeat.interval.ms': 1000,
                'broker.address.family': 'v4',
                'log_level': '7',  # Suppress logs
            }

            # Get authentication configuration
            from ..plugin.auth import AuthenticationManager
            auth_manager = AuthenticationManager(self.config)
            auth_config = auth_manager.configure_kafka_auth()
            consumer_config.update(auth_config)

            consumer = self._create_consumer(consumer_config)

            try:
                # Get topic metadata first
                metadata = consumer.list_topics(topic_name, timeout=3.0)
                topic_metadata = metadata.topics.get(topic_name)
                
                if not topic_metadata:
                    return False, "Topic not found"
                
                # Subscribe to topic to get partition assignments
                consumer.subscribe([topic_name])
                
                # Wait for assignment with shorter timeout
                assignment = None
                start_time = time.time()
                while time.time() - start_time < 2:  # 2 second max
                    assignment = consumer.assignment()
                    if assignment:
                        break
                    time.sleep(0.1)
                
                if not assignment:
                    return False, "Unable to get topic assignment"
                
                # Check offsets for each partition
                total_messages = 0
                empty_partitions = 0
                total_partitions = len(assignment)
                
                # For large partition counts, sample to avoid timeout
                max_partitions_to_check = 20  # Reduced limit
                partitions_to_check = list(assignment)
                
                if total_partitions > max_partitions_to_check:
                    # Sample partitions evenly
                    step = total_partitions // max_partitions_to_check
                    partitions_to_check = [assignment[i] for i in range(0, total_partitions, step)][:max_partitions_to_check]
                    self.logger.info(f"Topic has {total_partitions} partitions, sampling {len(partitions_to_check)} partitions")
                
                for partition in partitions_to_check:
                    try:
                        low, high = consumer.get_watermark_offsets(partition, timeout=10.0)
                        partition_messages = high - low
                        total_messages += partition_messages
                        if partition_messages == 0:
                            empty_partitions += 1
                    except Exception as e:
                        self.logger.debug(f"Failed to get offsets for partition {partition}: {e}")
                
                # Scale up results if we sampled
                if total_partitions > max_partitions_to_check:
                    scale_factor = total_partitions / len(partitions_to_check)
                    total_messages = int(total_messages * scale_factor)
                    empty_partitions = int(empty_partitions * scale_factor)
                
                # Determine topic state
                if total_messages == 0:
                    if total_partitions > max_partitions_to_check:
                        return True, f"Topic is truly empty (sampled {len(partitions_to_check)} of {total_partitions} partitions)"
                    else:
                        return True, "Topic is truly empty (no messages in any partition)"
                elif empty_partitions == total_partitions:
                    if total_partitions > max_partitions_to_check:
                        return True, f"Topic is truly empty (sampled {len(partitions_to_check)} of {total_partitions} partitions)"
                    else:
                        return True, "Topic is truly empty (all partitions have same beginning and end offsets)"
                else:
                    if total_partitions > max_partitions_to_check:
                        return False, f"Topic has approximately {total_messages} messages across {total_partitions} partitions (sampled {len(partitions_to_check)} partitions)"
                    else:
                        return False, f"Topic has {total_messages} messages across {total_partitions} partitions"
                
            finally:
                consumer.close()
                
        except Exception as e:
            self.logger.debug(f"Offset check failed: {e}")
            return False, "Unable to check topic offsets - proceeding with full processing"
    
    def read_latest_messages(
        self, 
        topic_name: str, 
        max_messages: int,
        timeout: int = 10  # Reduced from 30 to 10 seconds
    ) -> List[Tuple[Optional[bytes], bytes]]:
        """
        Read the latest messages from a topic using optimistic approach.
        
        Args:
            topic_name: Name of the topic
            max_messages: Maximum number of messages to read
            timeout: Timeout in seconds
            
        Returns:
            List of (key, value) tuples
        """
        
        # Suppress verbose logging for cleaner output
        if self.config.performance.verbose_logging:
            self.logger.info(f"Reading latest {max_messages} messages from topic: {topic_name}")
        start_time = time.time()
        
        # First, check if topic is truly empty
        try:
            is_empty, reason = self._check_topic_offsets(topic_name)

            if is_empty:
                print(f"⚠️  {reason}")
                return []
        except Exception as e:
            self.logger.debug(f"Offset check failed: {e}")
            print(f"⚠️  Unable to determine topic state - proceeding with message reading")
        
        # Use single optimized strategy to reduce connection load
        try:
            result = self._strategy_optimized(topic_name, max_messages, timeout)
            if result:
                elapsed_time = time.time() - start_time
                if self.config.performance.verbose_logging:
                    self.logger.info(f"Successfully read {len(result)} messages using optimized strategy in {elapsed_time:.2f}s")
                # Only show progress output if verbose logging is enabled or progress bars are disabled
                if self.config.performance.verbose_logging or not self.config.performance.show_progress:
                    print(f"  ✅ {len(result)} messages read")
                self._update_performance_stats(elapsed_time)
                return result
        except Exception as e:
            # Suppress this warning - not useful for users
            # if hasattr(self.config, 'performance') and hasattr(self.config.performance, 'verbose_logging') and self.config.performance.verbose_logging:
            #     self.logger.warning(f"Optimized strategy failed: {e}")
            pass
        
        # If optimized strategy fails, try simple fallback
        try:
            result = self._strategy_simple_fallback(topic_name, max_messages, timeout)
            if result:
                elapsed_time = time.time() - start_time
                if self.config.performance.verbose_logging:
                    self.logger.info(f"Successfully read {len(result)} messages using fallback strategy in {elapsed_time:.2f}s")
                # Only show progress output if verbose logging is enabled or progress bars are disabled
                if self.config.performance.verbose_logging or not self.config.performance.show_progress:
                    print(f"  ✅ {len(result)} messages read")
                self._update_performance_stats(elapsed_time)
                return result
        except Exception as e:
            # Suppress this warning - not useful for users
            # if hasattr(self.config, 'performance') and hasattr(self.config.performance, 'verbose_logging') and self.config.performance.verbose_logging:
            #     self.logger.warning(f"Fallback strategy failed: {e}")
            pass
        
        # If all strategies fail
        elapsed_time = time.time() - start_time
        # Suppress this warning - not useful for users
        # if hasattr(self.config, 'performance') and hasattr(self.config.performance, 'verbose_logging') and self.config.performance.verbose_logging:
        #     self.logger.warning(f"All strategies failed to read messages from topic: {topic_name}")
        print(f"  ⚠️  {topic_name} - no messages found")
        return []
    
    def read_messages_shared_consumer(self, topic_name: str, max_messages: int, timeout: int) -> List[Tuple[Optional[bytes], bytes]]:
        """Read messages using shared consumer for better performance."""
        consumer = self._get_shared_consumer()
        
        try:
            # Get topic metadata to find partitions
            metadata = consumer.list_topics(topic_name, timeout=5.0)
            topic_metadata = metadata.topics.get(topic_name)
            
            if not topic_metadata or not topic_metadata.partitions:
                if self.config.performance.verbose_logging:
                    self.logger.warning(f"Topic {topic_name} not found or has no partitions")
                return []
            
            # Manually assign partitions instead of subscribing
            partitions = [confluent_kafka.TopicPartition(topic_name, p) for p in topic_metadata.partitions.keys()]
            consumer.assign(partitions)
            
            # Seek to latest offset for each partition to read recent messages
            for partition in partitions:
                try:
                    # Get watermark offsets
                    low, high = consumer.get_watermark_offsets(partition, timeout=5.0)
                    if high > low:
                        # Seek to a position that will give us recent messages
                        # For max_messages, we want to read from the end backwards
                        target_offset = max(low, high - max_messages)
                        partition.offset = target_offset
                        consumer.seek(partition)
                except Exception as e:
                    if self.config.performance.verbose_logging:
                        self.logger.debug(f"Failed to seek partition {partition.partition}: {e}")
                    continue
            
            # Read messages from assigned partitions with optimized polling
            messages = []
            valid_messages = 0
            poll_start = time.time()
            batch_count = 0
            max_batches = max(200, max_messages // 2)  # Reduced batch limit
            consecutive_empty_polls = 0
            got_first_message = False
            # The first fetch after assign()+seek() can take several seconds on
            # Confluent Cloud while the broker prepares the fetch response.  Be
            # patient initially, then tighten up once data starts flowing.
            max_empty_polls_initial = 50   # 5s patience before first message
            max_empty_polls_streaming = 20  # 2s patience once data is flowing

            while valid_messages < max_messages and time.time() - poll_start < timeout and batch_count < max_batches:
                batch_count += 1

                msg = consumer.poll(0.1)

                if msg is None:
                    consecutive_empty_polls += 1
                    current_limit = max_empty_polls_streaming if got_first_message else max_empty_polls_initial
                    if consecutive_empty_polls >= current_limit:
                        break
                    continue

                consecutive_empty_polls = 0  # Reset counter on successful poll
                got_first_message = True
                    
                if msg.error():
                    if msg.error().code() == ConfluentKafkaError._PARTITION_EOF:
                        break
                    elif msg.error().code() == ConfluentKafkaError._UNKNOWN_TOPIC_OR_PART:
                        break
                    else:
                        continue
                
                try:
                    # Try to decode as text to validate
                    msg.value().decode('utf-8', errors='ignore')
                    valid_messages += 1
                    messages.append((msg.key(), msg.value()))
                    
                    if valid_messages >= max_messages:
                        break
                        
                except Exception:
                    continue
            
            return messages[:max_messages] if messages else []
            
        except Exception as e:
            if self.config.performance.verbose_logging:
                self.logger.debug(f"Shared consumer reading failed for {topic_name}: {e}")
            return []
        finally:
            # Unassign partitions to prepare for next topic
            try:
                consumer.unassign()
            except Exception:
                pass
    
    def _strategy_optimized(self, topic_name: str, max_messages: int, timeout: int) -> List[Tuple[Optional[bytes], bytes]]:
        """Highly optimized strategy: Batch reading with smart offset selection and message filtering."""
        
        consumer_config = {
                'bootstrap.servers': self.config.kafka.bootstrap_servers,
                'group.id': f'schema-infer-opt-{uuid.uuid4().hex[:12]}',
                'auto.offset.reset': self.config.kafka.auto_offset_reset,
                'session.timeout.ms': 15000,  # Balanced for reliability
                'heartbeat.interval.ms': 5000,  # Balanced for reliability
                'log_level': '7',
                'statistics.interval.ms': '0',  # Disable statistics to reduce telemetry
                'enable.auto.commit': 'false',  # Disable auto-commit telemetry
                'enable.partition.eof': 'false',  # Disable EOF telemetry
                'log.connection.close': 'false',  # Disable connection close logs
                'log.thread.name': 'false',  # Disable thread name logs
                'broker.address.family': 'v4',  # Skip IPv6 attempts
                'log.queue': 'false',  # Disable queue logs
                # Aggressive batch settings for maximum speed
                'fetch.max.bytes': 104857600,  # 100MB fetch size (doubled for speed)
                'max.partition.fetch.bytes': 20971520,  # 20MB per partition (doubled for speed)
                'fetch.wait.max.ms': 50,  # Reduced for speed
                'fetch.min.bytes': 20480,  # Doubled for better batching
                # Aggressive settings for maximum speed
                'metadata.max.age.ms': 2000,  # Reduced for speed
                'reconnect.backoff.ms': 25,  # Reduced for speed
                'reconnect.backoff.max.ms': 250,  # Reduced for speed
                'socket.timeout.ms': 25000,  # Balanced for reliability
                'api.version.request.timeout.ms': 5000,  # Balanced for reliability
                # Maximum speed optimizations
                'queued.min.messages': 5000,  # Increased for speed
                'queued.max.messages.kbytes': 131072,  # 128MB message buffer (doubled)
                'check.crcs': 'false',  # Skip CRC checks for speed
            }
        
        # Add API version settings from config
        if hasattr(self.config.kafka, 'api_version_request'):
            consumer_config['api.version.request'] = self.config.kafka.api_version_request
        if hasattr(self.config.kafka, 'api_version_fallback_ms'):
            consumer_config['api.version.fallback.ms'] = self.config.kafka.api_version_fallback_ms
        
        # Add authentication if configured
        from ..plugin.auth import AuthenticationManager
        auth_manager = AuthenticationManager(self.config)
        auth_config = auth_manager.configure_kafka_auth()
        consumer_config.update(auth_config)
        
        consumer = self._create_consumer(consumer_config)
        
        try:
            # Subscribe to topic
            consumer.subscribe([topic_name])
            
            # Wait for assignment with balanced timeout
            assignment_timeout = 5
            start_time = time.time()
            self.logger.info(f"Waiting for partition assignment for topic: {topic_name}")
            while time.time() - start_time < assignment_timeout:
                partitions = consumer.assignment()
                if partitions:
                    self.logger.info(f"Got {len(partitions)} partitions assigned: {[p.partition for p in partitions]}")
                    break
                consumer.poll(0.5)
            
            if not partitions:
                # Suppress this warning - not useful for users
                # if hasattr(self.config, 'performance') and hasattr(self.config.performance, 'verbose_logging') and self.config.performance.verbose_logging:
                #     self.logger.warning("No partitions assigned within timeout")
                # Try to get topic metadata to debug
                try:
                    metadata = consumer.list_topics(topic_name, timeout=3.0)
                    topic_metadata = metadata.topics.get(topic_name)
                    if topic_metadata:
                        self.logger.info(f"Topic metadata found: {topic_metadata}")
                        # Try to manually assign partitions
                        partitions = [confluent_kafka.TopicPartition(topic_name, p) for p in topic_metadata.partitions.keys()]
                        if partitions:
                            self.logger.info(f"Manually assigning {len(partitions)} partitions: {[p.partition for p in partitions]}")
                            consumer.assign(partitions)
                            # Check if assignment worked
                            assigned = consumer.assignment()
                            if assigned:
                                self.logger.info(f"Manual assignment successful: {[p.partition for p in assigned]}")
                                # Continue with message reading
                                return self._read_messages_from_assigned_partitions(consumer, partitions, max_messages, timeout, topic_name)
                    else:
                        self.logger.warning(f"Topic {topic_name} not found in metadata")
                except Exception as e:
                    self.logger.warning(f"Failed to get topic metadata: {e}")
                return []
            
            # Smart offset selection - read from all partitions to collect more messages
            all_messages = []
            messages_per_partition = max_messages // len(partitions) if partitions else max_messages
            
            # Create progress bar for partition reading
            progress_bar = tqdm(
                total=len(partitions),
                desc="Reading",
                unit="",
                disable=not self.config.performance.show_progress,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}',
                leave=False
            )
            
            # Read from partitions sequentially (consumer is not thread-safe)
            for partition in partitions:
                try:
                    partition_messages = self._read_partition_optimized(consumer, partition, messages_per_partition, timeout, topic_name)
                    if partition_messages:
                        all_messages.extend(partition_messages)
                        if self.config.performance.verbose_logging:
                            self.logger.info(f"Read {len(partition_messages)} messages from partition {partition.partition}, total: {len(all_messages)}")

                        if len(all_messages) >= max_messages:
                            progress_bar.close()
                            return all_messages[:max_messages]
                except Exception as e:
                    self.logger.debug(f"Error reading from partition {partition}: {e}")
                    continue

                progress_bar.update(1)
            
            # Fallback to sequential reading if parallel reading fails
            if not all_messages:
                for partition in partitions:
                    try:
                        # Get watermark offsets
                        low, high = consumer.get_watermark_offsets(partition, timeout=2.0)
                        if self.config.performance.verbose_logging:
                            self.logger.info(f"Partition {partition.partition} offsets: low={low}, high={high}")
                        
                        if high > low:
                            # Calculate smart offset - read from end but not too far back
                            target_offset = max(low, high - min(messages_per_partition * 2, 5000))  # Read 2x per partition or 5k, whichever is smaller
                            if self.config.performance.verbose_logging:
                                self.logger.info(f"Seeking to offset {target_offset} on partition {partition.partition}")
                            consumer.seek(confluent_kafka.TopicPartition(topic_name, partition.partition, target_offset))
                            
                            # Batch poll for messages from this partition
                            partition_messages = self._batch_poll_messages(consumer, messages_per_partition, timeout, topic_name)
                            if partition_messages:
                                all_messages.extend(partition_messages)
                                if self.config.performance.verbose_logging:
                                    self.logger.info(f"Read {len(partition_messages)} messages from partition {partition.partition}, total: {len(all_messages)}")
                                
                                # Update progress bar (simplified)
                                if self.config.performance.verbose_logging:
                                    progress_bar.set_postfix({
                                        'messages': len(all_messages),
                                        'target': max_messages
                                    })
                                
                                # If we have enough messages, return early
                                if len(all_messages) >= max_messages:
                                    if self.config.performance.verbose_logging:
                                        self.logger.info(f"Collected {len(all_messages)} messages from all partitions, returning early")
                                    progress_bar.close()
                                    return all_messages[:max_messages]
                                
                    except Exception as e:
                        self.logger.debug(f"Error reading from partition {partition}: {e}")
                        continue
                    
                    # Update progress bar for each partition processed
                    progress_bar.update(1)
            
            if all_messages:
                self.logger.info(f"Collected {len(all_messages)} messages from all partitions")
                progress_bar.close()
                return all_messages[:max_messages]
            
            # If we still don't have enough messages, try reading from beginning of all partitions
            if len(all_messages) < max_messages:
                self.logger.info(f"Only got {len(all_messages)} messages from end offsets, trying beginning offsets for more messages")
                for partition in partitions:
                    try:
                        low, high = consumer.get_watermark_offsets(partition, timeout=10.0)
                        if high > low:
                            self.logger.info(f"Seeking to beginning offset {low} on partition {partition.partition}")
                            consumer.seek(confluent_kafka.TopicPartition(topic_name, partition.partition, low))
                            
                            # Read additional messages from beginning
                            additional_messages = self._batch_poll_messages(consumer, max_messages - len(all_messages), timeout, topic_name)
                            if additional_messages:
                                all_messages.extend(additional_messages)
                                self.logger.info(f"Added {len(additional_messages)} messages from beginning of partition {partition.partition}, total: {len(all_messages)}")
                                if len(all_messages) >= max_messages:
                                    break
                                
                    except Exception as e:
                        self.logger.debug(f"Error reading from beginning offset for partition {partition}: {e}")
                        continue
            
            progress_bar.close()
            return all_messages[:max_messages] if all_messages else []
            
        finally:
            consumer.close()
    
    def _batch_poll_messages(self, consumer, max_messages: int, timeout: int, topic_name: str) -> List[Tuple[Optional[bytes], bytes]]:
        """Optimized batch message polling with early termination and filtering."""
        
        messages = []
        valid_messages = 0
        poll_start = time.time()
        batch_count = 0
        max_batches = max(500, max_messages)  # Increased batch limit for speed
        
        while valid_messages < max_messages and time.time() - poll_start < timeout and batch_count < max_batches:
            batch_count += 1
            
            # Batch poll - get multiple messages at once
            msg = consumer.poll(0.05)  # Reduced poll time for speed
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    self.logger.debug("Reached end of partition")
                    break
                # Suppress this warning - not useful for users
                # if hasattr(self.config, 'performance') and hasattr(self.config.performance, 'verbose_logging') and self.config.performance.verbose_logging:
                #     self.logger.warning(f"Message error: {msg.error()}")
                continue
            
            # Message filtering - skip empty or invalid messages early
            if msg.value() is None or len(msg.value()) == 0:
                continue
                
            # Check if message contains valid text data
            try:
                # Try to decode as text to validate
                msg.value().decode('utf-8', errors='ignore')
                valid_messages += 1
                messages.append((msg.key(), msg.value()))
                
                # Early termination - if we have enough valid messages, stop immediately
                if valid_messages >= max_messages:
                    if self.config.performance.verbose_logging:
                        self.logger.info(f"Found {valid_messages} valid messages, stopping early")
                    break
                    
            except Exception as e:
                self.logger.debug(f"Message validation failed: {e}")
                continue
        
        if self.config.performance.verbose_logging:
            self.logger.info(f"Batch polling completed: {valid_messages} valid messages from {batch_count} batches (target: {max_messages})")
        if valid_messages < max_messages:
            # Only show this warning if verbose logging is enabled
            # Suppress this warning - not useful for users
            # if hasattr(self.config, 'performance') and hasattr(self.config.performance, 'verbose_logging') and self.config.performance.verbose_logging:
            #     self.logger.warning(f"Only collected {valid_messages} messages out of {max_messages} requested. This might be due to topic having limited messages or reaching timeout/batch limits.")
            pass
        return messages
    
    def _read_partition_optimized(self, consumer, partition, messages_per_partition: int, timeout: int, topic_name: str) -> List[Tuple[Optional[bytes], bytes]]:
        """Read messages from a single partition with optimized settings."""
        try:
            # Get watermark offsets
            low, high = consumer.get_watermark_offsets(partition, timeout=3.0)
            if self.config.performance.verbose_logging:
                self.logger.info(f"Partition {partition.partition} offsets: low={low}, high={high}")
            
            if high > low:
                # Calculate smart offset - read from end but not too far back
                target_offset = max(low, high - min(messages_per_partition * 2, 5000))
                if self.config.performance.verbose_logging:
                    self.logger.info(f"Seeking to offset {target_offset} on partition {partition.partition}")
                consumer.seek(confluent_kafka.TopicPartition(topic_name, partition.partition, target_offset))
                
                # Batch poll for messages from this partition
                return self._batch_poll_messages(consumer, messages_per_partition, timeout, topic_name)
        except Exception as e:
            self.logger.debug(f"Error reading from partition {partition}: {e}")
        
        return []
    
    def _strategy_simple_fallback(self, topic_name: str, max_messages: int, timeout: int) -> List[Tuple[Optional[bytes], bytes]]:
        """Optimized fallback strategy: Batch consumer with smart sampling."""
        
        consumer_config = {
            'bootstrap.servers': self.config.kafka.bootstrap_servers,
            'group.id': f'schema-infer-fallback-{uuid.uuid4().hex[:12]}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Disable auto-commit for better control
            'session.timeout.ms': 20000,  # Stable session timeout
            'heartbeat.interval.ms': 6000,  # Stable heartbeat interval
            'log_level': '7',
            'log.connection.close': 'false',
            'log.thread.name': 'false',
            'broker.address.family': 'v4',
            'log.queue': 'false',
            'statistics.interval.ms': '0',  # Disable statistics to reduce telemetry
            # Aggressive batch settings for maximum speed
            'fetch.max.bytes': 52428800,  # 50MB fetch size for maximum speed
            'max.partition.fetch.bytes': 10485760,  # 10MB per partition for maximum speed
            'fetch.wait.max.ms': 100,  # Balanced wait time for speed and stability
            'fetch.min.bytes': 10240,  # Higher min bytes for better batching
            # Aggressive settings for maximum speed
            'metadata.max.age.ms': 5000,  # Very fast metadata refresh
            'reconnect.backoff.ms': 50,  # Very fast reconnect
            'reconnect.backoff.max.ms': 500,  # Minimal max reconnect backoff
            'socket.timeout.ms': 30000,  # Reduced socket timeout
            'api.version.request.timeout.ms': 3000,  # Minimal API version timeout
            # Maximum speed optimizations
            'queued.min.messages': 2000,  # Buffer many more messages
            'queued.max.messages.kbytes': 65536,  # 64MB message buffer
            'enable.partition.eof': 'false',  # Don't wait for EOF
            'check.crcs': 'false',  # Skip CRC checks for speed
        }
        
        # Add API version settings from config
        if hasattr(self.config.kafka, 'api_version_request'):
            consumer_config['api.version.request'] = self.config.kafka.api_version_request
        if hasattr(self.config.kafka, 'api_version_fallback_ms'):
            consumer_config['api.version.fallback.ms'] = self.config.kafka.api_version_fallback_ms
        
        # Add authentication if configured
        from ..plugin.auth import AuthenticationManager
        auth_manager = AuthenticationManager(self.config)
        auth_config = auth_manager.configure_kafka_auth()
        consumer_config.update(auth_config)
        
        consumer = self._create_consumer(consumer_config)
        
        try:
            consumer.subscribe([topic_name])
            
            # Wait for assignment with balanced timeout
            assignment_timeout = 5
            start_time = time.time()
            self.logger.info(f"Fallback strategy: Waiting for partition assignment for topic: {topic_name}")
            while time.time() - start_time < assignment_timeout:
                partitions = consumer.assignment()
                if partitions:
                    self.logger.info(f"Fallback strategy: Got {len(partitions)} partitions assigned: {[p.partition for p in partitions]}")
                    break
                consumer.poll(0.5)
            
            if not partitions:
                # Suppress this warning - not useful for users
                # if hasattr(self.config, 'performance') and hasattr(self.config.performance, 'verbose_logging') and self.config.performance.verbose_logging:
                #     self.logger.warning("No partitions assigned in fallback strategy")
                # Try to get topic metadata to debug
                try:
                    metadata = consumer.list_topics(topic_name, timeout=3.0)
                    topic_metadata = metadata.topics.get(topic_name)
                    if topic_metadata:
                        self.logger.info(f"Fallback strategy: Topic metadata found: {topic_metadata}")
                    else:
                        self.logger.warning(f"Fallback strategy: Topic {topic_name} not found in metadata")
                except Exception as e:
                    self.logger.warning(f"Fallback strategy: Failed to get topic metadata: {e}")
                return []
            
            # Smart sampling - read from multiple points in the topic
            messages = []
            
            # Try to get a sample from different parts of the topic
            for partition in partitions:
                try:
                    low, high = consumer.get_watermark_offsets(partition, timeout=10.0)
                    if high > low:
                        # Sample from beginning, middle, and end
                        sample_points = [
                            low,  # Beginning
                            low + (high - low) // 3,  # 1/3 point
                            low + 2 * (high - low) // 3,  # 2/3 point
                            max(low, high - 1000)  # Near end
                        ]
                        
                        for sample_offset in sample_points:
                            if sample_offset >= high:
                                continue
                                
                            consumer.seek(partition, sample_offset)
                            
                            # Read a small batch from this point
                            sample_messages = self._batch_poll_messages(consumer, max_messages // 4, timeout // 4, topic_name)
                            messages.extend(sample_messages)
                            
                            # If we have enough messages, stop sampling
                            if len(messages) >= max_messages:
                                break
                        
                        if messages:
                            break  # Found messages, no need to try other partitions
                            
                except Exception as e:
                    self.logger.debug(f"Error sampling from partition {partition}: {e}")
                    continue
            
            return messages[:max_messages]
            
        finally:
            consumer.close()
    
