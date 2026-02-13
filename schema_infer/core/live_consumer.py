"""
Persistent Kafka consumer for live schema inference.

Unlike OptimisticProcessor which creates ephemeral consumers with random UUIDs
and seeks to watermark offsets, this consumer uses a stable consumer group for
offset tracking, enabling resume-on-restart and multi-instance scaling.

Multi-instance support:
  Multiple instances can share the same consumer group. Kafka distributes
  partitions across instances. On rebalance, the on_revoke/on_assign
  callbacks notify the orchestrator so it can persist/load state for the
  affected topics.
"""

import os
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

os.environ.setdefault("KAFKA_LOG_LEVEL", "7")
os.environ.setdefault("RDKAFKA_LOG_LEVEL", "7")

from confluent_kafka import Consumer, KafkaError as ConfluentKafkaError, KafkaException

from ..config import Config
from ..utils.exceptions import LiveModeError
from ..utils.logger import get_logger


class LiveConsumer:
    """
    Persistent Kafka consumer for live schema inference.

    Uses a stable consumer group ID for offset tracking via Kafka's
    consumer group protocol. Offsets are committed manually after each batch
    is successfully processed, enabling resume-on-restart.

    Supports multi-instance scaling: multiple processes can share the same
    consumer group. Kafka distributes partitions across them. Rebalance
    callbacks notify the orchestrator to persist/load state.
    """

    def __init__(self, config: Config, group_id: str):
        self.config = config
        self.group_id = group_id
        self.logger = get_logger(__name__)
        self.consumer: Optional[Consumer] = None
        self._assigned_partitions: List[Any] = []
        self._assigned_topics: Set[str] = set()

        # Rebalance callbacks for the orchestrator
        self._on_topics_assigned: Optional[Callable[[Set[str]], None]] = None
        self._on_topics_revoked: Optional[Callable[[Set[str]], None]] = None

        self._initialize_consumer()

    def _initialize_consumer(self) -> None:
        """Create the confluent_kafka Consumer with stable group ID."""
        try:
            initial_offset = "latest"
            if hasattr(self.config, "live"):
                initial_offset = self.config.live.initial_offset

            consumer_config = {
                "bootstrap.servers": self.config.kafka.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": initial_offset,
                "enable.auto.commit": False,
                "session.timeout.ms": self.config.kafka.session_timeout_ms,
                "heartbeat.interval.ms": self.config.kafka.heartbeat_interval_ms,
                # Performance tuning for high topic counts
                "fetch.max.bytes": 52428800,  # 50MB
                "max.partition.fetch.bytes": 1048576,  # 1MB per partition
                "queued.min.messages": 1000,
                "queued.max.messages.kbytes": 65536,  # 64MB
                "fetch.wait.max.ms": 100,
                # Suppress librdkafka noise
                "log_level": "7",
                "log.connection.close": "false",
                "log.thread.name": "false",
                "broker.address.family": "v4",
                "enable.metrics.push": False,
                "log.queue": "false",
                "statistics.interval.ms": "0",
                "enable.partition.eof": "false",
            }

            # Add authentication
            from ..plugin.auth import AuthenticationManager

            auth_manager = AuthenticationManager(self.config)
            auth_config = auth_manager.configure_kafka_auth()
            consumer_config.update(auth_config)

            self.consumer = Consumer(consumer_config)
            self.logger.info(
                f"Initialized live consumer with group '{self.group_id}' "
                f"on {self.config.kafka.bootstrap_servers}"
            )
        except Exception as e:
            raise LiveModeError(f"Failed to initialize live consumer: {e}")

    def set_rebalance_callbacks(
        self,
        on_assigned: Optional[Callable[[Set[str]], None]] = None,
        on_revoked: Optional[Callable[[Set[str]], None]] = None,
    ) -> None:
        """
        Set callbacks for partition rebalance events.

        The orchestrator uses these to persist state for revoked topics
        and load state for newly assigned topics.

        Args:
            on_assigned: Called with set of topic names newly assigned.
            on_revoked: Called with set of topic names being revoked.
        """
        self._on_topics_assigned = on_assigned
        self._on_topics_revoked = on_revoked

    def subscribe(self, topics: List[str]) -> None:
        """
        Subscribe to topics with rebalance callbacks.

        Args:
            topics: List of topic names to subscribe to.
        """
        if not self.consumer:
            raise LiveModeError("Consumer not initialized")

        def on_assign(consumer, partitions):
            self._assigned_partitions = partitions
            new_topics = {p.topic for p in partitions}
            previously_assigned = self._assigned_topics.copy()
            self._assigned_topics = new_topics

            added = new_topics - previously_assigned
            if added:
                self.logger.info(
                    f"Topics assigned: {', '.join(sorted(added))} "
                    f"({len(partitions)} partitions total)"
                )
                if self._on_topics_assigned:
                    self._on_topics_assigned(added)

        def on_revoke(consumer, partitions):
            revoked_topics = {p.topic for p in partitions}
            self.logger.info(
                f"Topics revoked: {', '.join(sorted(revoked_topics))} "
                f"({len(partitions)} partitions)"
            )
            # Commit offsets for revoked partitions before they're taken away
            try:
                consumer.commit(asynchronous=False)
            except Exception as e:
                self.logger.warning(f"Failed to commit on revoke: {e}")

            # Notify orchestrator to persist state for revoked topics
            if self._on_topics_revoked:
                self._on_topics_revoked(revoked_topics)

            # Update assigned set
            self._assigned_topics -= revoked_topics

        self.consumer.subscribe(
            topics, on_assign=on_assign, on_revoke=on_revoke
        )
        self.logger.info(f"Subscribed to {len(topics)} topics")

    @property
    def assigned_topics(self) -> Set[str]:
        """Topics currently assigned to this consumer instance."""
        return self._assigned_topics.copy()

    def poll_batch(
        self, batch_size: int, batch_timeout_seconds: float
    ) -> Dict[str, List[Tuple[Optional[bytes], bytes]]]:
        """
        Poll messages up to batch_size or until batch_timeout_seconds elapses.

        Uses consume() batch API for better throughput at scale.

        Args:
            batch_size: Maximum number of messages to collect.
            batch_timeout_seconds: Maximum seconds to wait for messages.

        Returns:
            Dict mapping topic name to list of (key, value) tuples.
        """
        if not self.consumer:
            raise LiveModeError("Consumer not initialized")

        topic_messages: Dict[str, List[Tuple[Optional[bytes], bytes]]] = defaultdict(
            list
        )
        total_count = 0
        start_time = time.time()

        while total_count < batch_size:
            remaining = batch_timeout_seconds - (time.time() - start_time)
            if remaining <= 0:
                break

            # Use consume() batch API -- fetch up to remaining messages at once
            chunk_size = min(500, batch_size - total_count)
            poll_timeout = min(1.0, remaining)

            messages = self.consumer.consume(
                num_messages=chunk_size, timeout=poll_timeout
            )

            if not messages:
                continue

            for msg in messages:
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == ConfluentKafkaError._PARTITION_EOF:
                        continue
                    self.logger.warning(f"Consumer error: {msg.error()}")
                    continue

                value = msg.value()
                if value is not None:
                    topic_messages[msg.topic()].append((msg.key(), value))
                    total_count += 1

        return dict(topic_messages)

    def commit(self) -> None:
        """Synchronous offset commit."""
        if not self.consumer:
            raise LiveModeError("Consumer not initialized")
        try:
            self.consumer.commit(asynchronous=False)
            self.logger.debug("Offsets committed")
        except KafkaException as e:
            # No offsets to commit is not an error
            if "No offset stored" not in str(e):
                self.logger.warning(f"Failed to commit offsets: {e}")

    def close(self) -> None:
        """Close the consumer gracefully."""
        if self.consumer:
            try:
                self.consumer.close()
                self.logger.info("Live consumer closed")
            except Exception as e:
                self.logger.warning(f"Error closing live consumer: {e}")
            finally:
                self.consumer = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
