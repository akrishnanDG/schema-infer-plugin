"""
Live mode orchestrator for continuous schema inference.

Ties together the live consumer, format detection/parsing, incremental schema
state, schema generators, registry client, and state persistence.

Multi-instance scaling:
  Multiple instances of `schema-infer live` can share the same --consumer-group.
  Kafka distributes partitions across instances. On rebalance, state for
  revoked topics is persisted to disk and state for newly assigned topics is
  loaded from disk. This enables horizontal scaling to 1000s of topics.
"""

import signal
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import click

from ..config import Config
from ..core.incremental import IncrementalSchemaState, SchemaChangeReport
from ..core.live_consumer import LiveConsumer
from ..core.state_store import StateStore
from ..core.registry import SchemaRegistry
from ..formats.detector import FormatDetector
from ..formats.parsers import ParserFactory
from ..schemas.generators import SchemaGeneratorFactory
from ..utils.exceptions import LiveModeError, SchemaRegistryError
from ..utils.logger import get_logger

_SENTINEL = object()  # Distinguishes "not yet checked" from "checked, found None"


class LiveModeOrchestrator:
    """
    Orchestrates the live schema inference loop.

    Flow per batch cycle:
    1. Poll messages from Kafka via LiveConsumer
    2. Parse messages (detect format on first batch per topic, cache it)
    3. Merge parsed records into IncrementalSchemaState
    4. Detect schema changes
    5. If changes: generate schema, check compatibility, register, write file
    6. Commit offsets
    7. Persist dirty state

    Multi-instance scaling:
    - Rebalance callbacks persist state for revoked topics and load for assigned
    - Persistent thread pool avoids per-cycle overhead
    - batch_size auto-scales with topic count
    - Workers auto-scale with topic count
    """

    def __init__(
        self,
        config: Config,
        topics: List[str],
        schema_format: str,
        register: bool,
        output_dir: Optional[Path],
        state_dir: Optional[Path],
        batch_size: int,
        batch_timeout: float,
        consumer_group: str,
        context: Optional[str],
        on_incompatible: str,
        data_format: str,
    ):
        self.config = config
        self.topics = topics
        self.schema_format = schema_format
        self.register = register
        self.output_dir = output_dir
        self.batch_timeout = batch_timeout
        self.consumer_group = consumer_group
        self.on_incompatible = on_incompatible
        self.data_format = data_format
        self.logger = get_logger(__name__)

        # Auto-scale batch_size and workers based on topic count
        topic_count = len(topics)
        self.batch_size = self._auto_scale_batch_size(batch_size, topic_count)
        self._max_workers = self._auto_scale_workers(
            config.performance.max_workers, topic_count
        )

        # Schema Registry context
        if context is not None:
            config.schema_registry.context = context

        # State management
        self._states: Dict[str, IncrementalSchemaState] = {}
        self._states_lock = threading.Lock()
        self._topic_formats: Dict[str, str] = {}  # Cached detected format per topic
        self._topic_discriminators: Dict[str, Optional[str]] = {}  # Cached discriminator per topic
        self._disc_record_buffer: Dict[str, List[Dict[str, Any]]] = {}  # Buffered records for disc detection
        self._topic_flat_registered: Set[str] = set()  # Topics with flat schemas already in SR
        self._topic_event_types: Dict[str, Set[str]] = {}  # Known event types per topic
        self._topic_last_activity: Dict[str, float] = {}
        # Lock for topic metadata dicts (fast dict ops only — never hold during I/O).
        # Lock ordering: always acquire _states_lock before _metadata_lock if both needed.
        self._metadata_lock = threading.Lock()
        self._shutdown = False

        # Statistics (use lock for thread-safe updates)
        self._stats_lock = threading.Lock()
        self._total_messages = 0
        self._total_registrations = 0
        self._total_changes = 0
        self._start_time = 0.0
        self._last_summary_time = 0.0

        # Components
        self.format_detector = FormatDetector(
            confidence_threshold=config.inference.confidence_threshold,
            sample_size=config.inference.sample_size,
        )

        # State store
        effective_state_dir = state_dir or Path(
            config.live.state_dir or "~/.schema-infer/state"
        )
        self.state_store = StateStore(effective_state_dir) if config.live.persist_state else None

        # Registry
        self.registry = SchemaRegistry(config) if register else None

        # Registration semaphore for rate limiting
        self._registration_semaphore = threading.Semaphore(
            config.live.max_concurrent_registrations
        )

        # Persistent thread pool (created once, reused across batch cycles)
        self._executor: Optional[ThreadPoolExecutor] = None

    @staticmethod
    def _auto_scale_batch_size(user_batch_size: int, topic_count: int) -> int:
        """
        Auto-scale batch_size based on topic count.

        With many topics, a small batch_size means most topics get 0 messages
        per cycle. Scale up so each active topic gets a reasonable sample.
        """
        if topic_count <= 10:
            return user_batch_size  # User's value is fine for small sets
        # Target ~10 messages per topic per cycle, minimum user's value
        scaled = max(user_batch_size, topic_count * 10)
        # Cap at 50000 to avoid excessive memory usage
        return min(scaled, 50000)

    @staticmethod
    def _auto_scale_workers(user_max_workers: int, topic_count: int) -> int:
        """
        Auto-scale thread pool workers based on topic count.

        With 1000 topics, 4 workers is too few. Scale up, but cap
        at a reasonable limit to avoid thrashing.
        """
        if topic_count <= 10:
            return user_max_workers
        elif topic_count <= 100:
            return max(user_max_workers, 8)
        elif topic_count <= 500:
            return max(user_max_workers, 16)
        else:
            return max(user_max_workers, 32)

    def run(self) -> None:
        """Main loop. Runs until KeyboardInterrupt or shutdown signal."""

        # Register signal handlers
        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)

        def _handle_shutdown(signum, frame):
            self._shutdown = True

        signal.signal(signal.SIGINT, _handle_shutdown)
        signal.signal(signal.SIGTERM, _handle_shutdown)

        self._start_time = time.time()
        self._last_summary_time = self._start_time

        # Print startup banner
        self._print_startup()

        # Create persistent thread pool
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)

        try:
            with LiveConsumer(self.config, self.consumer_group) as consumer:
                # Wire up rebalance callbacks for multi-instance support
                consumer.set_rebalance_callbacks(
                    on_assigned=self._on_topics_assigned,
                    on_revoked=self._on_topics_revoked,
                )
                consumer.subscribe(self.topics)

                while not self._shutdown:
                    # Poll a batch
                    topic_messages = consumer.poll_batch(
                        self.batch_size, self.batch_timeout
                    )

                    if topic_messages:
                        self._process_batch(topic_messages)
                        consumer.commit()

                    # Periodic summary for large topic sets
                    now = time.time()
                    if (
                        now - self._last_summary_time
                        >= self.config.live.summary_interval_seconds
                        and len(self.topics) > 10
                    ):
                        self._print_periodic_summary()
                        self._last_summary_time = now

                    # Evict idle states
                    self._evict_idle_states()

        finally:
            # Restore original signal handlers
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)

            # Shutdown thread pool
            if self._executor:
                self._executor.shutdown(wait=True)
                self._executor = None

            # Persist all dirty states on shutdown
            self._persist_all_dirty_states()
            self._print_shutdown_summary()

    def _on_topics_assigned(self, topics: Set[str]) -> None:
        """
        Called when Kafka assigns new topic partitions to this instance.

        Loads persisted state from disk for the newly assigned topics.
        This enables multi-instance scaling: when a topic moves from
        instance A to instance B, B picks up A's persisted state.
        """
        if not self.state_store:
            return

        loaded = 0
        for topic_name in topics:
            if topic_name not in self._states:
                state = self.state_store.load(topic_name, self.config)
                if state:
                    with self._states_lock:
                        self._states[topic_name] = state
                    loaded += 1

        if loaded > 0:
            click.echo(
                f"[{_ts()}] Rebalance: loaded state for {loaded} "
                f"newly assigned topics"
            )

    def _on_topics_revoked(self, topics: Set[str]) -> None:
        """
        Called when Kafka revokes topic partitions from this instance.

        Persists dirty state to disk so the new owner can pick it up.
        Then removes the state from memory.
        """
        if not self.state_store:
            return

        persisted = 0
        with self._states_lock:
            for topic_name in topics:
                state = self._states.get(topic_name)
                if state and state.dirty:
                    try:
                        self.state_store.save(state)
                        state.dirty = False
                        persisted += 1
                    except Exception as e:
                        self.logger.warning(
                            f"Failed to persist state for {topic_name} on revoke: {e}"
                        )
                # Remove from memory -- new owner will load from disk
                self._states.pop(topic_name, None)

        # Clean all metadata dicts for revoked topics
        with self._metadata_lock:
            for topic_name in topics:
                self._topic_formats.pop(topic_name, None)
                self._topic_discriminators.pop(topic_name, None)
                self._disc_record_buffer.pop(topic_name, None)
                self._topic_flat_registered.discard(topic_name)
                self._topic_event_types.pop(topic_name, None)
                self._topic_last_activity.pop(topic_name, None)

        if persisted > 0:
            click.echo(
                f"[{_ts()}] Rebalance: persisted state for {persisted} "
                f"revoked topics"
            )

    def _process_batch(
        self, topic_messages: Dict[str, List[Tuple[Optional[bytes], bytes]]]
    ) -> None:
        """Process a batch of messages, potentially from multiple topics."""
        num_topics = len(topic_messages)
        max_workers = min(self._max_workers, num_topics)

        if max_workers <= 1 or not self._executor:
            # Single-threaded for small batches
            for topic_name, messages in topic_messages.items():
                self._process_topic_batch(topic_name, messages)
        else:
            # Parallel processing using persistent thread pool
            futures = {
                self._executor.submit(
                    self._process_topic_batch, topic_name, messages
                ): topic_name
                for topic_name, messages in topic_messages.items()
            }
            for future in as_completed(futures):
                topic_name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    click.echo(
                        f"  [{_ts()}] {topic_name}: Error processing batch: {e}",
                        err=True,
                    )

    def _process_topic_batch(
        self,
        topic_name: str,
        messages: List[Tuple[Optional[bytes], bytes]],
    ) -> None:
        """Process a batch of messages for a single topic.

        Handles both flat and multi-event topics. On the first batch,
        auto-detects if the topic has a discriminator field. If so,
        splits records by event type and maintains per-type states.
        """
        with self._stats_lock:
            self._total_messages += len(messages)
        with self._metadata_lock:
            self._topic_last_activity[topic_name] = time.time()

        # Parse messages
        parsed_records = self._parse_messages(topic_name, messages)
        if not parsed_records:
            return

        # Detect discriminator on every batch until one is found (JSON Schema only)
        discriminator = None
        if self.schema_format == "json-schema":
            with self._metadata_lock:
                cached_disc = self._topic_discriminators.get(topic_name, _SENTINEL)
                # Buffer records for detection until a discriminator is found
                if cached_disc is _SENTINEL or cached_disc is None:
                    buf = self._disc_record_buffer.get(topic_name, [])
                    buf.extend(parsed_records)
                    if len(buf) > 200:
                        buf = buf[-200:]
                    self._disc_record_buffer[topic_name] = buf

            if cached_disc is _SENTINEL or cached_disc is None:
                from ..schemas.inference import SchemaInferrer as SchemaAnalyzer
                analyzer = SchemaAnalyzer(
                    confidence_threshold=self.config.inference.confidence_threshold,
                    max_depth=self.config.inference.max_depth,
                )
                with self._metadata_lock:
                    check_records = list(self._disc_record_buffer.get(topic_name, parsed_records))
                disc = analyzer.detect_discriminator(check_records)
                with self._metadata_lock:
                    self._topic_discriminators[topic_name] = disc
                    if disc:
                        self._topic_event_types[topic_name] = set()
                        self._disc_record_buffer.pop(topic_name, None)
                if disc:
                    click.echo(f"[{_ts()}] {topic_name}: Detected discriminator field '{disc}'")

            with self._metadata_lock:
                discriminator = self._topic_discriminators.get(topic_name)

        if discriminator:
            self._process_multi_event_batch(topic_name, parsed_records, discriminator)
        else:
            self._process_flat_batch(topic_name, parsed_records)

    def _process_flat_batch(
        self,
        topic_name: str,
        parsed_records: List[Dict[str, Any]],
    ) -> None:
        """Process a batch as a flat (single event type) topic."""
        state = self._get_or_create_state(topic_name)
        new_schema = state.merge_batch(parsed_records)
        report = state.detect_changes(new_schema)

        if report is not None and report.has_changes:
            with self._stats_lock:
                self._total_changes += 1
            is_initial = state.total_records_processed == len(parsed_records)

            if is_initial:
                click.echo(
                    f"[{_ts()}] {topic_name}: Processed {len(parsed_records)} messages "
                    f"(total: {state.total_records_processed}). "
                    f"{len(new_schema.fields)} fields detected."
                )
            else:
                click.echo(
                    f"[{_ts()}] {topic_name}: Processed {len(parsed_records)} messages "
                    f"(total: {state.total_records_processed}). Schema change detected:"
                )
                click.echo(report.summary())

            if (
                self.register
                and self.registry
                and state.total_records_processed
                >= self.config.live.min_records_before_register
            ):
                self._handle_schema_registration(
                    topic_name, state, new_schema, report, is_initial
                )
            elif self.register and self.registry:
                click.echo(
                    f"[{_ts()}] {topic_name}: Waiting for "
                    f"{self.config.live.min_records_before_register} records "
                    f"before first registration "
                    f"({state.total_records_processed} so far)"
                )

            if self.output_dir:
                self._write_schema_file(topic_name, new_schema)
        else:
            if len(self.topics) <= 10:
                click.echo(
                    f"[{_ts()}] {topic_name}: Processed {len(parsed_records)} messages "
                    f"(total: {state.total_records_processed}). No schema changes."
                )

    def _process_multi_event_batch(
        self,
        topic_name: str,
        parsed_records: List[Dict[str, Any]],
        discriminator: str,
    ) -> None:
        """Process a batch for a multi-event topic.

        Splits records by discriminator value, maintains per-type states,
        and registers sub-schemas + main oneOf schema.
        """
        # Group records by event type
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for record in parsed_records:
            event_type = str(record.get(discriminator, "_unknown"))
            if event_type not in groups:
                groups[event_type] = []
            groups[event_type].append(record)

        any_changes = False
        new_event_type_discovered = False
        with self._metadata_lock:
            known_types = set(self._topic_event_types.get(topic_name, set()))

        for event_type, records in groups.items():
            # State key: topic:event_type
            state_key = f"{topic_name}__evt__{event_type}"
            state = self._get_or_create_state(state_key)

            new_schema = state.merge_batch(records)
            report = state.detect_changes(new_schema)

            if event_type not in known_types:
                known_types.add(event_type)
                new_event_type_discovered = True
                click.echo(
                    f"[{_ts()}] {topic_name}/{event_type}: New event type discovered "
                    f"({len(records)} records, {len(new_schema.fields)} fields)"
                )

            if report is not None and report.has_changes:
                any_changes = True
                click.echo(
                    f"[{_ts()}] {topic_name}/{event_type}: Schema change detected "
                    f"({len(records)} records, total: {state.total_records_processed})"
                )
                click.echo(report.summary())

                if self.output_dir:
                    self._write_schema_file(f"{topic_name}.{event_type}", new_schema)

        with self._metadata_lock:
            self._topic_event_types[topic_name] = known_types

        # Register if changes detected or new event type appeared
        if (any_changes or new_event_type_discovered) and self.register and self.registry:
            self._handle_multi_event_registration(topic_name, discriminator)

    def _handle_multi_event_registration(
        self,
        topic_name: str,
        discriminator: str,
    ) -> None:
        """Generate and register multi-event schemas (sub-schemas + main oneOf)."""
        from ..core.inferrer import SchemaInferrer
        from ..schemas.generators import JSONSchemaGenerator
        from ..core.merger import SchemaMerger

        inferrer = SchemaInferrer(self.config)
        json_gen = JSONSchemaGenerator()
        merger = SchemaMerger()

        with self._metadata_lock:
            event_types = sorted(self._topic_event_types.get(topic_name, set()))
        if len(event_types) < 2:
            return

        # Build sub-schemas from per-type states
        event_schema_objs = {}
        for event_type in event_types:
            state_key = f"{topic_name}__evt__{event_type}"
            with self._states_lock:
                state = self._states.get(state_key)
            if state and state.last_schema:
                event_schema_objs[event_type] = state.last_schema

        if not event_schema_objs:
            return

        # Generate schema files
        schema_files = json_gen.generate_multi_event(
            topic_name, event_schema_objs, discriminator
        )

        sub_contents = {
            et: schema_files[f"{topic_name}.{et}"]
            for et in event_types
            if f"{topic_name}.{et}" in schema_files
        }
        main_content = schema_files.get(topic_name, "")

        # Merge with existing SR schemas
        try:
            main_subject = self.registry._generate_subject_name(
                topic_name, self.schema_format
            )
            existing_main = self.registry.get_latest_schema(main_subject)
            if existing_main and "schema" in existing_main:
                import json
                existing_et = set()
                try:
                    em = json.loads(existing_main["schema"])
                    for ref in em.get("oneOf", []):
                        rn = ref.get("$ref", "")
                        if rn.startswith(f"{topic_name}-"):
                            existing_et.add(rn[len(f"{topic_name}-"):])
                except Exception:
                    pass

                existing_sub = merger.fetch_existing_sub_schemas(
                    self.registry, topic_name,
                    list(set(event_types) | existing_et)
                )
                merged = merger.merge_multi_event_schemas(
                    existing_main["schema"], sub_contents,
                    main_content, topic_name, existing_sub
                )
                main_content = merged[topic_name]
                sub_contents = {
                    et: merged[f"{topic_name}.{et}"]
                    for et in sorted(set(sub_contents.keys()) | set(existing_sub.keys()))
                    if f"{topic_name}.{et}" in merged
                }
        except Exception:
            pass

        # If transitioning from flat to multi-event, temporarily set compatibility to NONE
        main_subject = self.registry._generate_subject_name(
            topic_name, self.schema_format
        )
        previous_compat = None
        with self._metadata_lock:
            was_flat_registered = topic_name in self._topic_flat_registered

        if was_flat_registered:
            try:
                config_resp = self.registry.get_config(subject=main_subject)
                previous_compat = config_resp.get("compatibilityLevel", self.config.schema_registry.compatibility)
            except Exception:
                previous_compat = self.config.schema_registry.compatibility
            try:
                self.registry.set_config({"compatibility": "NONE"}, subject=main_subject)
                click.echo(f"[{_ts()}] {topic_name}: Transitioning from flat to multi-event, temporarily set compatibility to NONE")
            except Exception as e:
                click.echo(f"[{_ts()}] {topic_name}: Failed to set compatibility for transition: {e}", err=True)

        # Register (with semaphore for rate limiting)
        with self._registration_semaphore:
            try:
                reg_result = self.registry.register_multi_event_schemas(
                    topic_name, sub_contents, main_content, self.schema_format,
                    skip_compatibility_set=was_flat_registered,
                )
                with self._stats_lock:
                    self._total_registrations += len(reg_result)
                with self._metadata_lock:
                    self._topic_flat_registered.discard(topic_name)
                click.echo(
                    f"[{_ts()}] {topic_name}: Registered {len(reg_result)} multi-event schemas "
                    f"({len(sub_contents)} sub + 1 main)"
                )
            except Exception as e:
                click.echo(
                    f"[{_ts()}] {topic_name}: Multi-event registration failed: {e}",
                    err=True,
                )
            finally:
                # Always restore compatibility after transition attempt
                if previous_compat is not None:
                    try:
                        self.registry.set_config({"compatibility": previous_compat}, subject=main_subject)
                        click.echo(f"[{_ts()}] {topic_name}: Restored compatibility to {previous_compat}")
                    except Exception:
                        self.logger.warning(
                            f"{topic_name}: Failed to restore compatibility to {previous_compat}"
                        )

        # Write main schema file
        if self.output_dir:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            main_file = self.output_dir / f"{topic_name}.json"
            main_file.write_text(main_content)

    def _parse_messages(
        self,
        topic_name: str,
        messages: List[Tuple[Optional[bytes], bytes]],
    ) -> List[Dict[str, Any]]:
        """Parse raw messages into dicts using format detection."""
        message_values = [value for _, value in messages if value is not None]
        if not message_values:
            return []

        # Detect format on first batch, cache for subsequent batches
        with self._metadata_lock:
            cached_format = self._topic_formats.get(topic_name)

        if cached_format is None:
            if self.data_format != "auto":
                detected_format = self.data_format
            else:
                detected_format, confidence = self.format_detector.detect_format(
                    message_values
                )
                self.logger.debug(
                    f"{topic_name}: Detected format {detected_format} "
                    f"(confidence: {confidence:.2f})"
                )

            with self._metadata_lock:
                # Double-check: another thread or rebalance may have acted
                if topic_name not in self._topic_formats:
                    self._topic_formats[topic_name] = detected_format
                else:
                    detected_format = self._topic_formats[topic_name]

            # Also cache on the state
            with self._states_lock:
                state = self._states.get(topic_name)
            if state:
                state.detected_format = detected_format
        else:
            detected_format = cached_format

        # Create parser and parse
        try:
            parser = self._create_parser(detected_format, message_values)
            parsed_data = parser.parse_batch(message_values)

            if not parsed_data and detected_format != "raw-text":
                # Fallback to raw-text
                parser = ParserFactory.create_parser("raw-text")
                parsed_data = parser.parse_batch(message_values)
                if parsed_data:
                    with self._metadata_lock:
                        self._topic_formats[topic_name] = "raw-text"

            return parsed_data or []
        except Exception as e:
            self.logger.warning(f"{topic_name}: Failed to parse messages: {e}")
            return []

    def _create_parser(self, format_name: str, messages: List[bytes]) -> Any:
        """Create parser for the detected format. Mirrors SchemaInferrer._create_parser."""
        if format_name == "csv":
            delimiter = self.format_detector.detect_delimiter(
                [msg.decode("utf-8", errors="ignore") for msg in messages[:10]]
            )
            return ParserFactory.create_parser("csv", delimiter=delimiter or ",")
        elif format_name == "tsv":
            return ParserFactory.create_parser("tsv")
        elif format_name == "key-value":
            sample_texts = [
                msg.decode("utf-8", errors="ignore") for msg in messages[:5]
            ]
            separator = "=" if any("=" in t for t in sample_texts) else ":"
            return ParserFactory.create_parser(
                "key-value", key_value_separator=separator
            )
        elif format_name == "raw-text":
            return ParserFactory.create_parser("raw-text")
        else:
            return ParserFactory.create_parser("json")

    def _handle_schema_registration(
        self,
        topic_name: str,
        state: IncrementalSchemaState,
        new_schema: Any,
        report: SchemaChangeReport,
        is_initial: bool,
    ) -> None:
        """Generate schema, check compatibility, and register."""
        from ..core.inferrer import SchemaInferrer

        inferrer = SchemaInferrer(self.config)

        # Convert InferredSchema to dict then generate
        schema_dict = new_schema.to_dict()
        schema_dict["_metadata"] = {
            "format": state.detected_format or "json",
            "message_count": state.total_records_processed,
            "parsed_count": state.total_records_processed,
            "confidence": 1.0,
        }

        try:
            schema_content = inferrer.generate_schema(schema_dict, self.schema_format)
        except Exception as e:
            click.echo(
                f"[{_ts()}] {topic_name}: Failed to generate schema: {e}", err=True
            )
            return

        # Merge with existing SR schema to preserve additionalProperties setting
        if self.schema_format == "json-schema":
            try:
                from ..core.merger import SchemaMerger
                subject = self.registry._generate_subject_name(topic_name, self.schema_format)
                existing = self.registry.get_latest_schema(subject)
                if existing and "schema" in existing:
                    merger = SchemaMerger()
                    schema_content = merger.merge_flat_schemas(
                        existing["schema"], schema_content
                    )
            except Exception:
                pass

        # Validate schema
        from ..utils.validators import validate_generated_schema

        is_valid, validation_error = validate_generated_schema(
            schema_content, self.schema_format
        )
        if not is_valid:
            click.echo(
                f"[{_ts()}] {topic_name}: Generated schema is invalid: {validation_error}",
                err=True,
            )
            return

        # Check compatibility if subject already exists
        subject = self.registry._generate_subject_name(topic_name, self.schema_format)
        subject_exists = False
        if not is_initial:
            try:
                self.registry.get_latest_schema(subject)
                subject_exists = True
            except SchemaRegistryError:
                pass

        if subject_exists:
            try:
                is_compatible = self.registry.check_compatibility(
                    subject, schema_content, schema_format=self.schema_format
                )
            except SchemaRegistryError:
                # If compatibility check fails (e.g., no existing schema), proceed
                is_compatible = True

            if not is_compatible:
                compatibility = self.config.schema_registry.compatibility
                click.echo(
                    f"[{_ts()}] {topic_name}: Compatibility check FAILED "
                    f"(level: {compatibility})"
                )

                if self.on_incompatible in ("skip", "log"):
                    click.echo(
                        f"[{_ts()}] {topic_name}: Skipping registration "
                        f"(use --on-incompatible=force to override)"
                    )
                    if self.on_incompatible == "log" and self.output_dir:
                        self._write_incompatible_schema(
                            topic_name, schema_content
                        )
                    return
                elif self.on_incompatible == "force":
                    click.echo(
                        f"[{_ts()}] {topic_name}: Forcing registration "
                        f"(temporarily setting compatibility to NONE)"
                    )
                    original_compat = self.config.schema_registry.compatibility
                    try:
                        self.registry._set_subject_compatibility(subject, "NONE")
                        schema_id = self.registry.register_schema(
                            topic_name, schema_content, self.schema_format,
                            skip_compatibility_set=True,
                        )
                        click.echo(
                            f"[{_ts()}] {topic_name}: Schema force-registered (ID: {schema_id})"
                        )
                        with self._stats_lock:
                            self._total_registrations += 1
                    except Exception as e:
                        click.echo(
                            f"[{_ts()}] {topic_name}: Force registration failed: {e}",
                            err=True,
                        )
                    finally:
                        try:
                            self.registry._set_subject_compatibility(subject, original_compat)
                        except Exception:
                            self.logger.warning(
                                f"{topic_name}: Failed to restore subject compatibility to {original_compat}"
                            )
                    return
                elif self.on_incompatible == "fail":
                    click.echo(
                        f"[{_ts()}] {topic_name}: Incompatible schema -- exiting"
                    )
                    self._shutdown = True
                    return

        # Register
        with self._registration_semaphore:
            try:
                schema_id = self.registry.register_schema(
                    topic_name, schema_content, self.schema_format
                )
                with self._metadata_lock:
                    self._topic_flat_registered.add(topic_name)
                label = "Initial schema registered" if is_initial else "Schema updated"
                click.echo(f"[{_ts()}] {topic_name}: {label} (ID: {schema_id})")
                with self._stats_lock:
                    self._total_registrations += 1
            except Exception as e:
                click.echo(
                    f"[{_ts()}] {topic_name}: Registration failed: {e}", err=True
                )

    def _write_schema_file(self, topic_name: str, schema: Any) -> None:
        """Write schema to output directory."""
        from ..core.inferrer import SchemaInferrer

        inferrer = SchemaInferrer(self.config)
        schema_dict = schema.to_dict()
        schema_dict["_metadata"] = {"format": "json", "message_count": 0, "parsed_count": 0, "confidence": 1.0}

        try:
            schema_content = inferrer.generate_schema(schema_dict, self.schema_format)
            extensions = {"avro": "avsc", "protobuf": "proto", "json-schema": "json"}
            ext = extensions.get(self.schema_format, self.schema_format)
            self.output_dir.mkdir(parents=True, exist_ok=True)
            schema_file = self.output_dir / f"{topic_name}.{ext}"
            schema_file.write_text(schema_content)
        except Exception as e:
            self.logger.warning(f"Failed to write schema file for {topic_name}: {e}")

    def _write_incompatible_schema(
        self, topic_name: str, schema_content: str
    ) -> None:
        """Write incompatible schema for manual review."""
        extensions = {"avro": "avsc", "protobuf": "proto", "json-schema": "json"}
        ext = extensions.get(self.schema_format, self.schema_format)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        schema_file = self.output_dir / f"{topic_name}.incompatible.{ext}"
        schema_file.write_text(schema_content)
        click.echo(f"[{_ts()}] {topic_name}: Incompatible schema written to {schema_file}")

    def _get_or_create_state(self, topic_name: str) -> IncrementalSchemaState:
        """Get existing state or create/load a new one.

        Priority: in-memory > disk state > Schema Registry seed > empty.
        """
        with self._states_lock:
            if topic_name in self._states:
                return self._states[topic_name]

        # Try to load from disk (outside lock to avoid blocking)
        loaded = None
        if self.state_store:
            loaded = self.state_store.load(topic_name, self.config)

        # If no disk state, try to seed from Schema Registry (JSON Schema only)
        seeded = None
        if not loaded and self.registry and self.schema_format == "json-schema":
            try:
                subject = self.registry._generate_subject_name(
                    topic_name, self.schema_format
                )
                existing = self.registry.get_latest_schema(subject)
                if existing and "schema" in existing:
                    seeded = IncrementalSchemaState.seed_from_json_schema(
                        topic_name, existing["schema"], self.config
                    )
            except Exception:
                pass

        with self._states_lock:
            # Double-check after acquiring lock
            if topic_name in self._states:
                return self._states[topic_name]

            if loaded:
                self._states[topic_name] = loaded
                click.echo(
                    f"[{_ts()}] {topic_name}: Resumed from persisted state "
                    f"({loaded.total_records_processed} records)"
                )
            elif seeded:
                self._states[topic_name] = seeded
                click.echo(
                    f"[{_ts()}] {topic_name}: Seeded from existing Schema Registry schema "
                    f"({len(seeded.field_analysis)} fields)"
                )
            else:
                self._states[topic_name] = IncrementalSchemaState(
                    topic_name, self.config
                )

            return self._states[topic_name]

    def _persist_all_dirty_states(self) -> None:
        """Persist all states that have been modified."""
        if not self.state_store:
            return

        dirty_count = 0
        with self._states_lock:
            states_snapshot = list(self._states.items())

        for topic_name, state in states_snapshot:
            if state.dirty:
                try:
                    self.state_store.save(state)
                    state.dirty = False
                    dirty_count += 1
                except Exception as e:
                    self.logger.warning(
                        f"Failed to persist state for {topic_name}: {e}"
                    )

        if dirty_count > 0:
            click.echo(f"  Persisting state for {dirty_count} topics... done")

    def _evict_idle_states(self) -> None:
        """Evict states for topics that have been idle too long."""
        if not self.state_store:
            return

        now = time.time()
        evict_threshold = self.config.live.idle_evict_seconds
        to_evict = []

        with self._metadata_lock:
            activity_snapshot = dict(self._topic_last_activity)
        for topic_name, last_active in activity_snapshot.items():
            if now - last_active > evict_threshold and topic_name in self._states:
                to_evict.append(topic_name)

        if not to_evict:
            return

        with self._states_lock:
            for topic_name in to_evict:
                state = self._states.get(topic_name)
                if state is None:
                    continue
                if state.dirty:
                    try:
                        self.state_store.save(state)
                        state.dirty = False
                    except Exception as e:
                        self.logger.warning(
                            f"Failed to persist state for {topic_name} before eviction: {e}"
                        )
                del self._states[topic_name]
                self.logger.debug(f"Evicted idle state for {topic_name}")

    def _print_startup(self) -> None:
        """Print startup banner."""
        click.echo(
            f"Live mode started (batch: {self.batch_size} msgs / "
            f"{self.batch_timeout}s, format: {self.schema_format})"
        )
        click.echo(f"  Consumer group: {self.consumer_group}")
        if len(self.topics) <= 10:
            click.echo(f"  Topics: {', '.join(self.topics)}")
        else:
            click.echo(f"  Topics: {len(self.topics)} topics")
        click.echo(f"  Workers: {self._max_workers}")
        if self.register:
            ctx = self.config.schema_registry.context
            ctx_str = f" under context '{ctx}'" if ctx else ""
            click.echo(f"  Registering schemas to Schema Registry{ctx_str}")
            click.echo(
                f"  Compatibility: {self.config.schema_registry.compatibility} "
                f"(on incompatible: {self.on_incompatible})"
            )
        if self.output_dir:
            click.echo(f"  Output: {self.output_dir}/")
        if self.state_store:
            click.echo(f"  State persistence: {self.state_store.state_dir}")
        click.echo(f"  Press Ctrl+C to stop\n")

    def _print_periodic_summary(self) -> None:
        """Print periodic summary for large topic sets."""
        elapsed = time.time() - self._start_time
        with self._metadata_lock:
            activity_values = list(self._topic_last_activity.values())
        active = sum(
            1
            for t in activity_values
            if time.time() - t < self.config.live.summary_interval_seconds
        )
        with self._states_lock:
            states_in_mem = len(self._states)
        click.echo(
            f"[{_ts()}] Summary: {self._total_messages} messages processed, "
            f"{self._total_changes} schema changes, "
            f"{self._total_registrations} registrations, "
            f"{active}/{len(self.topics)} topics active, "
            f"{states_in_mem} states in memory, "
            f"uptime {_format_duration(elapsed)}"
        )

    def _print_shutdown_summary(self) -> None:
        """Print summary on shutdown."""
        elapsed = time.time() - self._start_time
        click.echo(f"\nLive mode stopped.")
        with self._metadata_lock:
            topic_count_seen = len(self._topic_last_activity)
        click.echo(
            f"  Processed {self._total_messages} messages across "
            f"{topic_count_seen} topics "
            f"in {_format_duration(elapsed)}"
        )
        if self._total_registrations > 0:
            click.echo(f"  Registered {self._total_registrations} schema versions")
        if self._total_changes > 0:
            click.echo(f"  Detected {self._total_changes} schema changes")


def _ts() -> str:
    """Current timestamp for log lines."""
    return time.strftime("%H:%M:%S")


def _format_duration(seconds: float) -> str:
    """Format a duration in seconds to a human-readable string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"
