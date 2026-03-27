"""
Comprehensive unit tests for LiveModeOrchestrator.

Tests cover partition ownership, rebalance callbacks, message parsing,
discriminator detection, flat and multi-event batch processing,
flat-to-multi-event transitions, and idle state eviction.
"""

import time
import threading
import pytest
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, call

from schema_infer.config import Config
from schema_infer.plugin.live import LiveModeOrchestrator
from schema_infer.core.incremental import IncrementalSchemaState, SchemaChangeReport

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config():
    """Build a Config with all fields the orchestrator touches."""
    config = Config()
    config.kafka.bootstrap_servers = "localhost:9092"
    config.kafka.auto_offset_reset = "latest"
    config.kafka.session_timeout_ms = 30000
    config.kafka.heartbeat_interval_ms = 10000
    config.schema_registry.url = "http://localhost:8081"
    config.schema_registry.compatibility = "BACKWARD"
    config.inference.max_messages = 50
    config.inference.timeout = 30
    config.inference.confidence_threshold = 0.8
    config.inference.max_depth = 20
    config.performance.max_workers = 4
    config.performance.batch_size = 100
    config.live.persist_state = False  # No disk I/O in unit tests
    config.live.min_records_before_register = 1
    config.live.idle_evict_seconds = 3600
    config.live.max_concurrent_registrations = 5
    config.live.summary_interval_seconds = 60
    config.live.state_dir = None
    return config


def _make_orchestrator(
    config=None,
    topics=None,
    schema_format="json-schema",
    register=False,
    output_dir=None,
    state_dir=None,
    batch_size=100,
    batch_timeout=5.0,
    consumer_group="test-group",
    context=None,
    on_incompatible="skip",
    data_format="auto",
    persist_state=False,
):
    """Create an orchestrator with sensible test defaults.

    When ``register=False`` (the default) no SchemaRegistry is instantiated,
    so external calls to the registry are not made.
    """
    if config is None:
        config = _make_config()
    config.live.persist_state = persist_state
    if topics is None:
        topics = ["test-topic"]

    with patch("schema_infer.plugin.live.FormatDetector"):
        orch = LiveModeOrchestrator(
            config=config,
            topics=topics,
            schema_format=schema_format,
            register=register,
            output_dir=output_dir,
            state_dir=state_dir,
            batch_size=batch_size,
            batch_timeout=batch_timeout,
            consumer_group=consumer_group,
            context=context,
            on_incompatible=on_incompatible,
            data_format=data_format,
        )
    return orch


# ===========================================================================
# TestPartitionOwnership
# ===========================================================================


class TestPartitionOwnership:
    """Tests for the _owns_partition_zero helper."""

    def test_owns_partition_zero_when_assigned(self):
        """Partition 0 in the set -> returns True."""
        orch = _make_orchestrator(config=_make_config())
        orch._topic_partitions["test-topic"] = {0, 1, 2}
        assert orch._owns_partition_zero("test-topic") is True

    def test_does_not_own_partition_zero(self):
        """Only partitions 1,2,3 -> returns False."""
        orch = _make_orchestrator(config=_make_config())
        orch._topic_partitions["test-topic"] = {1, 2, 3}
        assert orch._owns_partition_zero("test-topic") is False

    def test_owns_partition_zero_unknown_topic(self):
        """Topic not present in the partition map -> returns False."""
        orch = _make_orchestrator(config=_make_config())
        # _topic_partitions is empty — "unknown-topic" never appeared.
        assert orch._owns_partition_zero("unknown-topic") is False


# ===========================================================================
# TestOnTopicsAssigned
# ===========================================================================


class TestOnTopicsAssigned:
    """Tests for the _on_topics_assigned rebalance callback."""

    def test_updates_partition_map(self):
        """_on_topics_assigned should update _topic_partitions."""
        orch = _make_orchestrator(config=_make_config())
        partition_map = {"topic-a": {0, 1}, "topic-b": {2, 3}}
        orch._on_topics_assigned({"topic-a", "topic-b"}, partition_map)

        assert orch._topic_partitions["topic-a"] == {0, 1}
        assert orch._topic_partitions["topic-b"] == {2, 3}

    def test_loads_persisted_state(self):
        """When a state_store is present, persisted state is loaded."""

        orch = _make_orchestrator(config=_make_config(), persist_state=True)

        mock_state = Mock(spec=IncrementalSchemaState)
        mock_state.dirty = False

        mock_store = Mock()
        mock_store.load.return_value = mock_state
        orch.state_store = mock_store

        orch._on_topics_assigned({"topic-a"}, {"topic-a": {0}})

        mock_store.load.assert_called_once()
        call_args = mock_store.load.call_args
        assert call_args[0][0] == "topic-a"
        assert orch._states["topic-a"] is mock_state


# ===========================================================================
# TestOnTopicsRevoked
# ===========================================================================


class TestOnTopicsRevoked:
    """Tests for the _on_topics_revoked rebalance callback."""

    def test_persists_dirty_state(self):
        """Dirty states should be persisted via state_store.save()."""

        orch = _make_orchestrator(config=_make_config(), persist_state=True)

        mock_state = Mock(spec=IncrementalSchemaState)
        mock_state.dirty = True
        orch._states["topic-a"] = mock_state

        mock_store = Mock()
        orch.state_store = mock_store

        orch._on_topics_revoked({"topic-a"}, {"topic-a": {0}})

        mock_store.save.assert_called_once_with(mock_state)
        # State should be removed from memory after revocation
        assert "topic-a" not in orch._states

    def test_cleans_all_metadata(self):
        """All metadata dicts should be cleaned for revoked topics."""

        orch = _make_orchestrator(config=_make_config(), persist_state=True)

        mock_store = Mock()
        orch.state_store = mock_store

        topic = "topic-cleanup"
        # Populate every metadata dict
        orch._topic_formats[topic] = "json"
        orch._topic_discriminators[topic] = "event_type"
        orch._disc_record_buffer[topic] = [{"a": 1}]
        orch._topic_flat_registered.add(topic)
        orch._topic_event_types[topic] = {"typeA"}
        orch._topic_last_activity[topic] = time.time()
        orch._topic_partitions[topic] = {0}

        # Put a non-dirty state so save() is not called
        mock_state = Mock(spec=IncrementalSchemaState)
        mock_state.dirty = False
        orch._states[topic] = mock_state

        orch._on_topics_revoked({topic}, {topic: {0}})

        assert topic not in orch._topic_formats
        assert topic not in orch._topic_discriminators
        assert topic not in orch._disc_record_buffer
        assert topic not in orch._topic_flat_registered
        assert topic not in orch._topic_event_types
        assert topic not in orch._topic_last_activity
        assert topic not in orch._topic_partitions
        assert topic not in orch._states


# ===========================================================================
# TestParseMessages
# ===========================================================================


class TestParseMessages:
    """Tests for _parse_messages."""

    @patch("schema_infer.plugin.live.ParserFactory")
    def test_caches_detected_format(self, mock_parser_factory):
        """Format detection should run once; the cached value is reused."""
        orch = _make_orchestrator(config=_make_config(), data_format="auto")

        # Set up a mock FormatDetector that returns "json"
        orch.format_detector = Mock()
        orch.format_detector.detect_format.return_value = ("json", 0.95)

        # Parser mock
        mock_parser = Mock()
        mock_parser.parse_batch.return_value = [{"key": "value"}]
        mock_parser_factory.create_parser.return_value = mock_parser

        messages = [(None, b'{"key":"value"}')]

        # First call — should trigger format detection
        result1 = orch._parse_messages("topic-x", messages)
        assert orch.format_detector.detect_format.call_count == 1

        # Second call — should use cached format, no new detection
        result2 = orch._parse_messages("topic-x", messages)
        assert orch.format_detector.detect_format.call_count == 1  # Still 1
        assert orch._topic_formats["topic-x"] == "json"

    @patch("schema_infer.plugin.live.ParserFactory")
    def test_fallback_to_raw_text(self, mock_parser_factory):
        """When the primary parser returns empty, fall back to raw-text."""
        orch = _make_orchestrator(config=_make_config(), data_format="auto")

        orch.format_detector = Mock()
        orch.format_detector.detect_format.return_value = ("csv", 0.6)

        primary_parser = Mock()
        primary_parser.parse_batch.return_value = []  # Empty — trigger fallback

        fallback_parser = Mock()
        fallback_parser.parse_batch.return_value = [{"raw_text": "hello"}]

        # First call returns primary, second call returns fallback
        mock_parser_factory.create_parser.side_effect = [fallback_parser]

        # We need the orchestrator's _create_parser to return the primary parser
        # and then ParserFactory to return the fallback.
        with patch.object(orch, "_create_parser", return_value=primary_parser):
            result = orch._parse_messages("topic-fb", [(None, b"hello")])

        assert result == [{"raw_text": "hello"}]
        assert orch._topic_formats["topic-fb"] == "raw-text"

    def test_empty_messages_returns_empty(self):
        """An empty message list should return an empty list."""
        orch = _make_orchestrator(config=_make_config())
        result = orch._parse_messages("topic-empty", [])
        assert result == []


# ===========================================================================
# TestDiscriminatorDetection
# ===========================================================================


class TestDiscriminatorDetection:
    """Tests for discriminator detection in _process_topic_batch."""

    def _make_orch(self, schema_format="json-schema"):
        orch = _make_orchestrator(
            config=_make_config(),
            schema_format=schema_format,
        )
        return orch

    @patch("schema_infer.plugin.live.ParserFactory")
    def test_detects_discriminator_on_first_batch(self, mock_parser_factory):
        """When records have multiple event types, discriminator is detected."""
        orch = self._make_orch()

        records = [
            {"event_type": "click", "x": 10, "y": 20},
            {"event_type": "click", "x": 15, "y": 25},
            {"event_type": "view", "page": "/home"},
            {"event_type": "view", "page": "/about"},
            {"event_type": "purchase", "item": "widget"},
            {"event_type": "purchase", "item": "gadget"},
        ]

        # Mock _parse_messages to return the records directly
        with patch.object(orch, "_parse_messages", return_value=records):
            # Patch SchemaInferrer at its source module (imported locally as SchemaAnalyzer)
            with patch(
                "schema_infer.schemas.inference.SchemaInferrer"
            ) as mock_analyzer_cls:
                mock_analyzer = Mock()
                mock_analyzer.detect_discriminator.return_value = "event_type"
                mock_analyzer_cls.return_value = mock_analyzer

                # Mock _process_multi_event_batch to avoid full processing
                with patch.object(orch, "_process_multi_event_batch"):
                    messages = [(None, b"dummy")] * len(records)
                    orch._process_topic_batch("multi-topic", messages)

        assert orch._topic_discriminators["multi-topic"] == "event_type"

    @patch("schema_infer.plugin.live.ParserFactory")
    def test_no_discriminator_for_uniform_data(self, mock_parser_factory):
        """All records same shape -> detect_discriminator returns None."""
        orch = self._make_orch()

        records = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Carol", "age": 35},
            {"name": "Dave", "age": 28},
            {"name": "Eve", "age": 22},
        ]

        with patch.object(orch, "_parse_messages", return_value=records):
            with patch(
                "schema_infer.schemas.inference.SchemaInferrer"
            ) as mock_analyzer_cls:
                mock_analyzer = Mock()
                mock_analyzer.detect_discriminator.return_value = None
                mock_analyzer_cls.return_value = mock_analyzer

                with patch.object(orch, "_process_flat_batch"):
                    messages = [(None, b"dummy")] * len(records)
                    orch._process_topic_batch("flat-topic", messages)

        assert orch._topic_discriminators["flat-topic"] is None

    @patch("schema_infer.plugin.live.ParserFactory")
    def test_buffers_records_across_batches(self, mock_parser_factory):
        """Records from successive batches accumulate in the buffer."""
        orch = self._make_orch()

        batch1 = [{"event_type": "a", "x": 1}] * 3
        batch2 = [{"event_type": "b", "y": 2}] * 4

        with patch(
            "schema_infer.schemas.inference.SchemaInferrer"
        ) as mock_analyzer_cls:
            mock_analyzer = Mock()
            mock_analyzer.detect_discriminator.return_value = None
            mock_analyzer_cls.return_value = mock_analyzer

            with patch.object(orch, "_process_flat_batch"):
                # First batch
                with patch.object(orch, "_parse_messages", return_value=batch1):
                    orch._process_topic_batch("buf-topic", [(None, b"d")] * 3)

                # Second batch
                with patch.object(orch, "_parse_messages", return_value=batch2):
                    orch._process_topic_batch("buf-topic", [(None, b"d")] * 4)

        # Buffer should contain records from both batches
        assert len(orch._disc_record_buffer["buf-topic"]) == 7

    @patch("schema_infer.plugin.live.ParserFactory")
    def test_stops_checking_after_discriminator_found(self, mock_parser_factory):
        """Once discriminator is found, buffer is cleared."""
        orch = self._make_orch()

        records = [
            {"event_type": "click", "x": 10},
            {"event_type": "view", "page": "/"},
        ] * 5

        with patch(
            "schema_infer.schemas.inference.SchemaInferrer"
        ) as mock_analyzer_cls:
            mock_analyzer = Mock()
            mock_analyzer.detect_discriminator.return_value = "event_type"
            mock_analyzer_cls.return_value = mock_analyzer

            with patch.object(orch, "_process_multi_event_batch"):
                with patch.object(orch, "_parse_messages", return_value=records):
                    orch._process_topic_batch("disc-topic", [(None, b"d")] * 10)

        # Buffer should be cleared after discriminator found
        assert "disc-topic" not in orch._disc_record_buffer
        assert orch._topic_discriminators["disc-topic"] == "event_type"

    @patch("schema_infer.plugin.live.ParserFactory")
    def test_discriminator_only_for_json_schema(self, mock_parser_factory):
        """When schema_format is 'avro', discriminator detection is skipped."""
        orch = self._make_orch(schema_format="avro")

        records = [
            {"event_type": "click", "x": 10},
            {"event_type": "view", "page": "/"},
        ] * 5

        with patch.object(orch, "_parse_messages", return_value=records):
            with patch.object(orch, "_process_flat_batch") as mock_flat:
                orch._process_topic_batch("avro-topic", [(None, b"d")] * 10)
                mock_flat.assert_called_once()

        # No discriminator entry should exist
        assert "avro-topic" not in orch._topic_discriminators


# ===========================================================================
# TestProcessFlatBatch
# ===========================================================================


class TestProcessFlatBatch:
    """Tests for _process_flat_batch."""

    def test_merges_batch_and_detects_changes(self):
        """merge_batch and detect_changes should be invoked."""
        orch = _make_orchestrator(config=_make_config())

        mock_schema = Mock()
        mock_schema.fields = [Mock()]

        mock_state = Mock(spec=IncrementalSchemaState)
        mock_state.merge_batch.return_value = mock_schema
        mock_state.detect_changes.return_value = SchemaChangeReport()
        mock_state.total_records_processed = 5

        orch._states["flat-topic"] = mock_state
        with patch.object(orch, "_get_or_create_state", return_value=mock_state):
            orch._process_flat_batch("flat-topic", [{"a": 1}])

        mock_state.merge_batch.assert_called_once_with([{"a": 1}])
        mock_state.detect_changes.assert_called_once_with(mock_schema)

    def test_skips_registration_without_partition_zero(self):
        """When this instance does NOT own partition 0, registration is skipped."""
        orch = _make_orchestrator(config=_make_config(), register=True)
        # Bypass real registry creation
        orch.registry = Mock()
        orch._topic_partitions["no-p0-topic"] = {1, 2}

        mock_schema = Mock()
        mock_schema.fields = [Mock(name="f1")]

        report = SchemaChangeReport(added_fields=["f1"])

        mock_state = Mock(spec=IncrementalSchemaState)
        mock_state.merge_batch.return_value = mock_schema
        mock_state.detect_changes.return_value = report
        mock_state.total_records_processed = 100

        with patch.object(orch, "_get_or_create_state", return_value=mock_state):
            with patch.object(orch, "_handle_schema_registration") as mock_reg:
                orch._process_flat_batch("no-p0-topic", [{"a": 1}])

        mock_reg.assert_not_called()

    def test_registers_when_owns_partition_zero(self):
        """When this instance owns partition 0, registration IS called."""
        orch = _make_orchestrator(config=_make_config(), register=True)
        orch.registry = Mock()
        orch._topic_partitions["p0-topic"] = {0, 1}

        mock_schema = Mock()
        mock_schema.fields = [Mock(name="f1")]

        report = SchemaChangeReport(added_fields=["f1"])

        mock_state = Mock(spec=IncrementalSchemaState)
        mock_state.merge_batch.return_value = mock_schema
        mock_state.detect_changes.return_value = report
        mock_state.total_records_processed = 100

        with patch.object(orch, "_get_or_create_state", return_value=mock_state):
            with patch.object(orch, "_handle_schema_registration") as mock_reg:
                orch._process_flat_batch("p0-topic", [{"a": 1}])

        mock_reg.assert_called_once()


# ===========================================================================
# TestProcessMultiEventBatch
# ===========================================================================


class TestProcessMultiEventBatch:
    """Tests for _process_multi_event_batch."""

    def test_groups_by_discriminator(self):
        """Records should be grouped by the discriminator value."""
        orch = _make_orchestrator(config=_make_config())

        records = [
            {"event_type": "click", "x": 10},
            {"event_type": "click", "x": 20},
            {"event_type": "view", "page": "/home"},
        ]

        mock_state_click = Mock(spec=IncrementalSchemaState)
        mock_state_click.merge_batch.return_value = Mock(fields=[])
        mock_state_click.detect_changes.return_value = SchemaChangeReport()
        mock_state_click.total_records_processed = 2

        mock_state_view = Mock(spec=IncrementalSchemaState)
        mock_state_view.merge_batch.return_value = Mock(fields=[])
        mock_state_view.detect_changes.return_value = SchemaChangeReport()
        mock_state_view.total_records_processed = 1

        def fake_get_or_create(key):
            if "click" in key:
                return mock_state_click
            return mock_state_view

        with patch.object(orch, "_get_or_create_state", side_effect=fake_get_or_create):
            orch._process_multi_event_batch("multi-topic", records, "event_type")

        # Click state should receive 2 records
        click_call_args = mock_state_click.merge_batch.call_args[0][0]
        assert len(click_call_args) == 2
        assert all(r["event_type"] == "click" for r in click_call_args)

        # View state should receive 1 record
        view_call_args = mock_state_view.merge_batch.call_args[0][0]
        assert len(view_call_args) == 1
        assert view_call_args[0]["event_type"] == "view"

    def test_discovers_new_event_types(self):
        """_topic_event_types should be updated with discovered types."""
        orch = _make_orchestrator(config=_make_config())

        records = [
            {"event_type": "click", "x": 10},
            {"event_type": "purchase", "item": "widget"},
        ]

        mock_state = Mock(spec=IncrementalSchemaState)
        mock_state.merge_batch.return_value = Mock(fields=[])
        mock_state.detect_changes.return_value = SchemaChangeReport()
        mock_state.total_records_processed = 1

        with patch.object(orch, "_get_or_create_state", return_value=mock_state):
            orch._process_multi_event_batch("evt-topic", records, "event_type")

        assert "click" in orch._topic_event_types["evt-topic"]
        assert "purchase" in orch._topic_event_types["evt-topic"]


# ===========================================================================
# TestFlatToMultiTransition
# ===========================================================================


class TestFlatToMultiTransition:
    """Tests for flat-to-multi-event schema transition in _handle_multi_event_registration."""

    def _make_transition_orch(self):
        """Create an orchestrator wired for registration tests."""
        orch = _make_orchestrator(
            config=_make_config(),
            register=True,
            schema_format="json-schema",
        )
        orch.registry = Mock()
        orch.registry._generate_subject_name.return_value = "test-topic-value"
        orch.registry.register_multi_event_schemas.return_value = {"sub1": 1, "main": 2}
        orch.registry.get_config.return_value = {"compatibilityLevel": "BACKWARD"}
        # Default: get_latest_schema returns a flat (non-oneOf) schema
        orch.registry.get_latest_schema.return_value = {
            "schema": '{"type":"object","properties":{"x":{"type":"integer"}}}'
        }
        return orch

    @patch("schema_infer.schemas.generators.JSONSchemaGenerator")
    @patch("schema_infer.core.merger.SchemaMerger")
    @patch("schema_infer.core.inferrer.SchemaInferrer")
    def test_sets_compat_to_none_during_transition(
        self, mock_inferrer_cls, mock_merger_cls, mock_json_gen_cls
    ):
        """When a topic was previously flat-registered, compatibility is set to NONE."""
        orch = self._make_transition_orch()
        topic = "trans-topic"

        # Mark as previously flat-registered
        orch._topic_flat_registered.add(topic)
        orch._topic_event_types[topic] = {"typeA", "typeB"}

        # Set up per-type states
        for et in ("typeA", "typeB"):
            key = f"{topic}__evt__{et}"
            state = Mock(spec=IncrementalSchemaState)
            state.last_schema = Mock()
            orch._states[key] = state

        mock_json_gen = Mock()
        mock_json_gen.generate_multi_event.return_value = {
            topic: '{"oneOf":[]}',
            f"{topic}.typeA": '{"type":"object"}',
            f"{topic}.typeB": '{"type":"object"}',
        }
        mock_json_gen_cls.return_value = mock_json_gen

        mock_merger = Mock()
        mock_merger.fetch_existing_sub_schemas.return_value = {}
        mock_merger.merge_multi_event_schemas.return_value = {
            topic: '{"oneOf":[]}',
            f"{topic}.typeA": '{"type":"object"}',
            f"{topic}.typeB": '{"type":"object"}',
        }
        mock_merger_cls.return_value = mock_merger

        orch._handle_multi_event_registration(topic, "event_type")

        # Verify compatibility was set to NONE during transition
        set_config_calls = orch.registry.set_config.call_args_list
        none_calls = [
            c for c in set_config_calls if c[0][0].get("compatibility") == "NONE"
        ]
        assert (
            len(none_calls) >= 1
        ), "set_config(NONE) should be called during transition"

    @patch("schema_infer.schemas.generators.JSONSchemaGenerator")
    @patch("schema_infer.core.merger.SchemaMerger")
    @patch("schema_infer.core.inferrer.SchemaInferrer")
    def test_restores_compat_after_registration(
        self, mock_inferrer_cls, mock_merger_cls, mock_json_gen_cls
    ):
        """After registration, compatibility is restored to the original level."""
        orch = self._make_transition_orch()
        topic = "restore-topic"

        orch._topic_flat_registered.add(topic)
        orch._topic_event_types[topic] = {"typeA", "typeB"}

        for et in ("typeA", "typeB"):
            key = f"{topic}__evt__{et}"
            state = Mock(spec=IncrementalSchemaState)
            state.last_schema = Mock()
            orch._states[key] = state

        mock_json_gen = Mock()
        mock_json_gen.generate_multi_event.return_value = {
            topic: '{"oneOf":[]}',
            f"{topic}.typeA": '{"type":"object"}',
            f"{topic}.typeB": '{"type":"object"}',
        }
        mock_json_gen_cls.return_value = mock_json_gen

        mock_merger = Mock()
        mock_merger.fetch_existing_sub_schemas.return_value = {}
        mock_merger.merge_multi_event_schemas.return_value = {
            topic: '{"oneOf":[]}',
            f"{topic}.typeA": '{"type":"object"}',
            f"{topic}.typeB": '{"type":"object"}',
        }
        mock_merger_cls.return_value = mock_merger

        orch._handle_multi_event_registration(topic, "event_type")

        # The last set_config call should restore BACKWARD
        set_config_calls = orch.registry.set_config.call_args_list
        restore_calls = [
            c for c in set_config_calls if c[0][0].get("compatibility") == "BACKWARD"
        ]
        assert len(restore_calls) >= 1, "Compatibility should be restored to BACKWARD"

    @patch("schema_infer.schemas.generators.JSONSchemaGenerator")
    @patch("schema_infer.core.merger.SchemaMerger")
    @patch("schema_infer.core.inferrer.SchemaInferrer")
    def test_restores_compat_on_failure(
        self, mock_inferrer_cls, mock_merger_cls, mock_json_gen_cls
    ):
        """Even if registration throws, compatibility must still be restored."""
        orch = self._make_transition_orch()
        topic = "fail-topic"

        orch._topic_flat_registered.add(topic)
        orch._topic_event_types[topic] = {"typeA", "typeB"}

        for et in ("typeA", "typeB"):
            key = f"{topic}__evt__{et}"
            state = Mock(spec=IncrementalSchemaState)
            state.last_schema = Mock()
            orch._states[key] = state

        mock_json_gen = Mock()
        mock_json_gen.generate_multi_event.return_value = {
            topic: '{"oneOf":[]}',
            f"{topic}.typeA": '{"type":"object"}',
            f"{topic}.typeB": '{"type":"object"}',
        }
        mock_json_gen_cls.return_value = mock_json_gen

        mock_merger = Mock()
        mock_merger.fetch_existing_sub_schemas.return_value = {}
        mock_merger.merge_multi_event_schemas.return_value = {
            topic: '{"oneOf":[]}',
            f"{topic}.typeA": '{"type":"object"}',
            f"{topic}.typeB": '{"type":"object"}',
        }
        mock_merger_cls.return_value = mock_merger

        # Make registration blow up
        orch.registry.register_multi_event_schemas.side_effect = Exception(
            "Registry down"
        )

        # Should NOT raise — error is caught internally
        orch._handle_multi_event_registration(topic, "event_type")

        # Compatibility should STILL be restored (finally block)
        set_config_calls = orch.registry.set_config.call_args_list
        restore_calls = [
            c for c in set_config_calls if c[0][0].get("compatibility") == "BACKWARD"
        ]
        assert (
            len(restore_calls) >= 1
        ), "Compatibility should be restored even when registration fails"

    @patch("schema_infer.schemas.generators.JSONSchemaGenerator")
    @patch("schema_infer.core.merger.SchemaMerger")
    @patch("schema_infer.core.inferrer.SchemaInferrer")
    def test_skips_transition_if_already_oneof(
        self, mock_inferrer_cls, mock_merger_cls, mock_json_gen_cls
    ):
        """If the existing SR schema is already oneOf, skip compat-NONE transition."""
        orch = self._make_transition_orch()
        topic = "oneof-topic"

        orch._topic_flat_registered.add(topic)
        orch._topic_event_types[topic] = {"typeA", "typeB"}

        for et in ("typeA", "typeB"):
            key = f"{topic}__evt__{et}"
            state = Mock(spec=IncrementalSchemaState)
            state.last_schema = Mock()
            orch._states[key] = state

        # Return a schema that already has oneOf
        orch.registry.get_latest_schema.return_value = {
            "schema": '{"oneOf":[{"$ref":"typeA"},{"$ref":"typeB"}]}'
        }

        mock_json_gen = Mock()
        mock_json_gen.generate_multi_event.return_value = {
            topic: '{"oneOf":[]}',
            f"{topic}.typeA": '{"type":"object"}',
            f"{topic}.typeB": '{"type":"object"}',
        }
        mock_json_gen_cls.return_value = mock_json_gen

        mock_merger = Mock()
        mock_merger.fetch_existing_sub_schemas.return_value = {}
        mock_merger.merge_multi_event_schemas.return_value = {
            topic: '{"oneOf":[]}',
            f"{topic}.typeA": '{"type":"object"}',
            f"{topic}.typeB": '{"type":"object"}',
        }
        mock_merger_cls.return_value = mock_merger

        orch._handle_multi_event_registration(topic, "event_type")

        # Compatibility should NOT have been set to NONE because the
        # optimistic check detected that the existing schema is already oneOf.
        set_config_calls = orch.registry.set_config.call_args_list
        none_calls = [
            c for c in set_config_calls if c[0][0].get("compatibility") == "NONE"
        ]
        assert (
            len(none_calls) == 0
        ), "set_config(NONE) should NOT be called when SR already has oneOf"


# ===========================================================================
# TestEvictIdleStates
# ===========================================================================


class TestEvictIdleStates:
    """Tests for _evict_idle_states."""

    def test_evicts_idle_topics(self):
        """Topics idle longer than the threshold are evicted."""
        cfg = _make_config()
        cfg.live.idle_evict_seconds = 60
        orch = _make_orchestrator(config=cfg, persist_state=True)
        mock_store = Mock()
        orch.state_store = mock_store

        mock_state = Mock(spec=IncrementalSchemaState)
        mock_state.dirty = False
        orch._states["idle-topic"] = mock_state

        # Set activity to 120 seconds ago (beyond 60s threshold)
        orch._topic_last_activity["idle-topic"] = time.time() - 120

        orch._evict_idle_states()

        assert "idle-topic" not in orch._states

    def test_persists_dirty_before_eviction(self):
        """Dirty states are saved before being evicted."""
        cfg = _make_config()
        cfg.live.idle_evict_seconds = 60
        orch = _make_orchestrator(config=cfg, persist_state=True)
        mock_store = Mock()
        orch.state_store = mock_store

        mock_state = Mock(spec=IncrementalSchemaState)
        mock_state.dirty = True
        orch._states["dirty-idle"] = mock_state

        orch._topic_last_activity["dirty-idle"] = time.time() - 120

        orch._evict_idle_states()

        mock_store.save.assert_called_once_with(mock_state)
        assert "dirty-idle" not in orch._states

    def test_keeps_active_topics(self):
        """Topics with recent activity should NOT be evicted."""
        cfg = _make_config()
        cfg.live.idle_evict_seconds = 3600
        orch = _make_orchestrator(config=cfg, persist_state=True)
        mock_store = Mock()
        orch.state_store = mock_store

        mock_state = Mock(spec=IncrementalSchemaState)
        mock_state.dirty = False
        orch._states["active-topic"] = mock_state

        # Activity is very recent
        orch._topic_last_activity["active-topic"] = time.time()

        orch._evict_idle_states()

        assert "active-topic" in orch._states
        mock_store.save.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
