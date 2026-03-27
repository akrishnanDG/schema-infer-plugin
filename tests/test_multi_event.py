"""
Tests for multi-event schema detection, merging, and generation.
"""

import json
from collections import Counter
from unittest.mock import MagicMock, patch

import pytest

from schema_infer.config import Config
from schema_infer.core.incremental import IncrementalSchemaState
from schema_infer.core.merger import SchemaMerger
from schema_infer.schemas.generators import JSONSchemaGenerator
from schema_infer.schemas.inference import SchemaInferrer

# ---------------------------------------------------------------------------
# Test data fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def multi_event_records():
    """Records with clear discriminator field and different schemas per type."""
    records = []
    for i in range(10):
        records.append(
            {
                "event_type": "user_created",
                "user_id": f"u{i}",
                "name": f"User{i}",
                "email": f"u{i}@test.com",
            }
        )
    for i in range(8):
        records.append(
            {
                "event_type": "payment",
                "payment_id": f"p{i}",
                "amount": 10.0 + i,
                "currency": "USD",
            }
        )
    for i in range(7):
        records.append(
            {
                "event_type": "order_placed",
                "order_id": f"o{i}",
                "total": 100.0 + i,
                "items": i + 1,
            }
        )
    return records


@pytest.fixture
def single_event_records():
    """Records with same schema but varying values (no real discriminator)."""
    return [
        {"source": "web", "user_id": "u1", "action": "click", "page": "/home"},
        {"source": "mobile", "user_id": "u2", "action": "view", "page": "/products"},
        {"source": "api", "user_id": "u3", "action": "click", "page": "/cart"},
        {"source": "web", "user_id": "u4", "action": "view", "page": "/home"},
        {"source": "mobile", "user_id": "u5", "action": "click", "page": "/checkout"},
    ]


@pytest.fixture
def inferrer():
    return SchemaInferrer()


@pytest.fixture
def config():
    return Config()


# ---------------------------------------------------------------------------
# detect_discriminator tests
# ---------------------------------------------------------------------------


class TestDetectDiscriminator:
    def test_detects_event_type_field(self, inferrer, multi_event_records):
        """Should detect event_type as discriminator."""
        disc = inferrer.detect_discriminator(multi_event_records)
        assert disc == "event_type"

    def test_rejects_same_schema_field(self, inferrer, single_event_records):
        """Should reject 'source' because all groups have identical fields."""
        disc = inferrer.detect_discriminator(single_event_records)
        assert disc is None

    def test_returns_none_for_small_dataset(self, inferrer):
        """Should return None for fewer than 5 records."""
        records = [{"event_type": "a", "f": 1}, {"event_type": "b", "g": 2}]
        assert inferrer.detect_discriminator(records) is None

    def test_returns_none_for_no_string_fields(self, inferrer):
        """Should return None when no string fields exist."""
        records = [{"a": i, "b": i * 2} for i in range(10)]
        assert inferrer.detect_discriminator(records) is None

    def test_returns_none_for_high_cardinality(self, inferrer):
        """Should reject fields with too many unique values (IDs)."""
        records = [
            (
                {"event_type": "a", "id": f"id-{i}", "data": i}
                if i < 5
                else {"event_type": "b", "id": f"id-{i}", "other": i}
            )
            for i in range(10)
        ]
        disc = inferrer.detect_discriminator(records)
        # Should pick event_type, not id
        assert disc == "event_type"

    def test_prioritizes_known_field_names(self, inferrer):
        """Should prefer 'type' over other candidates with same cardinality."""
        records = []
        for i in range(10):
            records.append({"type": "a", "category": "x", "field_a": i, "shared": i})
        for i in range(10):
            records.append({"type": "b", "category": "x", "field_b": i, "shared": i})
        disc = inferrer.detect_discriminator(records)
        # 'category' has same value for all records ("x"), so only 'type' is valid
        assert disc == "type"

    def test_nullable_discriminator_values(self, inferrer):
        """Should handle records where discriminator is None."""
        records = []
        for i in range(8):
            records.append({"event_type": "a", "f1": i})
        records.append({"event_type": None, "f1": 99})
        for i in range(8):
            records.append({"event_type": "b", "f2": i})
        disc = inferrer.detect_discriminator(records)
        assert disc == "event_type"


# ---------------------------------------------------------------------------
# infer_multi_event_schemas tests
# ---------------------------------------------------------------------------


class TestInferMultiEventSchemas:
    def test_groups_by_discriminator(self, inferrer, multi_event_records):
        """Should produce one schema per event type."""
        schemas = inferrer.infer_multi_event_schemas(
            multi_event_records, "event_type", "test-topic"
        )
        assert set(schemas.keys()) == {"user_created", "payment", "order_placed"}

    def test_each_schema_has_correct_fields(self, inferrer, multi_event_records):
        """Each sub-schema should only contain fields from its event type."""
        schemas = inferrer.infer_multi_event_schemas(
            multi_event_records, "event_type", "test-topic"
        )
        user_fields = {f.name for f in schemas["user_created"].fields}
        assert "user_id" in user_fields
        assert "name" in user_fields
        assert "amount" not in user_fields

        payment_fields = {f.name for f in schemas["payment"].fields}
        assert "amount" in payment_fields
        assert "user_id" not in payment_fields

    def test_skips_groups_with_less_than_2_records(self, inferrer):
        """Should skip event types with fewer than 2 records."""
        records = [
            {"event_type": "a", "f1": 1},
            {"event_type": "a", "f1": 2},
            {"event_type": "b", "f2": 3},  # Only 1 record
        ]
        schemas = inferrer.infer_multi_event_schemas(records, "event_type", "test")
        assert "a" in schemas
        assert "b" not in schemas


# ---------------------------------------------------------------------------
# JSONSchemaGenerator.generate_multi_event tests
# ---------------------------------------------------------------------------


class TestGenerateMultiEvent:
    def test_generates_main_and_sub_schemas(self, inferrer, multi_event_records):
        """Should produce a main oneOf schema and per-type sub-schemas."""
        event_schemas = inferrer.infer_multi_event_schemas(
            multi_event_records, "event_type", "test-topic"
        )
        gen = JSONSchemaGenerator()
        result = gen.generate_multi_event("test-topic", event_schemas, "event_type")

        assert "test-topic" in result
        assert "test-topic.user_created" in result
        assert "test-topic.payment" in result
        assert "test-topic.order_placed" in result

    def test_main_schema_has_oneof_refs(self, inferrer, multi_event_records):
        """Main schema should use oneOf with $ref."""
        event_schemas = inferrer.infer_multi_event_schemas(
            multi_event_records, "event_type", "test-topic"
        )
        gen = JSONSchemaGenerator()
        result = gen.generate_multi_event("test-topic", event_schemas, "event_type")
        main = json.loads(result["test-topic"])

        assert "oneOf" in main
        refs = [r["$ref"] for r in main["oneOf"]]
        assert "test-topic-order_placed" in refs
        assert "test-topic-payment" in refs
        assert "test-topic-user_created" in refs

    def test_sub_schemas_are_valid_json_schema(self, inferrer, multi_event_records):
        """Each sub-schema should be valid JSON Schema."""
        event_schemas = inferrer.infer_multi_event_schemas(
            multi_event_records, "event_type", "test-topic"
        )
        gen = JSONSchemaGenerator()
        result = gen.generate_multi_event("test-topic", event_schemas, "event_type")

        for key, schema_json in result.items():
            schema = json.loads(schema_json)
            if key == "test-topic":
                assert "oneOf" in schema
            else:
                assert schema["type"] == "object"
                assert "properties" in schema
                assert schema.get("additionalProperties") is False


# ---------------------------------------------------------------------------
# SchemaMerger tests
# ---------------------------------------------------------------------------


class TestSchemaMerger:
    def test_merge_flat_adds_new_fields(self):
        """Merge should add fields from new schema."""
        merger = SchemaMerger()
        existing = json.dumps(
            {
                "type": "object",
                "properties": {"a": {"type": "string"}, "b": {"type": "integer"}},
                "required": [],
            }
        )
        new = json.dumps(
            {
                "type": "object",
                "properties": {"b": {"type": "integer"}, "c": {"type": "number"}},
                "required": [],
                "additionalProperties": False,
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        assert set(merged["properties"].keys()) == {"a", "b", "c"}

    def test_merge_flat_preserves_existing_fields(self):
        """Fields only in existing should be preserved."""
        merger = SchemaMerger()
        existing = json.dumps(
            {
                "type": "object",
                "properties": {"a": {"type": "string"}, "b": {"type": "integer"}},
                "required": [],
            }
        )
        new = json.dumps(
            {
                "type": "object",
                "properties": {"b": {"type": "integer"}},
                "required": [],
                "additionalProperties": False,
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        assert "a" in merged["properties"]

    def test_merge_flat_preserves_existing_type_on_conflict(self):
        """Existing type should be preserved when types differ (avoid compat errors)."""
        merger = SchemaMerger()
        existing = json.dumps(
            {"type": "object", "properties": {"a": {"type": "string"}}, "required": []}
        )
        new = json.dumps(
            {
                "type": "object",
                "properties": {"a": {"type": "integer"}},
                "required": [],
                "additionalProperties": False,
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        # Type conflict now widens to union instead of silently keeping existing
        assert merged["properties"]["a"]["type"] == ["string", "integer", "null"]

    def test_merge_flat_preserves_additional_properties_from_existing(self):
        """Should preserve additionalProperties from existing schema."""
        merger = SchemaMerger()
        # Existing has no additionalProperties (open model)
        existing = json.dumps(
            {"type": "object", "properties": {"a": {"type": "string"}}, "required": []}
        )
        # New has additionalProperties: false
        new = json.dumps(
            {
                "type": "object",
                "properties": {"a": {"type": "string"}, "b": {"type": "integer"}},
                "required": [],
                "additionalProperties": False,
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        # Should NOT have additionalProperties since existing didn't have it
        assert "additionalProperties" not in merged

    def test_merge_flat_preserves_closed_model(self):
        """Should preserve additionalProperties: false from existing."""
        merger = SchemaMerger()
        existing = json.dumps(
            {
                "type": "object",
                "properties": {"a": {"type": "string"}},
                "required": [],
                "additionalProperties": False,
            }
        )
        new = json.dumps(
            {
                "type": "object",
                "properties": {"a": {"type": "string"}, "b": {"type": "integer"}},
                "required": [],
                "additionalProperties": False,
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        assert merged["additionalProperties"] is False

    def test_merge_flat_handles_invalid_existing(self):
        """Should merge new fields into empty base when existing can't be parsed."""
        merger = SchemaMerger()
        new_json = json.dumps(
            {"type": "object", "properties": {"a": {"type": "string"}}}
        )
        result = merger.merge_flat_schemas("invalid json", new_json)
        merged = json.loads(result)
        # With invalid existing, merger starts from empty and merges new fields in
        assert merged["properties"]["a"]["type"] == "string"
        assert merged["type"] == "object"

    def test_merge_flat_skips_oneof_existing(self):
        """Should not merge flat into an existing oneOf schema."""
        merger = SchemaMerger()
        existing = json.dumps({"oneOf": [{"$ref": "sub-a"}]})
        new_json = json.dumps(
            {"type": "object", "properties": {"a": {"type": "string"}}}
        )
        result = merger.merge_flat_schemas(existing, new_json)
        assert result == new_json

    def test_merge_multi_event_preserves_existing_types(self):
        """Should preserve event types from existing that aren't in new."""
        merger = SchemaMerger()
        existing_main = json.dumps(
            {
                "oneOf": [
                    {"$ref": "topic-type_a"},
                    {"$ref": "topic-type_b"},
                ]
            }
        )
        new_schemas = {
            "type_a": json.dumps(
                {"type": "object", "properties": {"f1": {"type": "string"}}}
            )
        }
        new_main = json.dumps({"oneOf": [{"$ref": "topic-type_a"}]})
        existing_subs = {
            "type_a": json.dumps(
                {"type": "object", "properties": {"f1": {"type": "string"}}}
            ),
            "type_b": json.dumps(
                {"type": "object", "properties": {"f2": {"type": "integer"}}}
            ),
        }

        result = merger.merge_multi_event_schemas(
            existing_main, new_schemas, new_main, "topic", existing_subs
        )

        main = json.loads(result["topic"])
        refs = [r["$ref"] for r in main["oneOf"]]
        assert "topic-type_a" in refs
        assert "topic-type_b" in refs
        assert "topic.type_b" in result


# ---------------------------------------------------------------------------
# IncrementalSchemaState.seed_from_json_schema tests
# ---------------------------------------------------------------------------


class TestSeedFromJsonSchema:
    def test_seeds_fields_from_schema(self, config):
        """Should populate field_analysis from schema properties."""
        schema = json.dumps(
            {
                "type": "object",
                "properties": {
                    "name": {"type": ["string", "null"]},
                    "age": {"type": ["integer", "null"]},
                    "active": {"type": "boolean"},
                },
            }
        )
        state = IncrementalSchemaState.seed_from_json_schema("test", schema, config)
        assert "name" in state.field_analysis
        assert "age" in state.field_analysis
        assert "active" in state.field_analysis
        assert state.total_records_processed == 0  # Seeded from SR, not actual records

    def test_seeded_state_type_detection(self, config):
        """Seeded fields should have correct type counts."""
        schema = json.dumps(
            {
                "type": "object",
                "properties": {
                    "count": {"type": ["integer", "null"]},
                },
            }
        )
        state = IncrementalSchemaState.seed_from_json_schema("test", schema, config)
        assert "integer" in state.field_analysis["count"]["types"]

    def test_seeded_state_handles_invalid_json(self, config):
        """Should return empty state for invalid JSON."""
        state = IncrementalSchemaState.seed_from_json_schema("test", "invalid", config)
        assert len(state.field_analysis) == 0

    def test_seeded_state_is_dirty(self, config):
        """Seeded state should be marked dirty for persistence."""
        schema = json.dumps({"type": "object", "properties": {"a": {"type": "string"}}})
        state = IncrementalSchemaState.seed_from_json_schema("test", schema, config)
        assert state.dirty is True

    def test_seeded_state_can_merge_batch(self, config):
        """Seeded state should accept new batches on top."""
        schema = json.dumps(
            {"type": "object", "properties": {"existing_field": {"type": "string"}}}
        )
        state = IncrementalSchemaState.seed_from_json_schema("test", schema, config)

        # Merge a batch with a new field
        new_records = [{"existing_field": "val", "new_field": 42}]
        result = state.merge_batch(new_records)

        field_names = {f.name for f in result.fields}
        assert "existing_field" in field_names
        assert "new_field" in field_names


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_special_characters_in_event_type(self, inferrer):
        """Event types with special chars should work."""
        records = [
            {"event_type": "user.created", "f1": 1},
            {"event_type": "user.created", "f1": 2},
            {"event_type": "user.created", "f1": 3},
            {"event_type": "order-placed", "f2": 4},
            {"event_type": "order-placed", "f2": 5},
            {"event_type": "order-placed", "f2": 6},
        ]
        schemas = inferrer.infer_multi_event_schemas(records, "event_type", "test")
        assert "user.created" in schemas
        assert "order-placed" in schemas

    def test_empty_records_for_discriminator(self, inferrer):
        """Should handle empty record list."""
        assert inferrer.detect_discriminator([]) is None

    def test_single_event_type_returns_none(self):
        """infer_multi_event should return None for single event type."""
        from schema_infer.core.inferrer import SchemaInferrer as CoreInferrer

        inferrer = CoreInferrer(Config())
        messages = [
            (None, json.dumps({"event_type": "only_one", "f": i}).encode())
            for i in range(10)
        ]
        result = inferrer.infer_multi_event(messages, "test-topic")
        assert result is None
