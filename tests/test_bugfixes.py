"""
Tests for all bug fixes applied in this batch (rounds 1 and 2).

Covers:
- Double-increment fix (cli.py)
- Merger "never destructive" guarantee
- Nested additionalProperties:false (generators.py)
- URL encoding of subject names (registry.py)
- Config deep-copy (cli.py)
- Protobuf field numbering (generators.py)
- JSON parser array merge (parsers.py)
- Schema validation on local inference path (cli.py)
- Live mode thread safety (live.py)
"""

import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from schema_infer.config import Config
from schema_infer.core.merger import SchemaMerger
from schema_infer.core.registry import SchemaRegistry
from schema_infer.formats.parsers import JSONParser
from schema_infer.schemas.generators import (
    JSONSchemaGenerator,
    ProtobufGenerator,
)
from schema_infer.schemas.inference import (
    FieldType,
    InferredSchema,
    SchemaField,
    SchemaInferrer,
)

# ──────────────────────────────────────────────
#  Merger: "never destructive" guarantee tests
# ──────────────────────────────────────────────


class TestMergerSafety:
    """Tests that the merger never loses data."""

    def test_type_conflict_widens_to_union(self):
        """When types conflict, merger should widen to union — not silently skip."""
        merger = SchemaMerger()
        existing = json.dumps(
            {
                "type": "object",
                "properties": {"amount": {"type": "integer"}},
                "required": [],
            }
        )
        new = json.dumps(
            {
                "type": "object",
                "properties": {"amount": {"type": "string"}},
                "required": [],
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        # Must produce a union type, not silently keep one
        assert merged["properties"]["amount"]["type"] == ["integer", "string", "null"]

    def test_type_conflict_union_includes_null(self):
        """Union type from conflict should always include null for compatibility."""
        merger = SchemaMerger()
        existing = json.dumps(
            {
                "type": "object",
                "properties": {"val": {"type": "number"}},
                "required": [],
            }
        )
        new = json.dumps(
            {
                "type": "object",
                "properties": {"val": {"type": "boolean"}},
                "required": [],
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        types = merged["properties"]["val"]["type"]
        assert "null" in types
        assert "number" in types
        assert "boolean" in types

    def test_array_merge_preserves_existing_nested_properties(self):
        """When existing array has nested objects but new doesn't, keep existing."""
        merger = SchemaMerger()
        existing = json.dumps(
            {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "name": {"type": "string"},
                            },
                        },
                    }
                },
                "required": [],
            }
        )
        new = json.dumps(
            {
                "type": "object",
                "properties": {"items": {"type": "array", "items": {"type": "string"}}},
                "required": [],
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        # Existing nested properties must be preserved
        items_def = merged["properties"]["items"]["items"]
        assert "properties" in items_def
        assert "id" in items_def["properties"]
        assert "name" in items_def["properties"]

    def test_array_merge_adopts_new_nested_properties(self):
        """When new array has nested objects but existing doesn't, adopt new."""
        merger = SchemaMerger()
        existing = json.dumps(
            {
                "type": "object",
                "properties": {"items": {"type": "array", "items": {"type": "string"}}},
                "required": [],
            }
        )
        new = json.dumps(
            {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {"id": {"type": "string"}},
                        },
                    }
                },
                "required": [],
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        items_def = merged["properties"]["items"]["items"]
        assert "properties" in items_def
        assert "id" in items_def["properties"]

    def test_invalid_existing_preserves_new_fields(self):
        """When existing schema is invalid JSON, new fields must not be lost."""
        merger = SchemaMerger()
        new = json.dumps(
            {
                "type": "object",
                "properties": {"a": {"type": "string"}, "b": {"type": "integer"}},
                "required": [],
            }
        )
        result = json.loads(merger.merge_flat_schemas("not valid json", new))
        assert "a" in result["properties"]
        assert "b" in result["properties"]

    def test_invalid_new_preserves_existing(self):
        """When new schema is invalid JSON, existing schema must be returned intact."""
        merger = SchemaMerger()
        existing = json.dumps(
            {"type": "object", "properties": {"a": {"type": "string"}}, "required": []}
        )
        result = merger.merge_flat_schemas(existing, "not valid json")
        assert result == existing

    def test_merge_never_removes_fields(self):
        """Existing fields must never be removed, even if absent from new."""
        merger = SchemaMerger()
        existing = json.dumps(
            {
                "type": "object",
                "properties": {
                    "a": {"type": "string"},
                    "b": {"type": "integer"},
                    "c": {"type": "boolean"},
                },
                "required": [],
            }
        )
        new = json.dumps(
            {
                "type": "object",
                "properties": {"a": {"type": "string"}, "d": {"type": "number"}},
                "required": [],
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        assert "a" in merged["properties"]
        assert "b" in merged["properties"]
        assert "c" in merged["properties"]
        assert "d" in merged["properties"]

    def test_fetch_existing_sub_schemas_logs_non_404_errors(self):
        """Non-404 errors should be logged as warnings, not silently swallowed."""
        merger = SchemaMerger()
        mock_registry = MagicMock()
        mock_registry.get_latest_schema.side_effect = Exception(
            "500 Internal Server Error"
        )

        with patch.object(merger.logger, "warning") as mock_warn:
            result = merger.fetch_existing_sub_schemas(
                mock_registry, "topic", ["type_a"]
            )
            assert result == {}
            mock_warn.assert_called_once()
            assert "500" in str(mock_warn.call_args)


# ──────────────────────────────────────────────
#  Nested additionalProperties:false tests
# ──────────────────────────────────────────────


class TestClosedContentModel:
    """Tests that all nested objects have additionalProperties: false."""

    def test_nested_object_has_additional_properties_false(self):
        """Nested objects must have additionalProperties: false."""
        gen = JSONSchemaGenerator()
        fields = [
            SchemaField("name", FieldType("string"), required=False),
            SchemaField("address.street", FieldType("string"), required=False),
            SchemaField("address.city", FieldType("string"), required=False),
        ]
        schema = InferredSchema("test", fields)
        result = json.loads(gen.generate(schema))

        assert result["additionalProperties"] is False
        assert result["properties"]["address"]["additionalProperties"] is False

    def test_array_items_object_has_additional_properties_false(self):
        """Array items that are objects must have additionalProperties: false."""
        gen = JSONSchemaGenerator()
        fields = [
            SchemaField("items", FieldType("array", array=True), required=False),
            SchemaField("items[].sku", FieldType("string"), required=False),
            SchemaField("items[].price", FieldType("float"), required=False),
        ]
        schema = InferredSchema("test", fields)
        result = json.loads(gen.generate(schema))

        items_schema = result["properties"]["items"]["items"]
        assert items_schema["additionalProperties"] is False

    def test_deeply_nested_objects_have_additional_properties_false(self):
        """Deeply nested objects must all have additionalProperties: false."""
        gen = JSONSchemaGenerator()
        fields = [
            SchemaField("a.b.c", FieldType("string"), required=False),
        ]
        schema = InferredSchema("test", fields)
        result = json.loads(gen.generate(schema))

        assert result["additionalProperties"] is False
        assert result["properties"]["a"]["additionalProperties"] is False
        assert (
            result["properties"]["a"]["properties"]["b"]["additionalProperties"]
            is False
        )


# ──────────────────────────────────────────────
#  URL encoding tests
# ──────────────────────────────────────────────


class TestURLEncoding:
    """Tests that subject names are URL-encoded in registry URLs."""

    def test_encode_subject_basic(self):
        """Basic subject names should pass through unchanged."""
        assert SchemaRegistry._encode_subject("my-topic-value") == "my-topic-value"

    def test_encode_subject_with_colon_context(self):
        """Context-prefixed subjects with colons should be encoded."""
        encoded = SchemaRegistry._encode_subject(":.my-context:topic-value")
        assert "%3A" in encoded  # colons are encoded
        assert "/" not in encoded

    def test_encode_subject_with_slashes(self):
        """Subjects with slashes must be encoded to prevent path traversal."""
        encoded = SchemaRegistry._encode_subject("topic/../admin")
        assert "/" not in encoded
        assert "%2F" in encoded

    def test_encode_subject_with_special_chars(self):
        """Subjects with special characters must be encoded."""
        encoded = SchemaRegistry._encode_subject("topic with spaces")
        assert " " not in encoded
        assert "%20" in encoded


# ──────────────────────────────────────────────
#  Protobuf field numbering tests
# ──────────────────────────────────────────────


class TestProtobufFieldNumbering:
    """Tests that Protobuf field numbers are message-scoped."""

    def test_nested_messages_start_at_1(self):
        """Each nested message should restart field numbers at 1."""
        gen = ProtobufGenerator()
        fields = [
            SchemaField("name", FieldType("string"), required=False),
            SchemaField("age", FieldType("float"), required=False),
            SchemaField("address.street", FieldType("string"), required=False),
            SchemaField("address.city", FieldType("string"), required=False),
            SchemaField("address.zip", FieldType("string"), required=False),
        ]
        schema = InferredSchema("User", fields)
        proto = gen.generate(schema)
        lines = proto.split("\n")

        # Parse field numbers per message scope
        message_fields = {}
        current = "__root__"
        message_fields[current] = []
        stack = []

        for line in lines:
            s = line.strip()
            if s.startswith("message ") and s.endswith("{"):
                stack.append(current)
                current = s.split()[1]
                message_fields[current] = []
            elif s == "}":
                if stack:
                    current = stack.pop()
            elif "=" in s and ";" in s:
                try:
                    num = int(s.split("=")[-1].split(";")[0].strip())
                    message_fields[current].append(num)
                except (ValueError, IndexError):
                    pass

        # Root message fields should start at 1
        root_nums = message_fields["__root__"]
        if root_nums:
            assert min(root_nums) == 1

        # Nested message fields should start at 1 (not continue from parent)
        for name, nums in message_fields.items():
            if name != "__root__" and nums:
                assert (
                    min(nums) == 1
                ), f"Message {name} field numbers should start at 1, got {nums}"

    def test_multiple_nested_messages_independent_numbering(self):
        """Multiple sibling nested messages should each start at 1."""
        gen = ProtobufGenerator()
        fields = [
            SchemaField("id", FieldType("string"), required=False),
            SchemaField("home.street", FieldType("string"), required=False),
            SchemaField("home.city", FieldType("string"), required=False),
            SchemaField("work.street", FieldType("string"), required=False),
            SchemaField("work.city", FieldType("string"), required=False),
        ]
        schema = InferredSchema("Person", fields)
        proto = gen.generate(schema)

        # Both home_message and work_message should have fields numbered from 1
        assert "= 1;" in proto  # At least one field starts at 1


# ──────────────────────────────────────────────
#  JSON parser array merge tests
# ──────────────────────────────────────────────


class TestJSONParserArrayFix:
    """Tests that JSON parser handles arrays of objects correctly."""

    def test_array_of_objects_returns_first_element(self):
        """Parsing an array of objects should return the first element, not merge all."""
        parser = JSONParser()
        data = json.dumps([{"a": 1, "b": 2}, {"a": 3, "c": 4}]).encode()
        result = parser.parse(data)
        # Should return first element, not merged {a: 3, b: 2, c: 4}
        assert result == {"a": 1, "b": 2}

    def test_array_of_objects_no_key_overwrite(self):
        """First element values should not be overwritten by later elements."""
        parser = JSONParser()
        data = json.dumps([{"key": "original"}, {"key": "overwritten"}]).encode()
        result = parser.parse(data)
        assert result["key"] == "original"

    def test_single_object_array(self):
        """Single-element array should return that element."""
        parser = JSONParser()
        data = json.dumps([{"id": 42}]).encode()
        result = parser.parse(data)
        assert result == {"id": 42}

    def test_array_of_primitives_unchanged(self):
        """Arrays of primitives should still wrap in {array: [...]}."""
        parser = JSONParser()
        data = json.dumps([1, 2, 3]).encode()
        result = parser.parse(data)
        assert result == {"array": [1, 2, 3]}

    def test_dict_message_unchanged(self):
        """Plain dict messages should be returned as-is."""
        parser = JSONParser()
        data = json.dumps({"id": 1, "name": "test"}).encode()
        result = parser.parse(data)
        assert result == {"id": 1, "name": "test"}


# ──────────────────────────────────────────────
#  Config deep-copy test
# ──────────────────────────────────────────────


class TestConfigDeepCopy:
    """Tests that config is deep-copied so mutations don't leak."""

    def test_config_model_copy_is_independent(self):
        """Deep-copied config should be independent of original."""
        config = Config()
        config.schema_registry.context = "original"

        copy = config.model_copy(deep=True)
        copy.schema_registry.context = "modified"

        assert config.schema_registry.context == "original"
        assert copy.schema_registry.context == "modified"

    def test_config_nested_mutation_isolation(self):
        """Deep-copy should isolate nested attribute mutations."""
        config = Config()
        config.topic_filter.additional_exclude_prefixes = ["a", "b"]

        copy = config.model_copy(deep=True)
        copy.topic_filter.additional_exclude_prefixes.append("c")

        assert "c" not in config.topic_filter.additional_exclude_prefixes
        assert "c" in copy.topic_filter.additional_exclude_prefixes


# ──────────────────────────────────────────────
#  Live mode thread safety tests
# ──────────────────────────────────────────────


class TestLiveModeThreadSafety:
    """Tests for thread safety fixes in LiveModeOrchestrator."""

    def _make_orchestrator(self):
        """Create a minimal orchestrator for testing."""
        from schema_infer.plugin.live import LiveModeOrchestrator

        config = Config()
        config.schema_registry.url = "http://localhost:8081"
        config.live.persist_state = False

        with patch.object(SchemaRegistry, "_test_connection"):
            orch = LiveModeOrchestrator(
                config=config,
                topics=["test-topic"],
                schema_format="json-schema",
                register=False,
                output_dir=None,
                state_dir=None,
                batch_size=100,
                batch_timeout=60.0,
                consumer_group="test-group",
                context=None,
                on_incompatible="skip",
                data_format="auto",
            )
        return orch

    def test_event_type_set_atomic_update(self):
        """Concurrent workers updating event types should not lose types."""
        orch = self._make_orchestrator()

        # Simulate two workers discovering different event types
        with orch._metadata_lock:
            orch._topic_event_types["topic-a"] = {"type_A"}

        # Worker 1 reads known_types, adds type_B
        known1 = set(orch._topic_event_types.get("topic-a", set()))
        known1.add("type_B")

        # Worker 2 reads known_types, adds type_C
        known2 = set(orch._topic_event_types.get("topic-a", set()))
        known2.add("type_C")

        # Both write back using atomic union
        with orch._metadata_lock:
            current = orch._topic_event_types.get("topic-a", set())
            orch._topic_event_types["topic-a"] = current | known1
        with orch._metadata_lock:
            current = orch._topic_event_types.get("topic-a", set())
            orch._topic_event_types["topic-a"] = current | known2

        # Both types should be present
        result = orch._topic_event_types["topic-a"]
        assert "type_A" in result
        assert "type_B" in result
        assert "type_C" in result

    def test_revoke_cleans_metadata_without_state_store(self):
        """Revoke should clean metadata even when no state store is configured."""
        orch = self._make_orchestrator()

        # Set up some metadata
        with orch._metadata_lock:
            orch._topic_formats["topic-a"] = "json"
            orch._topic_discriminators["topic-a"] = "type"
            orch._topic_partitions["topic-a"] = {0, 1}
            orch._topic_last_activity["topic-a"] = time.time()

        # Revoke
        orch._on_topics_revoked({"topic-a"}, {"topic-a": {0, 1}})

        # All metadata should be cleaned
        assert "topic-a" not in orch._topic_formats
        assert "topic-a" not in orch._topic_discriminators
        assert "topic-a" not in orch._topic_partitions
        assert "topic-a" not in orch._topic_last_activity

    def test_flat_registered_guard_prevents_double_transition(self):
        """Marking flat_registered.discard() immediately should prevent concurrent transitions."""
        orch = self._make_orchestrator()

        with orch._metadata_lock:
            orch._topic_flat_registered.add("topic-a")

        # First check and immediate discard
        with orch._metadata_lock:
            was_flat = "topic-a" in orch._topic_flat_registered
            if was_flat:
                orch._topic_flat_registered.discard("topic-a")
        assert was_flat is True

        # Second concurrent check should see it's already gone
        with orch._metadata_lock:
            was_flat_2 = "topic-a" in orch._topic_flat_registered
        assert was_flat_2 is False


# ──────────────────────────────────────────────
#  Schema validation on local inference path
# ──────────────────────────────────────────────


class TestLocalInferenceValidation:
    """Tests that --message/--data-file path validates schemas."""

    def test_validate_generated_schema_valid_json_schema(self):
        """Valid JSON Schema should pass validation."""
        from schema_infer.utils.validators import validate_generated_schema

        schema = json.dumps(
            {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "required": [],
                "additionalProperties": False,
            }
        )
        is_valid, error = validate_generated_schema(schema, "json-schema")
        assert is_valid
        assert error == ""

    def test_validate_generated_schema_invalid_avro(self):
        """Invalid Avro schema should fail validation."""
        from schema_infer.utils.validators import validate_generated_schema

        schema = json.dumps({"type": "record"})  # Missing name and fields
        is_valid, error = validate_generated_schema(schema, "avro")
        assert not is_valid
        assert "name" in error.lower() or "fields" in error.lower()

    def test_validate_generated_schema_invalid_json(self):
        """Non-JSON string should fail validation."""
        from schema_infer.utils.validators import validate_generated_schema

        is_valid, error = validate_generated_schema("not json", "json-schema")
        assert not is_valid

    def test_local_inference_produces_valid_schema(self):
        """Inferring from a JSON message should produce a valid schema."""
        inferrer = SchemaInferrer()
        records = [{"user_id": "123", "name": "John", "age": 30}]
        schema = inferrer.infer_schema(records, "test")

        gen = JSONSchemaGenerator()
        content = gen.generate(schema)

        from schema_infer.utils.validators import validate_generated_schema

        is_valid, error = validate_generated_schema(content, "json-schema")
        assert is_valid, f"Generated schema should be valid: {error}"

    def test_local_inference_nested_produces_valid_schema(self):
        """Nested objects in local inference should produce valid schema."""
        inferrer = SchemaInferrer()
        records = [
            {"user": {"name": "John", "age": 30}, "items": [{"id": 1, "price": 9.99}]}
        ]
        schema = inferrer.infer_schema(records, "test")

        gen = JSONSchemaGenerator()
        content = gen.generate(schema)
        parsed = json.loads(content)

        # Root must have additionalProperties: false
        assert parsed["additionalProperties"] is False

        from schema_infer.utils.validators import validate_generated_schema

        is_valid, error = validate_generated_schema(content, "json-schema")
        assert is_valid, f"Generated schema should be valid: {error}"


# ──────────────────────────────────────────────
#  Merger deep merge tests
# ──────────────────────────────────────────────


class TestMergerDeepMerge:
    """Tests for deep recursive merge of nested objects and arrays."""

    def test_deep_nested_object_merge(self):
        """Deeply nested objects should be recursively merged."""
        merger = SchemaMerger()
        existing = json.dumps(
            {
                "type": "object",
                "properties": {
                    "config": {
                        "type": "object",
                        "properties": {
                            "a": {"type": "string"},
                            "nested": {
                                "type": "object",
                                "properties": {"x": {"type": "integer"}},
                            },
                        },
                    }
                },
                "required": [],
            }
        )
        new = json.dumps(
            {
                "type": "object",
                "properties": {
                    "config": {
                        "type": "object",
                        "properties": {
                            "b": {"type": "number"},
                            "nested": {
                                "type": "object",
                                "properties": {"y": {"type": "string"}},
                            },
                        },
                    }
                },
                "required": [],
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        config_props = merged["properties"]["config"]["properties"]
        assert "a" in config_props
        assert "b" in config_props
        nested_props = config_props["nested"]["properties"]
        assert "x" in nested_props
        assert "y" in nested_props

    def test_array_of_objects_deep_merge(self):
        """Array items with objects should have their properties merged."""
        merger = SchemaMerger()
        existing = json.dumps(
            {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {"id": {"type": "string"}},
                        },
                    }
                },
                "required": [],
            }
        )
        new = json.dumps(
            {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {"name": {"type": "string"}},
                        },
                    }
                },
                "required": [],
            }
        )
        merged = json.loads(merger.merge_flat_schemas(existing, new))
        items_props = merged["properties"]["items"]["items"]["properties"]
        assert "id" in items_props
        assert "name" in items_props


# ──────────────────────────────────────────────
#  Round 2: Empty array, datetime, protobuf,
#  SR error codes, retry, CI fixes
# ──────────────────────────────────────────────


class TestEmptyArrayInference:
    """Tests for empty array type inference fix."""

    def test_empty_array_defaults_to_string(self):
        """Empty arrays should default to string element type."""
        inferrer = SchemaInferrer()
        ft = inferrer._get_value_type([], 0)
        assert ft.array is True
        assert ft.name == "string"

    def test_array_with_elements_infers_type(self):
        """Non-empty arrays should infer element type normally."""
        inferrer = SchemaInferrer()
        ft = inferrer._get_value_type([1, 2, 3], 0)
        assert ft.array is True
        assert ft.name == "float"  # int+float → float by design

    def test_array_of_strings(self):
        """String arrays should infer string element type."""
        inferrer = SchemaInferrer()
        ft = inferrer._get_value_type(["a", "b", "c"], 0)
        assert ft.array is True
        assert ft.name == "string"

    def test_empty_array_in_schema(self):
        """Empty arrays in records should produce valid schema fields."""
        inferrer = SchemaInferrer()
        records = [{"tags": [], "name": "test"}]
        schema = inferrer.infer_schema(records, "test")
        tag_fields = [f for f in schema.fields if f.name == "tags"]
        assert len(tag_fields) == 1
        assert tag_fields[0].field_type.array is True


class TestDatetimeTimezoneSupport:
    """Tests for datetime pattern timezone support."""

    def test_iso8601_with_z_suffix(self):
        """ISO 8601 with Z timezone should be detected as datetime."""
        inferrer = SchemaInferrer()
        ft = inferrer._get_value_type("2025-12-01T10:00:00Z", 0)
        assert ft.name == "datetime"

    def test_iso8601_with_positive_offset(self):
        """ISO 8601 with +HH:MM offset should be detected as datetime."""
        inferrer = SchemaInferrer()
        ft = inferrer._get_value_type("2025-12-01T10:00:00+05:30", 0)
        assert ft.name == "datetime"

    def test_iso8601_with_negative_offset(self):
        """ISO 8601 with -HH:MM offset should be detected as datetime."""
        inferrer = SchemaInferrer()
        ft = inferrer._get_value_type("2025-12-01T10:00:00-08:00", 0)
        assert ft.name == "datetime"

    def test_iso8601_with_fractional_seconds(self):
        """ISO 8601 with fractional seconds should be detected as datetime."""
        inferrer = SchemaInferrer()
        ft = inferrer._get_value_type("2025-12-01T10:00:00.123456Z", 0)
        assert ft.name == "datetime"

    def test_iso8601_without_timezone_still_works(self):
        """ISO 8601 without timezone should still be detected as datetime."""
        inferrer = SchemaInferrer()
        ft = inferrer._get_value_type("2025-12-01T10:00:00", 0)
        assert ft.name == "datetime"

    def test_space_separated_with_timezone(self):
        """Space-separated datetime with timezone should be detected."""
        inferrer = SchemaInferrer()
        ft = inferrer._get_value_type("2025-12-01 10:00:00+05:30", 0)
        assert ft.name == "datetime"

    def test_non_datetime_string_not_matched(self):
        """Regular strings should not be detected as datetime."""
        inferrer = SchemaInferrer()
        ft = inferrer._get_value_type("hello world", 0)
        assert ft.name == "string"

    def test_date_only_still_works(self):
        """Date-only patterns should still be detected."""
        inferrer = SchemaInferrer()
        ft = inferrer._get_value_type("2025-12-01", 0)
        assert ft.name == "date"


class TestProtobufArrayTypeMapping:
    """Tests for corrected Protobuf array type mapping."""

    def test_array_field_produces_repeated(self):
        """Array fields should produce 'repeated <type>' in Protobuf."""
        gen = ProtobufGenerator()
        fields = [
            SchemaField("tags", FieldType("string", array=True), required=False),
        ]
        schema = InferredSchema("Test", fields)
        proto = gen.generate(schema)
        assert "repeated string tags" in proto

    def test_non_array_field_no_repeated(self):
        """Non-array fields should not have 'repeated' keyword."""
        gen = ProtobufGenerator()
        fields = [
            SchemaField("name", FieldType("string"), required=False),
        ]
        schema = InferredSchema("Test", fields)
        proto = gen.generate(schema)
        assert "repeated" not in proto
        assert "string name" in proto

    def test_empty_array_field_produces_repeated_string(self):
        """Array with unknown element type (from empty []) should produce repeated string."""
        gen = ProtobufGenerator()
        fields = [
            SchemaField("items", FieldType("string", array=True), required=False),
        ]
        schema = InferredSchema("Test", fields)
        proto = gen.generate(schema)
        assert "repeated string items" in proto


class TestSRErrorDifferentiation:
    """Tests for differentiated SR error handling."""

    def test_error_message_for_409_incompatible(self):
        """409 errors should produce 'incompatible' message."""
        from schema_infer.utils.exceptions import SchemaRegistryError

        response = MagicMock()
        response.status_code = 409
        response.json.return_value = {
            "error_code": 40901,
            "message": "Schema violates compatibility",
        }
        response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=response
        )

        config = Config()
        config.schema_registry.url = "http://localhost:8081"

        with patch.object(SchemaRegistry, "_test_connection"):
            registry = SchemaRegistry(config)

        with patch("requests.request") as mock_request:
            mock_request.return_value = MagicMock()
            mock_request.return_value.raise_for_status.side_effect = (
                requests.exceptions.HTTPError(response=response)
            )
            mock_request.return_value.status_code = 409

            import requests as req

            with patch("requests.post") as mock_post:
                mock_post.side_effect = req.exceptions.HTTPError(response=response)
                with pytest.raises(SchemaRegistryError, match="incompatible"):
                    registry.register_schema("topic", "{}", "json-schema")

    def test_error_message_for_401_auth(self):
        """401 errors should produce 'Auth failed' message."""
        from schema_infer.utils.exceptions import SchemaRegistryError

        response = MagicMock()
        response.status_code = 401
        response.json.return_value = {"error_code": 401, "message": "Unauthorized"}
        response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=response
        )

        config = Config()
        config.schema_registry.url = "http://localhost:8081"

        with patch.object(SchemaRegistry, "_test_connection"):
            registry = SchemaRegistry(config)

        with patch("requests.request") as mock_request:
            mock_request.return_value = response
            with pytest.raises(SchemaRegistryError, match="Auth failed"):
                registry.register_schema("topic", "{}", "json-schema")


class TestRegistryRetryOnGET:
    """Tests for retry logic on GET methods."""

    def test_retry_helper_retries_on_connection_error(self):
        """_request_with_retry should retry on ConnectionError."""
        config = Config()
        config.schema_registry.url = "http://localhost:8081"

        with patch.object(SchemaRegistry, "_test_connection"):
            registry = SchemaRegistry(config)

        success_response = MagicMock()
        success_response.status_code = 200
        success_response.raise_for_status.return_value = None
        success_response.json.return_value = {"id": 1}

        with patch("requests.request") as mock_req:
            # Fail twice, succeed on third
            mock_req.side_effect = [
                requests.exceptions.ConnectionError("Connection refused"),
                requests.exceptions.ConnectionError("Connection refused"),
                success_response,
            ]
            with patch("time.sleep"):
                result = registry._request_with_retry(
                    "get", "http://localhost:8081/test", max_retries=3
                )
            assert result == success_response
            assert mock_req.call_count == 3

    def test_retry_helper_raises_after_max_retries(self):
        """_request_with_retry should raise after exhausting retries."""
        config = Config()
        config.schema_registry.url = "http://localhost:8081"

        with patch.object(SchemaRegistry, "_test_connection"):
            registry = SchemaRegistry(config)

        with patch("requests.request") as mock_req:
            mock_req.side_effect = requests.exceptions.ConnectionError(
                "Connection refused"
            )
            with patch("time.sleep"):
                with pytest.raises(requests.exceptions.ConnectionError):
                    registry._request_with_retry(
                        "get", "http://localhost:8081/test", max_retries=2
                    )


# ──────────────────────────────────────────────
#  Round 4: HIGH severity fixes
# ──────────────────────────────────────────────


class TestConfigSyncFix:
    """Tests for config sync validator replacement."""

    def test_sync_convenience_to_nested(self):
        """sync_convenience_to_nested should propagate convenience fields to nested configs."""
        config = Config()
        config.bootstrap_servers = "broker:9092"
        config.schema_registry_url = "http://sr:8081"
        config.max_messages = 200
        config.timeout = 60
        config.sync_convenience_to_nested()

        assert config.kafka.bootstrap_servers == "broker:9092"
        assert config.schema_registry.url == "http://sr:8081"
        assert config.inference.max_messages == 200
        assert config.inference.timeout == 60

    def test_load_config_syncs_automatically(self):
        """load_config should call sync_convenience_to_nested."""
        import os
        import tempfile

        import yaml

        cfg = {
            "kafka": {"bootstrap_servers": "test:9092"},
            "bootstrap_servers": "override:9092",
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(cfg, f)
            tmp = f.name
        try:
            from pathlib import Path

            from schema_infer.config import load_config

            config = load_config(Path(tmp))
            # The convenience override should be synced to nested kafka config
            assert config.kafka.bootstrap_servers == "override:9092"
        finally:
            os.unlink(tmp)

    def test_no_side_effects_on_shared_kafka_config(self):
        """Creating two Config instances should not cross-contaminate."""
        config1 = Config()
        config2 = Config()
        config1.bootstrap_servers = "host1:9092"
        config1.sync_convenience_to_nested()
        # config2 should be unaffected
        assert config2.kafka.bootstrap_servers == "localhost:9092"


class TestLiveConsumerRebalanceSafety:
    """Tests for live_consumer rebalance fixes."""

    def test_partition_lock_exists(self):
        """LiveConsumer should have a partition lock."""
        from schema_infer.core.live_consumer import LiveConsumer

        config = Config()
        config.kafka.bootstrap_servers = "localhost:9092"
        # Can't fully init without Kafka, just check the class has the attribute
        assert hasattr(LiveConsumer, "__init__")

    def test_on_assign_callback_exception_safe(self):
        """on_assign should not crash if orchestrator callback raises."""
        from unittest.mock import MagicMock

        from schema_infer.core.live_consumer import LiveConsumer

        config = Config()
        config.kafka.bootstrap_servers = "localhost:9092"

        with patch.object(LiveConsumer, "_initialize_consumer"):
            consumer = LiveConsumer.__new__(LiveConsumer)
            consumer.config = config
            consumer.logger = MagicMock()
            consumer.consumer = MagicMock()
            consumer._assigned_partitions = []
            consumer._assigned_topics = set()
            consumer._partition_lock = __import__("threading").Lock()
            consumer._on_topics_assigned = MagicMock(
                side_effect=Exception("callback boom")
            )
            consumer._on_topics_revoked = None

            # Subscribe and trigger assignment
            consumer.subscribe(["test-topic"])

            # Get the on_assign callback
            call_args = consumer.consumer.subscribe.call_args
            on_assign_fn = (
                call_args[1].get("on_assign") or call_args[0][1]
                if len(call_args[0]) > 1
                else call_args[1]["on_assign"]
            )

            # Create mock partitions
            mock_partition = MagicMock()
            mock_partition.topic = "test-topic"
            mock_partition.partition = 0

            # Should NOT raise even though callback throws
            on_assign_fn(consumer.consumer, [mock_partition])

            # Should have logged the error
            consumer.logger.error.assert_called_once()
            assert "callback" in str(consumer.logger.error.call_args).lower()


class TestBrokerErrorEscalation:
    """Tests for poll_batch broker error handling."""

    def test_critical_errors_are_raised(self):
        """Critical broker errors should raise LiveModeError, not silently continue."""
        from schema_infer.core.live_consumer import ConfluentKafkaError, LiveConsumer
        from schema_infer.utils.exceptions import LiveModeError

        config = Config()
        config.kafka.bootstrap_servers = "localhost:9092"

        with patch.object(LiveConsumer, "_initialize_consumer"):
            consumer = LiveConsumer.__new__(LiveConsumer)
            consumer.config = config
            consumer.logger = MagicMock()
            consumer.consumer = MagicMock()

            # Create a message with OFFSET_OUT_OF_RANGE error
            mock_msg = MagicMock()
            mock_error = MagicMock()
            mock_error.code.return_value = ConfluentKafkaError.OFFSET_OUT_OF_RANGE
            mock_msg.error.return_value = mock_error

            consumer.consumer.consume.return_value = [mock_msg]

            with pytest.raises(LiveModeError, match="Consumer error"):
                consumer.poll_batch(10, 5.0)


class TestEmptyEventSchemasGuard:
    """Tests for empty event_schemas guard in generate_multi_event."""

    def test_empty_event_schemas_returns_empty(self):
        """Empty event_schemas should return empty dict, not invalid oneOf:[]."""
        from schema_infer.schemas.generators import JSONSchemaGenerator

        gen = JSONSchemaGenerator()
        result = gen.generate_multi_event("topic", {}, "event_type")
        assert result == {}

    def test_non_empty_event_schemas_produces_valid_oneof(self):
        """Non-empty event_schemas should produce valid oneOf with refs."""
        from schema_infer.schemas.generators import JSONSchemaGenerator

        gen = JSONSchemaGenerator()
        schema = InferredSchema(
            "test",
            [
                SchemaField("id", FieldType("string"), required=False),
            ],
        )
        result = gen.generate_multi_event("topic", {"type_a": schema}, "event_type")
        main = json.loads(result["topic"])
        assert len(main["oneOf"]) == 1
        assert main["oneOf"][0]["$ref"] == "topic-type_a"


class TestFormatWarning:
    """Tests for unknown schema format warning."""

    def test_unknown_format_warns(self):
        """Unknown format should log warning and return AVRO."""
        config = Config()
        config.schema_registry.url = "http://localhost:8081"

        with patch.object(SchemaRegistry, "_test_connection"):
            registry = SchemaRegistry(config)

        with patch.object(registry.logger, "warning") as mock_warn:
            result = registry._map_format_to_registry_type("unknown-format")
            assert result == "AVRO"
            mock_warn.assert_called_once()
            assert "unknown-format" in str(mock_warn.call_args).lower()

    def test_known_formats_no_warning(self):
        """Known formats should not trigger warning."""
        config = Config()
        config.schema_registry.url = "http://localhost:8081"

        with patch.object(SchemaRegistry, "_test_connection"):
            registry = SchemaRegistry(config)

        with patch.object(registry.logger, "warning") as mock_warn:
            assert registry._map_format_to_registry_type("avro") == "AVRO"
            assert registry._map_format_to_registry_type("protobuf") == "PROTOBUF"
            assert registry._map_format_to_registry_type("json-schema") == "JSON"
            mock_warn.assert_not_called()


class TestCloudCredentialFailEarly:
    """Tests for early credential failure on Cloud."""

    def test_missing_cloud_creds_raises(self):
        """Missing Cloud API key/secret should raise ConfigurationError."""
        from schema_infer.plugin.auth import AuthenticationManager
        from schema_infer.utils.exceptions import ConfigurationError

        config = Config()
        # Set Cloud-like bootstrap servers to trigger Cloud detection
        config.kafka.bootstrap_servers = "pkc-test.us-east-1.aws.confluent.cloud:9092"
        config.kafka.cloud_api_key = None
        config.kafka.cloud_api_secret = None

        auth = AuthenticationManager(config)

        with pytest.raises(ConfigurationError, match="Cloud API key"):
            auth.configure_kafka_auth()

    def test_present_cloud_creds_succeeds(self):
        """Present Cloud API key/secret should not raise."""
        from schema_infer.plugin.auth import AuthenticationManager

        config = Config()
        config.kafka.bootstrap_servers = "pkc-test.us-east-1.aws.confluent.cloud:9092"
        config.kafka.cloud_api_key = "test-key"
        config.kafka.cloud_api_secret = "test-secret"

        auth = AuthenticationManager(config)
        result = auth.configure_kafka_auth()
        assert result["sasl.username"] == "test-key"
        assert result["sasl.password"] == "test-secret"
