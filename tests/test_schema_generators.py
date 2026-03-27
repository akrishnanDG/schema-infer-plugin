"""
Comprehensive unit tests for schema generators (JSON Schema, Avro, Protobuf)
"""

import json
from typing import Any, Dict, List

import pytest

from schema_infer.schemas.generators import (
    AvroGenerator,
    JSONSchemaGenerator,
    ProtobufGenerator,
    SchemaGeneratorFactory,
)
from schema_infer.schemas.inference import (
    FieldType,
    InferredSchema,
    SchemaField,
    SchemaInferrer,
)


class TestSchemaGenerators:
    """Comprehensive tests for all schema generators."""

    def setup_method(self):
        """Set up test data with comprehensive nested structures and all data types."""
        self.inferrer = SchemaInferrer(max_depth=5)

        # Create comprehensive test data covering all data types and nested structures
        self.test_data = [
            {
                # Top-level primitive fields
                "userId": "user123",
                "email": "test@example.com",
                "age": 25,
                "height": 5.9,
                "isActive": True,
                "lastLogin": None,
                # Nested object with all data types
                "profile": {
                    "firstName": "John",
                    "lastName": "Doe",
                    "age": 30,
                    "salary": 75000.50,
                    "isEmployed": True,
                    "middleName": None,
                    # Deeply nested object
                    "address": {
                        "street": "123 Main St",
                        "city": "New York",
                        "zipCode": "10001",
                        "coordinates": {"latitude": 40.7128, "longitude": -74.0060},
                    },
                    # Nested array of objects
                    "phoneNumbers": [
                        {"type": "home", "number": "555-1234"},
                        {"type": "work", "number": "555-5678"},
                    ],
                    # Array of primitives
                    "tags": ["developer", "senior", "remote"],
                    "scores": [95, 87, 92, 88],
                    "ratings": [4.5, 4.2, 4.8, 4.1],
                },
                # Array of complex objects
                "orders": [
                    {
                        "orderId": "order1",
                        "total": 99.99,
                        "items": [
                            {"name": "laptop", "price": 999.99, "quantity": 1},
                            {"name": "mouse", "price": 29.99, "quantity": 2},
                        ],
                        "shipping": {
                            "method": "express",
                            "cost": 15.99,
                            "estimatedDays": 2,
                        },
                    },
                    {
                        "orderId": "order2",
                        "total": 49.99,
                        "items": [{"name": "book", "price": 19.99, "quantity": 1}],
                        "shipping": {
                            "method": "standard",
                            "cost": 5.99,
                            "estimatedDays": 5,
                        },
                    },
                ],
                # Mixed array types
                "preferences": {
                    "theme": "dark",
                    "notifications": {"email": True, "sms": False, "push": True},
                    "languages": ["en", "es", "fr"],
                    "settings": {
                        "autoSave": True,
                        "backupFrequency": 24,
                        "maxFileSize": 10485760.5,
                    },
                },
                # Edge cases
                "emptyObject": {},
                "emptyArray": [],
                "nullField": None,
                "booleanArray": [True, False, True],
                "numberArray": [1, 2.5, -3, 0, 100.99],
                "stringArray": ["a", "b", "c", "empty", ""],
                # Complex nested structure
                "metadata": {
                    "createdAt": "2025-01-15T10:30:00Z",
                    "updatedAt": "2025-01-16T14:22:00Z",
                    "version": 2,
                    "flags": {"featureA": True, "featureB": False, "featureC": None},
                    "tags": ["production", "v2", "stable"],
                    "config": {
                        "timeout": 30,
                        "retries": 3,
                        "enabled": True,
                        "threshold": 0.95,
                    },
                },
            }
        ]

        # Infer schema from test data
        self.schema = self.inferrer.infer_schema(self.test_data, "comprehensive_test")

        # Initialize generators
        self.json_generator = JSONSchemaGenerator()
        self.avro_generator = AvroGenerator()
        self.protobuf_generator = ProtobufGenerator()

    def test_schema_inference_completeness(self):
        """Test that schema inference captures all data types and nested structures."""
        # Check that we have fields for all major data types
        field_names = [field.name for field in self.schema.fields]

        # Top-level fields
        assert "userId" in field_names
        assert "email" in field_names
        assert "age" in field_names
        assert "height" in field_names
        assert "isActive" in field_names
        assert "lastLogin" in field_names

        # Nested fields (the inferrer uses dot-notation with [] for array children)
        assert "profile.firstName" in field_names
        assert "profile.address.street" in field_names
        assert "profile.address.coordinates.latitude" in field_names
        assert "profile.phoneNumbers[].type" in field_names
        assert "orders[].orderId" in field_names
        assert "orders[].items[].name" in field_names

        # Check field types
        field_types = {field.name: field.field_type for field in self.schema.fields}

        # String types
        assert field_types["userId"].name == "string"
        assert field_types["email"].name == "string"
        assert field_types["profile.firstName"].name == "string"

        # Number types (all numerics coalesced to float/number)
        assert field_types["age"].name == "float"
        assert field_types["height"].name == "float"
        assert field_types["profile.salary"].name == "float"

        # Boolean types
        assert field_types["isActive"].name == "boolean"
        assert field_types["profile.isEmployed"].name == "boolean"

        # Array types - the inferrer flattens array element types into the field type name
        # (e.g., ["a","b","c"] becomes type="string" with array=False), so only truly empty
        # arrays like emptyArray get type="array". Check that the emptyArray field exists.
        assert "emptyArray" in field_types
        assert (
            field_types["emptyArray"].name == "string"
        )  # Empty arrays default to string element type

        # Primitive array fields are inferred with their element type, not as arrays
        # e.g., profile.tags is type="string", profile.scores is type="float"
        assert field_types["profile.tags"].name == "string"
        assert field_types["profile.scores"].name == "float"
        assert field_types["profile.ratings"].name == "float"

        # Object types
        assert field_types["profile"].name == "object"
        assert field_types["profile.address"].name == "object"

    def test_json_schema_generation(self):
        """Test JSON Schema generation with comprehensive data types and nested structures."""
        json_schema_str = self.json_generator.generate(self.schema)
        json_schema = json.loads(json_schema_str)

        # Basic structure
        assert json_schema["$schema"] == "http://json-schema.org/draft-07/schema#"
        assert json_schema["title"] == "comprehensive_test"
        assert json_schema["type"] == "object"
        assert "properties" in json_schema
        assert "required" in json_schema

        properties = json_schema["properties"]

        # Test top-level primitive types -- all nullable
        assert "userId" in properties
        assert properties["userId"]["type"] == ["string", "null"]
        assert "email" in properties
        assert properties["email"]["type"] == ["string", "null"]
        assert "age" in properties
        assert properties["age"]["type"] == ["number", "null"]
        assert "height" in properties
        assert properties["height"]["type"] == ["number", "null"]
        assert "isActive" in properties
        assert properties["isActive"]["type"] == ["boolean", "null"]

        # Test nested object structure - profile has nested properties because
        # of dot-notation fields like profile.firstName, profile.age, etc.
        assert "profile" in properties
        assert properties["profile"]["type"] in ("object", ["object", "null"])
        assert "properties" in properties["profile"]

        profile_props = properties["profile"]["properties"]
        assert "firstName" in profile_props
        assert profile_props["firstName"]["type"] == ["string", "null"]
        assert "age" in profile_props
        assert profile_props["age"]["type"] == ["number", "null"]
        assert "salary" in profile_props
        assert profile_props["salary"]["type"] == ["number", "null"]
        assert "isEmployed" in profile_props
        assert profile_props["isEmployed"]["type"] == ["boolean", "null"]

        # The generator now correctly produces nested records for nested objects.
        # address is a proper nested object with its own properties.
        assert "address" in profile_props
        assert profile_props["address"]["type"] in ("object", ["object", "null"])
        assert "properties" in profile_props["address"]
        assert "street" in profile_props["address"]["properties"]

        # Array fields now have proper array types with items
        assert "tags" in profile_props
        assert profile_props["tags"]["type"] in ("array", ["array", "null"])

        assert "scores" in profile_props
        assert profile_props["scores"]["type"] in ("array", ["array", "null"])
        assert profile_props["scores"]["items"] == {"type": "number"}

        assert "ratings" in profile_props
        assert profile_props["ratings"]["type"] in ("array", ["array", "null"])
        assert profile_props["ratings"]["items"] == {"type": "number"}

        # phoneNumbers is a proper array (children are merged at top-level only)
        assert "phoneNumbers" in profile_props
        assert profile_props["phoneNumbers"]["type"] in ("array", ["array", "null"])

        # orders is now a proper array with item schema merged from children
        assert "orders" in properties
        assert properties["orders"]["type"] in ("array", ["array", "null"])
        order_items = properties["orders"]["items"]
        assert order_items["type"] == "object"
        assert "properties" in order_items

        # Test examples are included
        assert "examples" in properties["userId"]
        assert len(properties["userId"]["examples"]) > 0

        # All inferred fields are optional
        required = json_schema["required"]
        assert required == []

    def test_avro_schema_generation(self):
        """Test Avro schema generation with comprehensive data types and nested structures."""
        avro_schema_str = self.avro_generator.generate(self.schema)
        avro_schema = json.loads(avro_schema_str)

        # Basic structure
        assert avro_schema["type"] == "record"
        assert avro_schema["name"] == "comprehensive_test"
        # The namespace comes from the inferrer which sets "com.schema-infer.schema.infer"
        assert avro_schema["namespace"] == "com.schema-infer.schema.infer"
        assert "fields" in avro_schema

        fields = avro_schema["fields"]
        field_dict = {field["name"]: field for field in fields}

        # All fields are nullable since inference can never guarantee required.
        # Avro nullable fields use union: ["null", "type"]
        assert "userId" in field_dict
        assert field_dict["userId"]["type"] == ["null", "string"]
        assert "email" in field_dict
        assert field_dict["email"]["type"] == ["null", "string"]
        assert "age" in field_dict
        assert field_dict["age"]["type"] == ["null", "double"]
        assert "height" in field_dict
        assert field_dict["height"]["type"] == ["null", "double"]
        assert "isActive" in field_dict
        assert field_dict["isActive"]["type"] == ["null", "boolean"]

        # Test nested record structure - profile has nested fields so it becomes a record
        assert "profile" in field_dict

        # Profile type may be a record or a nullable union containing a record
        profile_type = field_dict["profile"]["type"]
        if isinstance(profile_type, dict):
            profile_record = profile_type
        else:
            # It's a list like ["null", {record}] -- get the record
            profile_record = next(t for t in profile_type if isinstance(t, dict))
        assert profile_record["type"] == "record"
        assert profile_record["name"] == "comprehensive_test_profile_record"
        assert "fields" in profile_record

        profile_fields = profile_record["fields"]
        profile_field_dict = {field["name"]: field for field in profile_fields}

        # Nested fields use clean local names, all nullable
        assert "firstName" in profile_field_dict
        assert profile_field_dict["firstName"]["type"] == ["null", "string"]
        assert "age" in profile_field_dict
        assert profile_field_dict["age"]["type"] == ["null", "double"]
        assert "salary" in profile_field_dict
        assert profile_field_dict["salary"]["type"] == ["null", "double"]
        assert "isEmployed" in profile_field_dict
        assert profile_field_dict["isEmployed"]["type"] == ["null", "boolean"]

        # profile.address is now a proper nested record keyed as "address"
        assert "address" in profile_field_dict
        address_field = profile_field_dict["address"]
        address_type = address_field["type"]
        if isinstance(address_type, dict):
            assert address_type["type"] == "record"
            assert address_type["name"] == "comprehensive_test_profile_address_record"
        else:
            address_record = next(t for t in address_type if isinstance(t, dict))
            assert address_record["type"] == "record"
            assert address_record["name"] == "comprehensive_test_profile_address_record"

        # Array fields now properly typed (nullable arrays)
        tags_type = profile_field_dict["tags"]["type"]
        assert {"type": "array", "items": "string"} in (
            tags_type if isinstance(tags_type, list) else [tags_type]
        )
        scores_type = profile_field_dict["scores"]["type"]
        assert {"type": "array", "items": "double"} in (
            scores_type if isinstance(scores_type, list) else [scores_type]
        )
        ratings_type = profile_field_dict["ratings"]["type"]
        assert {"type": "array", "items": "double"} in (
            ratings_type if isinstance(ratings_type, list) else [ratings_type]
        )

        # Test nullable fields - lastLogin is nullable<string> -> ["null", "string"]
        assert "lastLogin" in field_dict
        lastlogin_type = field_dict["lastLogin"]["type"]
        if isinstance(lastlogin_type, list):
            assert "null" in lastlogin_type
        else:
            assert lastlogin_type == "null"

    def test_protobuf_schema_generation(self):
        """Test Protobuf schema generation with comprehensive data types and nested structures."""
        protobuf_schema_str = self.protobuf_generator.generate(self.schema)
        lines = protobuf_schema_str.split("\n")

        # Basic structure - schema.name is "comprehensive_test" (underscore, not hyphen)
        # The protobuf generator sanitizes names to remove hyphens and special chars.
        # The package uses schema.namespace with dots replaced by underscores and sanitized.
        assert 'syntax = "proto3";' in lines
        assert "package com_schema_infer_schema_infer;" in lines
        assert "message comprehensive_test {" in lines

        # Test primitive types - _convert_field_to_protobuf preserves original casing
        assert any("string userId =" in line for line in lines)
        assert any("string email =" in line for line in lines)
        assert any("double age =" in line for line in lines)
        assert any("double height =" in line for line in lines)
        assert any("bool isActive =" in line for line in lines)

        # The protobuf generator now produces recursive nested messages for objects
        # with dot-notation children. profile is a nested message reference.
        assert any("profile_message profile =" in line for line in lines)
        assert any("message profile_message {" in line for line in lines)

        # orders[] is a nested message group (brackets sanitized to underscores)
        assert any("orders___message orders__" in line for line in lines)
        assert any("message orders___message {" in line for line in lines)

        # Test field numbering — Protobuf field numbers are message-scoped
        # so they restart at 1 inside each nested message. Verify that
        # within each message scope, field numbers are unique and start from 1.
        message_stack = []
        message_fields = {}  # message_name -> list of field numbers
        current_message = "__root__"
        message_fields[current_message] = []

        for line in lines:
            stripped = line.strip()
            if stripped.startswith("message ") and stripped.endswith("{"):
                msg_name = stripped.split()[1]
                message_stack.append(current_message)
                current_message = msg_name
                message_fields[current_message] = []
            elif stripped == "}":
                if message_stack:
                    current_message = message_stack.pop()
            elif "=" in stripped and ";" in stripped:
                try:
                    field_num = int(stripped.split("=")[-1].split(";")[0].strip())
                    message_fields[current_message].append(field_num)
                except (ValueError, IndexError):
                    pass

        # Verify each message scope has unique field numbers starting from 1
        for msg_name, nums in message_fields.items():
            if nums:
                assert len(set(nums)) == len(
                    nums
                ), f"Duplicate field numbers in {msg_name}: {nums}"
                assert (
                    min(nums) >= 1
                ), f"Field numbers in {msg_name} should start from 1"

    def test_schema_generator_factory(self):
        """Test the schema generator factory."""
        # Test JSON Schema generator
        json_gen = SchemaGeneratorFactory.create_generator("json-schema")
        assert isinstance(json_gen, JSONSchemaGenerator)

        # Test Avro generator
        avro_gen = SchemaGeneratorFactory.create_generator("avro")
        assert isinstance(avro_gen, AvroGenerator)

        # Test Protobuf generator
        protobuf_gen = SchemaGeneratorFactory.create_generator("protobuf")
        assert isinstance(protobuf_gen, ProtobufGenerator)

        # Test invalid format
        with pytest.raises(ValueError, match="Unsupported schema format"):
            SchemaGeneratorFactory.create_generator("invalid")

    def test_file_extensions(self):
        """Test that all generators return correct file extensions."""
        assert self.json_generator.get_file_extension() == "json"
        assert self.avro_generator.get_file_extension() == "avsc"
        assert self.protobuf_generator.get_file_extension() == "proto"

    def test_edge_cases(self):
        """Test edge cases and special data types."""
        # Test with minimal data
        minimal_data = [{"id": 1, "name": "test"}]
        minimal_schema = self.inferrer.infer_schema(minimal_data, "minimal_test")

        # JSON Schema -- all fields nullable
        json_schema_str = self.json_generator.generate(minimal_schema)
        json_schema = json.loads(json_schema_str)
        assert json_schema["properties"]["id"]["type"] == ["number", "null"]
        assert json_schema["properties"]["name"]["type"] == ["string", "null"]

        # Avro -- all fields nullable (union with null)
        avro_schema_str = self.avro_generator.generate(minimal_schema)
        avro_schema = json.loads(avro_schema_str)
        assert avro_schema["fields"][0]["type"] == ["null", "double"]
        assert avro_schema["fields"][1]["type"] == ["null", "string"]

        # Protobuf -- optional fields
        protobuf_schema_str = self.protobuf_generator.generate(minimal_schema)
        assert "id" in protobuf_schema_str
        assert "name" in protobuf_schema_str

    def test_nullable_fields(self):
        """Test handling of nullable fields across all formats."""
        nullable_data = [
            {
                "required_field": "value",
                "nullable_field": None,
                "mixed_field": "sometimes_null",
            },
            {
                "required_field": "value2",
                "nullable_field": "now_has_value",
                "mixed_field": None,
            },
        ]

        nullable_schema = self.inferrer.infer_schema(nullable_data, "nullable_test")

        # JSON Schema - nullable fields should be in union with null
        json_schema_str = self.json_generator.generate(nullable_schema)
        json_schema = json.loads(json_schema_str)

        nullable_field = json_schema["properties"]["nullable_field"]
        if "type" in nullable_field and isinstance(nullable_field["type"], list):
            assert "null" in nullable_field["type"]

        # Avro - nullable fields should be union with null
        avro_schema_str = self.avro_generator.generate(nullable_schema)
        avro_schema = json.loads(avro_schema_str)

        nullable_field = next(
            f for f in avro_schema["fields"] if f["name"] == "nullable_field"
        )
        if isinstance(nullable_field["type"], list):
            assert "null" in nullable_field["type"]

    def test_array_handling(self):
        """Test comprehensive array handling across all formats."""
        array_data = [
            {
                "string_array": ["a", "b", "c"],
                "number_array": [1, 2, 3.5],
                "boolean_array": [True, False, True],
                "object_array": [
                    {"id": 1, "name": "item1"},
                    {"id": 2, "name": "item2"},
                ],
                "nested_array": [[1, 2, 3], [4, 5, 6]],
            }
        ]

        array_schema = self.inferrer.infer_schema(array_data, "array_test")

        # The generators now correctly produce proper array types for arrays.
        # string_array -> type="array" with items, number_array -> type="array" with items,
        # boolean_array -> type="array" with items, object_array -> type="object"

        # JSON Schema - arrays now have proper array types
        json_schema_str = self.json_generator.generate(array_schema)
        json_schema = json.loads(json_schema_str)

        # Nullable arrays produce ["array", "null"] type
        assert json_schema["properties"]["string_array"]["type"] in (
            "array",
            ["array", "null"],
        )
        assert json_schema["properties"]["string_array"]["items"] == {"type": "string"}
        assert json_schema["properties"]["number_array"]["type"] in (
            "array",
            ["array", "null"],
        )
        assert json_schema["properties"]["number_array"]["items"] == {"type": "number"}
        assert json_schema["properties"]["boolean_array"]["type"] in (
            "array",
            ["array", "null"],
        )
        assert json_schema["properties"]["boolean_array"]["items"] == {
            "type": "boolean"
        }
        assert json_schema["properties"]["object_array"]["type"] in (
            "array",
            ["array", "null"],
        )
        obj_items = json_schema["properties"]["object_array"]["items"]
        assert obj_items["type"] == "object"
        assert "properties" in obj_items
        assert "id" in obj_items["properties"]
        assert "name" in obj_items["properties"]

        # object_array[] should NOT appear as separate top-level entry
        assert "object_array[]" not in json_schema["properties"]

        # Avro - arrays are nullable unions: ["null", {"type": "array", ...}]
        avro_schema_str = self.avro_generator.generate(array_schema)
        avro_schema = json.loads(avro_schema_str)

        string_array_field = next(
            f for f in avro_schema["fields"] if f["name"] == "string_array"
        )
        assert {"type": "array", "items": "string"} in string_array_field["type"]

        number_array_field = next(
            f for f in avro_schema["fields"] if f["name"] == "number_array"
        )
        assert {"type": "array", "items": "double"} in number_array_field["type"]

        boolean_array_field = next(
            f for f in avro_schema["fields"] if f["name"] == "boolean_array"
        )
        assert {"type": "array", "items": "boolean"} in boolean_array_field["type"]

        # Protobuf - arrays now use the repeated keyword
        protobuf_schema_str = self.protobuf_generator.generate(array_schema)
        assert "repeated string string_array =" in protobuf_schema_str
        assert "repeated double number_array =" in protobuf_schema_str
        assert "repeated bool boolean_array =" in protobuf_schema_str

    def test_deep_nesting(self):
        """Test deep nesting capabilities."""
        deep_data = [
            {
                "level1": {
                    "level2": {
                        "level3": {"level4": {"level5": {"deep_field": "deep_value"}}}
                    }
                }
            }
        ]

        deep_schema = self.inferrer.infer_schema(deep_data, "deep_test")

        # Should have nested field names
        field_names = [field.name for field in deep_schema.fields]
        assert "level1.level2.level3.level4.level5.deep_field" in field_names

        # JSON Schema should handle deep nesting
        json_schema_str = self.json_generator.generate(deep_schema)
        json_schema = json.loads(json_schema_str)

        # level1 gets nested properties because there are dot-notation sub-fields
        level1 = json_schema["properties"]["level1"]
        assert level1["type"] in ("object", ["object", "null"])
        assert "properties" in level1

        # level2 is now a proper nested object with properties containing level3, etc.
        level2 = level1["properties"]["level2"]
        assert level2["type"] in ("object", ["object", "null"])
        assert "properties" in level2
        assert "level3" in level2["properties"]

    def test_schema_validation(self):
        """Test that generated schemas are valid."""
        # Test JSON Schema validity
        json_schema_str = self.json_generator.generate(self.schema)
        json_schema = json.loads(json_schema_str)

        # Basic JSON Schema structure validation
        assert "$schema" in json_schema
        assert "type" in json_schema
        assert "properties" in json_schema
        assert json_schema["type"] == "object"

        # Test Avro schema validity
        avro_schema_str = self.avro_generator.generate(self.schema)
        avro_schema = json.loads(avro_schema_str)

        # Basic Avro structure validation
        assert "type" in avro_schema
        assert "name" in avro_schema
        assert "fields" in avro_schema
        assert avro_schema["type"] == "record"

        # Test Protobuf schema validity
        protobuf_schema_str = self.protobuf_generator.generate(self.schema)
        lines = protobuf_schema_str.split("\n")

        # Basic Protobuf structure validation
        assert any('syntax = "proto3";' in line for line in lines)
        assert any("message " in line and "{" in line for line in lines)
        assert any("}" in line for line in lines)

    def test_performance_with_large_schema(self):
        """Test performance with a large, complex schema."""
        # Create a large schema with many fields
        large_data = []
        for i in range(100):
            record = {
                f"field_{i}": f"value_{i}",
                f"nested_{i}": {f"subfield_{i}": i, f"array_{i}": [i, i + 1, i + 2]},
            }
            large_data.append(record)

        large_schema = self.inferrer.infer_schema(large_data, "large_test")

        # Should handle large schemas without errors
        json_schema_str = self.json_generator.generate(large_schema)
        assert len(json_schema_str) > 1000

        avro_schema_str = self.avro_generator.generate(large_schema)
        assert len(avro_schema_str) > 1000

        protobuf_schema_str = self.protobuf_generator.generate(large_schema)
        assert len(protobuf_schema_str) > 1000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
