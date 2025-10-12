"""
Comprehensive unit tests for schema inference engine
"""

import pytest
from typing import List, Dict, Any

from schema_infer.schemas.inference import SchemaInferrer, InferredSchema, SchemaField, FieldType


class TestSchemaInference:
    """Comprehensive tests for schema inference engine."""
    
    def setup_method(self):
        """Set up test data."""
        self.inferrer = SchemaInferrer(max_depth=5, confidence_threshold=0.8)
    
    def test_basic_data_types(self):
        """Test inference of basic data types."""
        data = [
            {
                "string_field": "hello",
                "int_field": 42,
                "float_field": 3.14,
                "boolean_field": True,
                "null_field": None
            }
        ]
        
        schema = self.inferrer.infer_schema(data, "basic_types")
        
        field_types = {field.name: field.field_type for field in schema.fields}
        
        assert field_types["string_field"].name == "string"
        assert field_types["int_field"].name == "int"
        assert field_types["float_field"].name == "float"
        assert field_types["boolean_field"].name == "boolean"
        assert field_types["null_field"].name == "null"
    
    def test_array_inference(self):
        """Test inference of array types."""
        data = [
            {
                "string_array": ["a", "b", "c"],
                "number_array": [1, 2, 3],
                "mixed_array": [1, "two", 3.0],
                "empty_array": [],
                "nested_array": [[1, 2], [3, 4]]
            }
        ]
        
        schema = self.inferrer.infer_schema(data, "array_types")
        
        field_types = {field.name: field.field_type for field in schema.fields}
        
        assert field_types["string_array"].array == True
        assert field_types["string_array"].name == "string"
        
        assert field_types["number_array"].array == True
        assert field_types["number_array"].name == "int"
        
        assert field_types["mixed_array"].array == True
        assert field_types["mixed_array"].name == "union"
        
        assert field_types["empty_array"].array == True
        
        assert field_types["nested_array"].array == True
        assert field_types["nested_array"].name == "array"
    
    def test_nested_object_inference(self):
        """Test inference of nested objects."""
        data = [
            {
                "user": {
                    "name": "John",
                    "age": 30,
                    "address": {
                        "street": "123 Main St",
                        "city": "New York"
                    }
                }
            }
        ]
        
        schema = self.inferrer.infer_schema(data, "nested_objects")
        
        field_names = [field.name for field in schema.fields]
        
        # Should have nested field names
        assert "user.name" in field_names
        assert "user.age" in field_names
        assert "user.address.street" in field_names
        assert "user.address.city" in field_names
        
        # Check types
        field_types = {field.name: field.field_type for field in schema.fields}
        assert field_types["user.name"].name == "string"
        assert field_types["user.age"].name == "int"
        assert field_types["user.address.street"].name == "string"
        assert field_types["user.address.city"].name == "string"
    
    def test_array_of_objects_inference(self):
        """Test inference of arrays containing objects."""
        data = [
            {
                "items": [
                    {"id": 1, "name": "item1"},
                    {"id": 2, "name": "item2"}
                ]
            }
        ]
        
        schema = self.inferrer.infer_schema(data, "array_of_objects")
        
        field_names = [field.name for field in schema.fields]
        
        # Should have array notation for nested fields
        assert "items[].id" in field_names
        assert "items[].name" in field_names
        
        # Check types
        field_types = {field.name: field.field_type for field in schema.fields}
        assert field_types["items[].id"].name == "int"
        assert field_types["items[].name"].name == "string"
    
    def test_nullable_fields(self):
        """Test inference of nullable fields."""
        data = [
            {"field": "value"},
            {"field": None},
            {"field": "another_value"}
        ]
        
        schema = self.inferrer.infer_schema(data, "nullable_fields")
        
        field_types = {field.name: field.field_type for field in schema.fields}
        
        # Field should be nullable
        assert field_types["field"].nullable == True
        assert field_types["field"].name == "string"
    
    def test_union_types(self):
        """Test inference of union types."""
        data = [
            {"field": "string"},
            {"field": 42},
            {"field": True},
            {"field": None}
        ]
        
        schema = self.inferrer.infer_schema(data, "union_types")
        
        field_types = {field.name: field.field_type for field in schema.fields}
        
        # Should detect as union type
        assert field_types["field"].name == "union"
        assert field_types["field"].nullable == True
    
    def test_examples_collection(self):
        """Test that examples are collected correctly."""
        data = [
            {"field": "value1"},
            {"field": "value2"},
            {"field": "value3"},
            {"field": "value4"},
            {"field": "value5"},
            {"field": "value6"}  # Should be limited to 5 examples
        ]
        
        schema = self.inferrer.infer_schema(data, "examples_test")
        
        field = next(f for f in schema.fields if f.name == "field")
        
        # Should have examples
        assert len(field.examples) > 0
        assert len(field.examples) <= 5  # Limited to 5 examples
        assert "value1" in field.examples
    
    def test_required_field_detection(self):
        """Test detection of required vs optional fields."""
        data = [
            {"required_field": "always_present", "optional_field": "sometimes"},
            {"required_field": "always_present", "optional_field": None},
            {"required_field": "always_present"}  # optional_field missing
        ]
        
        schema = self.inferrer.infer_schema(data, "required_fields")
        
        required_field = next(f for f in schema.fields if f.name == "required_field")
        optional_field = next(f for f in schema.fields if f.name == "optional_field")
        
        assert required_field.required == True
        assert optional_field.required == False
    
    def test_max_depth_limiting(self):
        """Test that max depth is respected."""
        # Create deeply nested data
        data = [{"level1": {"level2": {"level3": {"level4": {"level5": {"level6": "deep"}}}}}}]
        
        # Set max depth to 3
        limited_inferrer = SchemaInferrer(max_depth=3)
        schema = limited_inferrer.infer_schema(data, "deep_test")
        
        field_names = [field.name for field in schema.fields]
        
        # Should not go beyond max depth
        assert "level1.level2.level3.level4" not in field_names
        assert "level1.level2.level3" in field_names
    
    def test_empty_data_handling(self):
        """Test handling of empty data."""
        with pytest.raises(ValueError, match="No data provided"):
            self.inferrer.infer_schema([], "empty_test")
    
    def test_single_record(self):
        """Test inference from single record."""
        data = [{"field": "value"}]
        
        schema = self.inferrer.infer_schema(data, "single_record")
        
        assert len(schema.fields) == 1
        assert schema.fields[0].name == "field"
        assert schema.fields[0].field_type.name == "string"
    
    def test_complex_nested_arrays(self):
        """Test inference of complex nested array structures."""
        data = [
            {
                "matrix": [
                    [1, 2, 3],
                    [4, 5, 6]
                ],
                "objects": [
                    {
                        "nested": {
                            "array": [1, 2, 3]
                        }
                    }
                ]
            }
        ]
        
        schema = self.inferrer.infer_schema(data, "complex_arrays")
        
        field_names = [field.name for field in schema.fields]
        
        # Should handle nested arrays
        assert "matrix[][]" in field_names
        assert "objects[].nested.array[]" in field_names
    
    def test_field_type_equality(self):
        """Test FieldType equality and hashing."""
        type1 = FieldType("string", nullable=False, array=False)
        type2 = FieldType("string", nullable=False, array=False)
        type3 = FieldType("string", nullable=True, array=False)
        
        assert type1 == type2
        assert type1 != type3
        assert hash(type1) == hash(type2)
        assert hash(type1) != hash(type3)
    
    def test_schema_field_creation(self):
        """Test SchemaField creation and properties."""
        field_type = FieldType("string", nullable=True, array=False)
        field = SchemaField(
            name="test_field",
            field_type=field_type,
            required=True,
            description="Test field",
            examples={"example1", "example2"},
            default_value="default"
        )
        
        assert field.name == "test_field"
        assert field.field_type == field_type
        assert field.required == True
        assert field.description == "Test field"
        assert "example1" in field.examples
        assert field.default_value == "default"
    
    def test_inferred_schema_creation(self):
        """Test InferredSchema creation and properties."""
        fields = [
            SchemaField("field1", FieldType("string"), True, "Field 1", {"example1"}),
            SchemaField("field2", FieldType("int"), False, "Field 2", {"example2"})
        ]
        
        schema = InferredSchema(
            name="test_schema",
            fields=fields,
            namespace="com.test",
            description="Test schema"
        )
        
        assert schema.name == "test_schema"
        assert len(schema.fields) == 2
        assert schema.namespace == "com.test"
        assert schema.description == "Test schema"
    
    def test_array_handling_strategies(self):
        """Test different array handling strategies."""
        data = [{"array": [1, "two", 3.0]}]
        
        # Test union strategy
        union_inferrer = SchemaInferrer(array_handling="union")
        union_schema = union_inferrer.infer_schema(data, "union_test")
        union_field = next(f for f in union_schema.fields if f.name == "array")
        assert union_field.field_type.name == "union"
        
        # Test first strategy
        first_inferrer = SchemaInferrer(array_handling="first")
        first_schema = first_inferrer.infer_schema(data, "first_test")
        first_field = next(f for f in first_schema.fields if f.name == "array")
        assert first_field.field_type.name == "int"  # First element type
        
        # Test all strategy
        all_inferrer = SchemaInferrer(array_handling="all")
        all_schema = all_inferrer.infer_schema(data, "all_test")
        all_field = next(f for f in all_schema.fields if f.name == "array")
        assert all_field.field_type.name == "union"  # Mixed types
    
    def test_null_handling_strategies(self):
        """Test different null handling strategies."""
        data = [
            {"field": "value"},
            {"field": None}
        ]
        
        # Test optional strategy
        optional_inferrer = SchemaInferrer(null_handling="optional")
        optional_schema = optional_inferrer.infer_schema(data, "optional_test")
        optional_field = next(f for f in optional_schema.fields if f.name == "field")
        assert optional_field.required == False
        
        # Test required strategy
        required_inferrer = SchemaInferrer(null_handling="required")
        required_schema = required_inferrer.infer_schema(data, "required_test")
        required_field = next(f for f in required_schema.fields if f.name == "field")
        assert required_field.required == True
        
        # Test ignore strategy
        ignore_inferrer = SchemaInferrer(null_handling="ignore")
        ignore_schema = ignore_inferrer.infer_schema(data, "ignore_test")
        ignore_field = next(f for f in ignore_schema.fields if f.name == "field")
        assert ignore_field.required == True  # Nulls ignored, so field appears required


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
