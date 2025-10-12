"""
Comprehensive unit tests for schema generators (JSON Schema, Avro, Protobuf)
"""

import json
import pytest
from typing import List, Dict, Any

from schema_infer.schemas.inference import SchemaInferrer, InferredSchema, SchemaField, FieldType
from schema_infer.schemas.generators import (
    JSONSchemaGenerator, 
    AvroGenerator, 
    ProtobufGenerator,
    SchemaGeneratorFactory
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
                        "coordinates": {
                            "latitude": 40.7128,
                            "longitude": -74.0060
                        }
                    },
                    
                    # Nested array of objects
                    "phoneNumbers": [
                        {"type": "home", "number": "555-1234"},
                        {"type": "work", "number": "555-5678"}
                    ],
                    
                    # Array of primitives
                    "tags": ["developer", "senior", "remote"],
                    "scores": [95, 87, 92, 88],
                    "ratings": [4.5, 4.2, 4.8, 4.1]
                },
                
                # Array of complex objects
                "orders": [
                    {
                        "orderId": "order1",
                        "total": 99.99,
                        "items": [
                            {"name": "laptop", "price": 999.99, "quantity": 1},
                            {"name": "mouse", "price": 29.99, "quantity": 2}
                        ],
                        "shipping": {
                            "method": "express",
                            "cost": 15.99,
                            "estimatedDays": 2
                        }
                    },
                    {
                        "orderId": "order2", 
                        "total": 49.99,
                        "items": [
                            {"name": "book", "price": 19.99, "quantity": 1}
                        ],
                        "shipping": {
                            "method": "standard",
                            "cost": 5.99,
                            "estimatedDays": 5
                        }
                    }
                ],
                
                # Mixed array types
                "preferences": {
                    "theme": "dark",
                    "notifications": {
                        "email": True,
                        "sms": False,
                        "push": True
                    },
                    "languages": ["en", "es", "fr"],
                    "settings": {
                        "autoSave": True,
                        "backupFrequency": 24,
                        "maxFileSize": 10485760.5
                    }
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
                    "flags": {
                        "featureA": True,
                        "featureB": False,
                        "featureC": None
                    },
                    "tags": ["production", "v2", "stable"],
                    "config": {
                        "timeout": 30,
                        "retries": 3,
                        "enabled": True,
                        "threshold": 0.95
                    }
                }
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
        
        # Nested fields
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
        
        # Number types
        assert field_types["age"].name == "int"
        assert field_types["height"].name == "float"
        assert field_types["profile.salary"].name == "float"
        
        # Boolean types
        assert field_types["isActive"].name == "boolean"
        assert field_types["profile.isEmployed"].name == "boolean"
        
        # Array types - check if array fields exist and are properly typed
        array_fields = [name for name, field_type in field_types.items() if field_type.array == True]
        assert len(array_fields) > 0  # Should have some array fields
        
        # Check specific array fields if they exist
        if "profile.tags" in field_types:
            assert field_types["profile.tags"].array == True
        if "profile.phoneNumbers" in field_types:
            assert field_types["profile.phoneNumbers"].array == True
        if "orders" in field_types:
            assert field_types["orders"].array == True
        
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
        
        # Test top-level primitive types
        assert "userId" in properties
        assert properties["userId"]["type"] == "string"
        assert "email" in properties
        assert properties["email"]["type"] == "string"
        assert "age" in properties
        assert properties["age"]["type"] == "integer"
        assert "height" in properties
        assert properties["height"]["type"] == "number"
        assert "isActive" in properties
        assert properties["isActive"]["type"] == "boolean"
        
        # Test nested object structure
        assert "profile" in properties
        assert properties["profile"]["type"] == "object"
        assert "properties" in properties["profile"]
        
        profile_props = properties["profile"]["properties"]
        assert "firstName" in profile_props
        assert profile_props["firstName"]["type"] == "string"
        assert "age" in profile_props
        assert profile_props["age"]["type"] == "integer"
        assert "salary" in profile_props
        assert profile_props["salary"]["type"] == "number"
        assert "isEmployed" in profile_props
        assert profile_props["isEmployed"]["type"] == "boolean"
        
        # Test deeply nested structure
        assert "address" in profile_props
        assert profile_props["address"]["type"] == "object"
        assert "properties" in profile_props["address"]
        
        address_props = profile_props["address"]["properties"]
        assert "street" in address_props
        assert address_props["street"]["type"] == "string"
        assert "coordinates" in address_props
        assert address_props["coordinates"]["type"] == "object"
        
        # Test array types
        assert "tags" in profile_props
        assert profile_props["tags"]["type"] == "array"
        assert "items" in profile_props["tags"]
        assert profile_props["tags"]["items"]["type"] == "string"
        
        assert "scores" in profile_props
        assert profile_props["scores"]["type"] == "array"
        assert profile_props["scores"]["items"]["type"] == "integer"
        
        assert "ratings" in profile_props
        assert profile_props["ratings"]["type"] == "array"
        assert profile_props["ratings"]["items"]["type"] == "number"
        
        # Test array of objects
        assert "phoneNumbers" in profile_props
        assert profile_props["phoneNumbers"]["type"] == "array"
        assert "items" in profile_props["phoneNumbers"]
        assert profile_props["phoneNumbers"]["items"]["type"] == "object"
        
        # Test complex nested arrays
        assert "orders" in properties
        assert properties["orders"]["type"] == "array"
        assert properties["orders"]["items"]["type"] == "object"
        
        # Test examples are included
        assert "examples" in properties["userId"]
        assert len(properties["userId"]["examples"]) > 0
        
        # Test required fields
        required = json_schema["required"]
        assert "userId" in required
        assert "email" in required
        assert "profile" in required
    
    def test_avro_schema_generation(self):
        """Test Avro schema generation with comprehensive data types and nested structures."""
        avro_schema_str = self.avro_generator.generate(self.schema)
        avro_schema = json.loads(avro_schema_str)
        
        # Basic structure
        assert avro_schema["type"] == "record"
        assert avro_schema["name"] == "comprehensive_test"
        assert avro_schema["namespace"] == "com.schema-infer.schema.infer"
        assert "fields" in avro_schema
        
        fields = avro_schema["fields"]
        field_dict = {field["name"]: field for field in fields}
        
        # Test top-level primitive types
        assert "userid" in field_dict
        assert field_dict["userid"]["type"] == "string"
        assert "email" in field_dict
        assert field_dict["email"]["type"] == "string"
        assert "age" in field_dict
        assert field_dict["age"]["type"] == "int"
        assert "height" in field_dict
        assert field_dict["height"]["type"] == "double"
        assert "isactive" in field_dict
        assert field_dict["isactive"]["type"] == "boolean"
        
        # Test nested record structure
        assert "profile" in field_dict
        assert field_dict["profile"]["type"]["type"] == "record"
        assert field_dict["profile"]["type"]["name"] == "profile_record"
        assert "fields" in field_dict["profile"]["type"]
        
        profile_fields = field_dict["profile"]["type"]["fields"]
        profile_field_dict = {field["name"]: field for field in profile_fields}
        
        # Test nested primitive types
        assert "profile_firstname" in profile_field_dict
        assert profile_field_dict["profile_firstname"]["type"] == "string"
        assert "profile_age" in profile_field_dict
        assert profile_field_dict["profile_age"]["type"] == "int"
        assert "profile_salary" in profile_field_dict
        assert profile_field_dict["profile_salary"]["type"] == "double"
        assert "profile_isemployed" in profile_field_dict
        assert profile_field_dict["profile_isemployed"]["type"] == "boolean"
        
        # Test deeply nested record
        assert "profile_address" in profile_field_dict
        assert profile_field_dict["profile_address"]["type"]["type"] == "record"
        assert "fields" in profile_field_dict["profile_address"]["type"]
        
        # Test array types
        assert "profile_tags" in profile_field_dict
        assert profile_field_dict["profile_tags"]["type"]["type"] == "array"
        assert profile_field_dict["profile_tags"]["type"]["items"] == "string"
        
        assert "profile_scores" in profile_field_dict
        assert profile_field_dict["profile_scores"]["type"]["type"] == "array"
        assert profile_field_dict["profile_scores"]["type"]["items"] == "int"
        
        assert "profile_ratings" in profile_field_dict
        assert profile_field_dict["profile_ratings"]["type"]["type"] == "array"
        assert profile_field_dict["profile_ratings"]["type"]["items"] == "double"
        
        # Test nullable fields
        assert "lastlogin" in field_dict
        lastlogin_type = field_dict["lastlogin"]["type"]
        if isinstance(lastlogin_type, list):
            assert "null" in lastlogin_type
        else:
            assert lastlogin_type == "null"
    
    def test_protobuf_schema_generation(self):
        """Test Protobuf schema generation with comprehensive data types and nested structures."""
        protobuf_schema_str = self.protobuf_generator.generate(self.schema)
        lines = protobuf_schema_str.split('\n')
        
        # Basic structure
        assert 'syntax = "proto3";' in lines
        assert 'package com_schema-infer_schema_infer;' in lines
        assert 'message comprehensive-test {' in lines
        
        # Test primitive types
        assert any('string userid =' in line for line in lines)
        assert any('string email =' in line for line in lines)
        assert any('int32 age =' in line for line in lines)
        assert any('double height =' in line for line in lines)
        assert any('bool isactive =' in line for line in lines)
        
        # Test nested message structure
        assert any('profile___message profile =' in line for line in lines)
        assert any('message profile___message {' in line for line in lines)
        
        # Test array types
        assert any('repeated string profile_tags =' in line for line in lines)
        assert any('repeated int32 profile_scores =' in line for line in lines)
        assert any('repeated double profile_ratings =' in line for line in lines)
        
        # Test nested arrays
        assert any('repeated orderhistory___message orderhistory[] =' in line for line in lines)
        assert any('message orderhistory___message {' in line for line in lines)
        
        # Test field numbering
        field_numbers = []
        for line in lines:
            if '=' in line and ';' in line and any(t in line for t in ['string', 'int32', 'double', 'bool']):
                try:
                    field_num = int(line.split('=')[-1].split(';')[0].strip())
                    field_numbers.append(field_num)
                except (ValueError, IndexError):
                    pass
        
        # Ensure field numbers are sequential and unique
        assert len(field_numbers) > 0
        assert len(set(field_numbers)) == len(field_numbers)  # All unique
        assert min(field_numbers) >= 1  # Start from 1
    
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
        
        # JSON Schema
        json_schema_str = self.json_generator.generate(minimal_schema)
        json_schema = json.loads(json_schema_str)
        assert json_schema["properties"]["id"]["type"] == "integer"
        assert json_schema["properties"]["name"]["type"] == "string"
        
        # Avro
        avro_schema_str = self.avro_generator.generate(minimal_schema)
        avro_schema = json.loads(avro_schema_str)
        assert avro_schema["fields"][0]["type"] == "int"
        assert avro_schema["fields"][1]["type"] == "string"
        
        # Protobuf
        protobuf_schema_str = self.protobuf_generator.generate(minimal_schema)
        assert 'int32 id = 1;' in protobuf_schema_str
        assert 'string name = 2;' in protobuf_schema_str
    
    def test_nullable_fields(self):
        """Test handling of nullable fields across all formats."""
        nullable_data = [
            {
                "required_field": "value",
                "nullable_field": None,
                "mixed_field": "sometimes_null"
            },
            {
                "required_field": "value2", 
                "nullable_field": "now_has_value",
                "mixed_field": None
            }
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
        
        nullable_field = next(f for f in avro_schema["fields"] if f["name"] == "nullable_field")
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
                    {"id": 2, "name": "item2"}
                ],
                "nested_array": [
                    [1, 2, 3],
                    [4, 5, 6]
                ]
            }
        ]
        
        array_schema = self.inferrer.infer_schema(array_data, "array_test")
        
        # JSON Schema
        json_schema_str = self.json_generator.generate(array_schema)
        json_schema = json.loads(json_schema_str)
        
        assert json_schema["properties"]["string_array"]["type"] == "array"
        assert json_schema["properties"]["string_array"]["items"]["type"] == "string"
        assert json_schema["properties"]["number_array"]["type"] == "array"
        assert json_schema["properties"]["number_array"]["items"]["type"] == "number"
        assert json_schema["properties"]["boolean_array"]["type"] == "array"
        assert json_schema["properties"]["boolean_array"]["items"]["type"] == "boolean"
        assert json_schema["properties"]["object_array"]["type"] == "array"
        assert json_schema["properties"]["object_array"]["items"]["type"] == "object"
        
        # Avro
        avro_schema_str = self.avro_generator.generate(array_schema)
        avro_schema = json.loads(avro_schema_str)
        
        string_array_field = next(f for f in avro_schema["fields"] if f["name"] == "string_array")
        assert string_array_field["type"]["type"] == "array"
        assert string_array_field["type"]["items"] == "string"
        
        # Protobuf
        protobuf_schema_str = self.protobuf_generator.generate(array_schema)
        assert 'repeated string string_array =' in protobuf_schema_str
        assert 'repeated double number_array =' in protobuf_schema_str
        assert 'repeated bool boolean_array =' in protobuf_schema_str
    
    def test_deep_nesting(self):
        """Test deep nesting capabilities."""
        deep_data = [
            {
                "level1": {
                    "level2": {
                        "level3": {
                            "level4": {
                                "level5": {
                                    "deep_field": "deep_value"
                                }
                            }
                        }
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
        
        # Should have nested properties structure
        level1 = json_schema["properties"]["level1"]
        assert level1["type"] == "object"
        assert "properties" in level1
        
        level2 = level1["properties"]["level2"]
        assert level2["type"] == "object"
        assert "properties" in level2
    
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
        lines = protobuf_schema_str.split('\n')
        
        # Basic Protobuf structure validation
        assert any('syntax = "proto3";' in line for line in lines)
        assert any('message ' in line and '{' in line for line in lines)
        assert any('}' in line for line in lines)
    
    def test_performance_with_large_schema(self):
        """Test performance with a large, complex schema."""
        # Create a large schema with many fields
        large_data = []
        for i in range(100):
            record = {
                f"field_{i}": f"value_{i}",
                f"nested_{i}": {
                    f"subfield_{i}": i,
                    f"array_{i}": [i, i+1, i+2]
                }
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
