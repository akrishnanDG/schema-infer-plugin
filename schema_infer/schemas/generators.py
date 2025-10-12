"""
Schema generators for different formats (Avro, Protobuf, JSON Schema)
"""

import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from .inference import InferredSchema, SchemaField, FieldType
from ..utils.logger import get_logger


class BaseSchemaGenerator(ABC):
    """Base class for schema generators."""
    
    def __init__(self):
        """Initialize generator."""
        self.logger = get_logger(__name__)
    
    @abstractmethod
    def generate(self, schema: InferredSchema) -> str:
        """
        Generate schema in the target format.
        
        Args:
            schema: Inferred schema
            
        Returns:
            Schema as string
        """
        pass
    
    @abstractmethod
    def get_file_extension(self) -> str:
        """
        Get the file extension for this schema format.
        
        Returns:
            File extension (without dot)
        """
        pass


class AvroGenerator(BaseSchemaGenerator):
    """Generator for Avro schemas."""
    
    def generate(self, schema: InferredSchema) -> str:
        """Generate Avro schema."""
        
        # Sanitize the record name for Avro compatibility
        sanitized_name = self._sanitize_avro_name(schema.name)
        
        avro_schema = {
            "type": "record",
            "name": sanitized_name,
            "namespace": schema.namespace or "com.schema.infer",
            "doc": schema.description or f"Auto-generated Avro schema for {schema.name}",
            "fields": []
        }
        
        # Build nested record structure from flat field names
        nested_fields = self._build_nested_avro_fields(schema.fields)
        avro_schema["fields"] = nested_fields
        
        return json.dumps(avro_schema, indent=2)
    
    def _build_nested_avro_fields(self, fields: List[SchemaField]) -> List[Dict[str, Any]]:
        """Build nested record structure from flat field names."""
        avro_fields = []
        
        # Group fields by their top-level name
        top_level_fields = {}
        nested_fields = {}
        
        for field in fields:
            if '.' in field.name:
                # This is a nested field
                parts = field.name.split('.')
                top_level = parts[0]
                nested_path = '.'.join(parts[1:])
                
                if top_level not in nested_fields:
                    nested_fields[top_level] = []
                nested_fields[top_level].append((nested_path, field))
            else:
                # This is a top-level field
                top_level_fields[field.name] = field
        
        # Add top-level fields
        for field_name, field in top_level_fields.items():
            avro_field = self._convert_field_to_avro(field)
            avro_fields.append(avro_field)
        
        # Add nested fields as record types
        for top_level, nested_list in nested_fields.items():
            if top_level not in top_level_fields:
                # Create record type for nested structure
                nested_record = {
                    "name": self._sanitize_avro_name(top_level),
                    "type": {
                        "type": "record",
                        "name": self._sanitize_avro_name(f"{top_level}_record"),
                        "fields": self._build_nested_avro_record_fields(nested_list)
                    },
                    "doc": f"Nested record for {top_level}"
                }
                avro_fields.append(nested_record)
            else:
                # Update existing top-level field to be a record type
                for i, field in enumerate(avro_fields):
                    if field["name"] == self._sanitize_avro_name(top_level):
                        field["type"] = {
                            "type": "record",
                            "name": self._sanitize_avro_name(f"{top_level}_record"),
                            "fields": self._build_nested_avro_record_fields(nested_list)
                        }
                        break
        
        return avro_fields
    
    def _build_nested_avro_record_fields(self, nested_fields: List[tuple]) -> List[Dict[str, Any]]:
        """Build nested record fields from field paths."""
        avro_fields = []
        
        # Group by first part of path
        groups = {}
        for path, field in nested_fields:
            parts = path.split('.')
            first_part = parts[0]
            remaining_path = '.'.join(parts[1:]) if len(parts) > 1 else None
            
            if first_part not in groups:
                groups[first_part] = []
            groups[first_part].append((remaining_path, field))
        
        for field_name, field_list in groups.items():
            if any(path is None for path, _ in field_list):
                # This is a leaf field
                leaf_field = next(field for path, field in field_list if path is None)
                avro_field = self._convert_field_to_avro(leaf_field)
                avro_fields.append(avro_field)
            else:
                # This is a nested record
                nested_record = {
                    "name": self._sanitize_avro_name(field_name),
                    "type": {
                        "type": "record",
                        "name": self._sanitize_avro_name(f"{field_name}_record"),
                        "fields": self._build_nested_avro_record_fields(field_list)
                    },
                    "doc": f"Nested record for {field_name}"
                }
                avro_fields.append(nested_record)
        
        return avro_fields
    
    def _convert_field_to_avro(self, field: SchemaField) -> Dict[str, Any]:
        """Convert a schema field to Avro format."""
        
        avro_field = {
            "name": self._sanitize_avro_name(field.name),
            "type": self._convert_type_to_avro(field.field_type),
            "doc": field.description or f"Field {field.name}"
        }
        
        if field.default_value is not None:
            avro_field["default"] = field.default_value
        elif not field.required:
            # Make field optional by wrapping in union with null
            current_type = avro_field["type"]
            if isinstance(current_type, str) and current_type != "null":
                avro_field["type"] = ["null", current_type]
                avro_field["default"] = None
        
        return avro_field
    
    def _convert_type_to_avro(self, field_type: FieldType) -> Any:
        """Convert FieldType to Avro type."""
        
        # Map our types to Avro types
        type_mapping = {
            "string": "string",
            "int": "int",
            "float": "double",
            "boolean": "boolean",
            "null": "null",
            "object": "record",
            "array": "array",
            "union": "union"
        }
        
        base_type = type_mapping.get(field_type.name, "string")
        
        if field_type.array:
            if base_type == "record":
                # For object arrays, we need to define the record type
                return {
                    "type": "array",
                    "items": "string"  # Simplified for now
                }
            else:
                return {
                    "type": "array",
                    "items": base_type
                }
        
        return base_type
    
    def _sanitize_avro_name(self, name: str) -> str:
        """
        Sanitize a name for Avro compatibility.
        
        Avro names must:
        - Start with [A-Za-z_]
        - Contain only [A-Za-z0-9_]
        - Not contain hyphens or other special characters
        
        Args:
            name: Original name
            
        Returns:
            Sanitized name safe for Avro
        """
        import re
        
        # Replace hyphens and other invalid characters with underscores
        sanitized = re.sub(r'[^A-Za-z0-9_]', '_', name)
        
        # Ensure it starts with a letter or underscore
        if sanitized and not re.match(r'^[A-Za-z_]', sanitized):
            sanitized = f"record_{sanitized}"
        
        # Ensure it's not empty
        if not sanitized:
            sanitized = "record"
        
        # Ensure it's not too long (Avro has practical limits)
        if len(sanitized) > 64:
            sanitized = sanitized[:64]
        
        return sanitized
    
    def get_file_extension(self) -> str:
        """Get Avro file extension."""
        return "avsc"


class ProtobufGenerator(BaseSchemaGenerator):
    """Generator for Protocol Buffers schemas."""
    
    def generate(self, schema: InferredSchema) -> str:
        """Generate Protobuf schema."""
        
        lines = []
        
        # Add header
        lines.append(f'syntax = "proto3";')
        lines.append("")
        
        # Add package
        if schema.namespace:
            package_name = schema.namespace.replace(".", "_").lower()
            lines.append(f'package {package_name};')
            lines.append("")
        
        # Add message definition
        lines.append(f'message {schema.name} {{')
        
        if schema.description:
            lines.append(f'  // {schema.description}')
        
        # Build nested message structure from flat field names
        nested_structure = self._build_nested_protobuf_structure(schema.fields)
        field_number = 1
        field_number = self._add_protobuf_fields(lines, nested_structure, field_number, indent="  ")
        
        lines.append("}")
        
        return "\n".join(lines)
    
    def _build_nested_protobuf_structure(self, fields: List[SchemaField]) -> Dict[str, Any]:
        """Build nested message structure from flat field names."""
        structure = {
            "top_level_fields": {},
            "nested_messages": {}
        }
        
        for field in fields:
            if '.' in field.name:
                # This is a nested field
                parts = field.name.split('.')
                top_level = parts[0]
                nested_path = '.'.join(parts[1:])
                
                if top_level not in structure["nested_messages"]:
                    structure["nested_messages"][top_level] = []
                structure["nested_messages"][top_level].append((nested_path, field))
            else:
                # This is a top-level field
                structure["top_level_fields"][field.name] = field
        
        return structure
    
    def _add_protobuf_fields(self, lines: List[str], structure: Dict[str, Any], field_number: int, indent: str) -> int:
        """Add Protobuf fields to lines list."""
        
        # Add top-level fields
        for field_name, field in structure["top_level_fields"].items():
            protobuf_field = self._convert_field_to_protobuf(field, field_number)
            lines.append(f'{indent}{protobuf_field}')
            field_number += 1
        
        # Add nested messages (simplified to avoid recursion)
        for top_level, nested_list in structure["nested_messages"].items():
            if top_level not in structure["top_level_fields"]:
                # Create nested message
                message_name = self._sanitize_protobuf_name(f"{top_level}_message")
                lines.append(f'{indent}{message_name} {top_level} = {field_number}; // Nested message for {top_level}')
                field_number += 1
                
                # Add nested message definition
                lines.append(f'{indent}message {message_name} {{')
                
                # Add nested fields directly (avoiding recursion for now)
                for nested_path, field in nested_list:
                    if '.' not in nested_path:  # Only add leaf fields
                        protobuf_field = self._convert_field_to_protobuf(field, field_number)
                        lines.append(f'{indent}  {protobuf_field}')
                        field_number += 1
                
                lines.append(f'{indent}}}')
        
        return field_number
    
    def _sanitize_protobuf_name(self, name: str) -> str:
        """Sanitize a name for Protobuf compatibility."""
        import re
        
        # Replace invalid characters with underscores
        sanitized = re.sub(r'[^A-Za-z0-9_]', '_', name)
        
        # Ensure it starts with a letter or underscore
        if sanitized and not re.match(r'^[A-Za-z_]', sanitized):
            sanitized = f"message_{sanitized}"
        
        # Ensure it's not empty
        if not sanitized:
            sanitized = "message"
        
        return sanitized
    
    def _convert_field_to_protobuf(self, field: SchemaField, field_number: int) -> str:
        """Convert a schema field to Protobuf format."""
        
        protobuf_type = self._convert_type_to_protobuf(field.field_type)
        
        # Protobuf field format: type name = field_number;
        field_name = field.name.lower().replace(" ", "_")
        
        # Add comment if available
        comment = ""
        if field.description:
            comment = f" // {field.description}"
        
        return f"{protobuf_type} {field_name} = {field_number};{comment}"
    
    def _convert_type_to_protobuf(self, field_type: FieldType) -> str:
        """Convert FieldType to Protobuf type."""
        
        # Map our types to Protobuf types
        type_mapping = {
            "string": "string",
            "int": "int32",
            "float": "double",
            "boolean": "bool",
            "null": "string",  # Protobuf doesn't have null type
            "object": "string",  # Simplified for now
            "array": "repeated",
            "union": "string"  # Simplified for now
        }
        
        base_type = type_mapping.get(field_type.name, "string")
        
        if field_type.array and base_type != "repeated":
            return f"repeated {base_type}"
        
        return base_type
    
    def get_file_extension(self) -> str:
        """Get Protobuf file extension."""
        return "proto"


class JSONSchemaGenerator(BaseSchemaGenerator):
    """Generator for JSON Schema."""
    
    def generate(self, schema: InferredSchema) -> str:
        """Generate JSON Schema."""
        
        json_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": schema.name,
            "description": schema.description or f"Auto-generated JSON Schema for {schema.name}",
            "type": "object",
            "properties": {},
            "required": []
        }
        
        # Add namespace as custom property
        if schema.namespace:
            json_schema["$id"] = f"{schema.namespace}/{schema.name}"
        
        # Build nested object structure from flat field names
        nested_properties = self._build_nested_properties(schema.fields)
        json_schema["properties"] = nested_properties["properties"]
        json_schema["required"] = nested_properties["required"]
        
        return json.dumps(json_schema, indent=2)
    
    def get_file_extension(self) -> str:
        """Get the file extension for JSON Schema."""
        return "json"
    
    def _build_nested_properties(self, fields: List[SchemaField]) -> Dict[str, Any]:
        """Build nested object structure from flat field names."""
        properties = {}
        required = []
        
        # Group fields by their top-level name
        top_level_fields = {}
        nested_fields = {}
        
        for field in fields:
            if '.' in field.name:
                # This is a nested field
                parts = field.name.split('.')
                top_level = parts[0]
                nested_path = '.'.join(parts[1:])
                
                if top_level not in nested_fields:
                    nested_fields[top_level] = []
                nested_fields[top_level].append((nested_path, field))
            else:
                # This is a top-level field
                top_level_fields[field.name] = field
        
        # Add top-level fields
        for field_name, field in top_level_fields.items():
            property_schema = self._convert_field_to_json_schema(field)
            properties[field_name] = property_schema
            if field.required:
                required.append(field_name)
        
        # Add nested fields
        for top_level, nested_list in nested_fields.items():
            if top_level not in properties:
                # Create object type for nested structure
                properties[top_level] = {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            
            # Build nested structure
            nested_props = self._build_nested_structure(nested_list)
            properties[top_level]["properties"] = nested_props["properties"]
            properties[top_level]["required"] = nested_props["required"]
        
        return {
            "properties": properties,
            "required": required
        }
    
    def _build_nested_structure(self, nested_fields: List[tuple]) -> Dict[str, Any]:
        """Build nested structure from field paths."""
        properties = {}
        required = []
        
        # Group by first part of path
        groups = {}
        for path, field in nested_fields:
            parts = path.split('.')
            first_part = parts[0]
            remaining_path = '.'.join(parts[1:]) if len(parts) > 1 else None
            
            if first_part not in groups:
                groups[first_part] = []
            groups[first_part].append((remaining_path, field))
        
        for field_name, field_list in groups.items():
            if any(path is None for path, _ in field_list):
                # This is a leaf field
                leaf_field = next(field for path, field in field_list if path is None)
                property_schema = self._convert_field_to_json_schema(leaf_field)
                properties[field_name] = property_schema
                if leaf_field.required:
                    required.append(field_name)
            else:
                # This is a nested object
                properties[field_name] = {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
                
                # Recursively build nested structure
                nested_props = self._build_nested_structure(field_list)
                properties[field_name]["properties"] = nested_props["properties"]
                properties[field_name]["required"] = nested_props["required"]
        
        return {
            "properties": properties,
            "required": required
        }
    
    def _convert_field_to_json_schema(self, field: SchemaField) -> Dict[str, Any]:
        """Convert a schema field to JSON Schema format."""
        
        property_schema = self._convert_type_to_json_schema(field.field_type)
        
        # Add description
        if field.description:
            property_schema["description"] = field.description
        
        # Add default value
        if field.default_value is not None:
            property_schema["default"] = field.default_value
        
        # Add examples
        if field.examples:
            property_schema["examples"] = field.examples
        
        return property_schema
    
    def _convert_type_to_json_schema(self, field_type: FieldType) -> Dict[str, Any]:
        """Convert FieldType to JSON Schema type."""
        
        # Map our types to JSON Schema types
        type_mapping = {
            "string": "string",
            "int": "integer",
            "float": "number",
            "boolean": "boolean",
            "null": "null",
            "object": "object",
            "array": "array",
            "union": "string"  # Simplified for now
        }
        
        base_type = type_mapping.get(field_type.name, "string")
        
        if field_type.array:
            return {
                "type": "array",
                "items": {"type": base_type}
            }
        
        if field_type.nullable and base_type != "null":
            return {
                "type": [base_type, "null"]
            }
        
        return {"type": base_type}


class SchemaGeneratorFactory:
    """Factory for creating schema generators."""
    
    @staticmethod
    def create_generator(format_name: str) -> BaseSchemaGenerator:
        """
        Create a schema generator for the specified format.
        
        Args:
            format_name: Name of the schema format
            
        Returns:
            Appropriate generator instance
        """
        
        generators = {
            "avro": AvroGenerator,
            "protobuf": ProtobufGenerator,
            "json-schema": JSONSchemaGenerator,
        }
        
        if format_name not in generators:
            raise ValueError(f"Unsupported schema format: {format_name}")
        
        generator_class = generators[format_name]
        return generator_class()
