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
        # Pass schema name as prefix to avoid Avro name collisions across topics
        self._record_prefix = sanitized_name
        self._used_record_names = set()
        nested_fields = self._build_nested_avro_fields(schema.fields)
        avro_schema["fields"] = nested_fields
        
        return json.dumps(avro_schema, indent=2)
    
    def _build_nested_avro_fields(self, fields: List[SchemaField]) -> List[Dict[str, Any]]:
        """Build nested record structure from flat field names."""
        avro_fields = []

        # Group fields
        top_level_fields = {}
        nested_fields = {}
        array_child_fields = {}

        for field in fields:
            if '[]' in field.name:
                array_parent = field.name.split('[]')[0]
                child_path = field.name.split('[]', 1)[1]
                if child_path.startswith('.'):
                    child_path = child_path[1:]
                if array_parent not in array_child_fields:
                    array_child_fields[array_parent] = []
                if child_path:
                    array_child_fields[array_parent].append((child_path, field))
            elif '.' in field.name:
                parts = field.name.split('.')
                top_level = parts[0]
                nested_path = '.'.join(parts[1:])
                if top_level not in nested_fields:
                    nested_fields[top_level] = []
                nested_fields[top_level].append((nested_path, field))
            else:
                top_level_fields[field.name] = field

        # Add top-level fields
        for field_name, field in top_level_fields.items():
            if field_name in array_child_fields and array_child_fields[field_name]:
                # Array with known item schema
                item_fields = self._build_nested_avro_record_fields(array_child_fields[field_name])
                avro_field = {
                    "name": self._sanitize_avro_name(field_name),
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": self._unique_record_name(f"{self._record_prefix}_{field_name}_item"),
                            "fields": item_fields
                        }
                    },
                    "doc": field.description or f"Field {field_name}"
                }
            else:
                avro_field = self._convert_field_to_avro(field)
            avro_fields.append(avro_field)

        # Add nested fields as record types
        for top_level, nested_list in nested_fields.items():
            if top_level not in top_level_fields:
                nested_record = {
                    "name": self._sanitize_avro_name(top_level),
                    "type": {
                        "type": "record",
                        "name": self._unique_record_name(f"{self._record_prefix}_{top_level}_record"),
                        "fields": self._build_nested_avro_record_fields(nested_list, top_level)
                    },
                    "doc": f"Nested record for {top_level}"
                }
                avro_fields.append(nested_record)
            else:
                for i, f in enumerate(avro_fields):
                    if f["name"] == self._sanitize_avro_name(top_level):
                        f["type"] = {
                            "type": "record",
                            "name": self._unique_record_name(f"{self._record_prefix}_{top_level}_record"),
                            "fields": self._build_nested_avro_record_fields(nested_list, top_level)
                        }
                        break

        # Add remaining array child groups without parent
        for array_parent, children in array_child_fields.items():
            if array_parent not in top_level_fields and children:
                item_fields = self._build_nested_avro_record_fields(children, array_parent)
                avro_fields.append({
                    "name": self._sanitize_avro_name(array_parent),
                    "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": self._unique_record_name(f"{self._record_prefix}_{array_parent}_item"),
                            "fields": item_fields
                        }
                    },
                    "doc": f"Array field {array_parent}"
                })

        return avro_fields
    
    def _build_nested_avro_record_fields(self, nested_fields: List[tuple], parent_path: str = "") -> List[Dict[str, Any]]:
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
            leaf_entries = [(path, field) for path, field in field_list if path is None]
            nested_entries = [(path, field) for path, field in field_list if path is not None]
            # Build a unique path for this level to avoid Avro name collisions
            full_path = f"{parent_path}_{field_name}" if parent_path else field_name

            if leaf_entries and not nested_entries:
                leaf_field = leaf_entries[0][1]
                avro_field = self._convert_field_to_avro(leaf_field)
                # Use the local field name, not the full dotted path
                avro_field["name"] = self._sanitize_avro_name(field_name)
                avro_fields.append(avro_field)
            elif nested_entries:
                nested_record = {
                    "name": self._sanitize_avro_name(field_name),
                    "type": {
                        "type": "record",
                        "name": self._unique_record_name(f"{self._record_prefix}_{full_path}_record"),
                        "fields": self._build_nested_avro_record_fields(nested_entries, full_path)
                    },
                    "doc": f"Nested record for {field_name}"
                }
                avro_fields.append(nested_record)

        return avro_fields
    
    def _convert_field_to_avro(self, field: SchemaField) -> Dict[str, Any]:
        """Convert a schema field to Avro format."""

        avro_type = self._convert_type_to_avro(field.field_type)

        avro_field = {
            "name": self._sanitize_avro_name(field.name),
            "type": avro_type,
            "doc": field.description or f"Field {field.name}"
        }

        if field.default_value is not None:
            avro_field["default"] = field.default_value
        elif not field.required:
            # Make field optional by wrapping in union with null
            if isinstance(avro_type, str) and avro_type != "null":
                avro_field["type"] = ["null", avro_type]
                avro_field["default"] = None
            elif isinstance(avro_type, dict):
                avro_field["type"] = ["null", avro_type]
                avro_field["default"] = None
        else:
            # Add type-appropriate defaults for schema evolution compatibility
            avro_field["default"] = self._get_avro_default(avro_type)

        return avro_field

    def _get_avro_default(self, avro_type: Any) -> Any:
        """Get a sensible default value for an Avro type."""
        if isinstance(avro_type, dict):
            inner_type = avro_type.get("type")
            if inner_type == "array":
                return []
            return {}
        defaults = {
            "string": "",
            "int": 0,
            "long": 0,
            "float": 0.0,
            "double": 0.0,
            "boolean": False,
            "null": None,
        }
        return defaults.get(avro_type, "")
    
    def _convert_type_to_avro(self, field_type: FieldType) -> Any:
        """Convert FieldType to Avro type."""

        # Map our types to Avro types
        # Note: "object", "array", and "union" are not valid standalone Avro types,
        # so they fall back to "string" to produce a valid schema.
        type_mapping = {
            "string": "string",
            "int": "int",
            "float": "double",
            "boolean": "boolean",
            "null": "null",
            "datetime": "string",  # Avro uses string with logicalType
            "date": "string",
            "enum": "string",      # Simplified enum as string
        }

        base_type = type_mapping.get(field_type.name, "string")

        if field_type.array:
            return {
                "type": "array",
                "items": base_type
            }

        return base_type
    
    def _unique_record_name(self, base_name: str) -> str:
        """Generate a unique Avro record name, appending a suffix if needed."""
        name = self._sanitize_avro_name(base_name)
        if name not in self._used_record_names:
            self._used_record_names.add(name)
            return name
        counter = 2
        while f"{name}_{counter}" in self._used_record_names:
            counter += 1
        unique = f"{name}_{counter}"
        self._used_record_names.add(unique)
        return unique

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
            package_name = self._sanitize_protobuf_name(schema.namespace.replace(".", "_").lower())
            lines.append(f'package {package_name};')
            lines.append("")

        # Add message definition
        message_name = self._sanitize_protobuf_name(schema.name)
        lines.append(f'message {message_name} {{')
        
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

        # Add top-level fields (skip those that have nested children)
        for field_name, field in structure["top_level_fields"].items():
            if field_name not in structure["nested_messages"]:
                protobuf_field = self._convert_field_to_protobuf(field, field_number)
                lines.append(f'{indent}{protobuf_field}')
                field_number += 1

        # Add nested messages (recursive)
        for top_level, nested_list in structure["nested_messages"].items():
            message_name = self._sanitize_protobuf_name(f"{top_level}_message")
            field_name = self._sanitize_protobuf_name(top_level)
            lines.append(f'{indent}{message_name} {field_name} = {field_number};')
            field_number += 1

            # Build sub-structure recursively
            lines.append(f'{indent}message {message_name} {{')
            sub_structure = {"top_level_fields": {}, "nested_messages": {}}
            for nested_path, field in nested_list:
                if '.' in nested_path:
                    parts = nested_path.split('.', 1)
                    sub_top = parts[0]
                    sub_rest = parts[1]
                    if sub_top not in sub_structure["nested_messages"]:
                        sub_structure["nested_messages"][sub_top] = []
                    sub_structure["nested_messages"][sub_top].append((sub_rest, field))
                else:
                    sub_structure["top_level_fields"][nested_path] = field
            field_number = self._add_protobuf_fields(lines, sub_structure, field_number, indent + "  ")
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
        # Use the last part of dotted name to get the local field name
        local_name = field.name.rsplit('.', 1)[-1] if '.' in field.name else field.name
        field_name = self._sanitize_protobuf_name(local_name)
        
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
            "null": "string",
            "object": "string",
            "array": "repeated",
            "union": "string",
            "datetime": "string",  # Protobuf uses string for timestamps
            "date": "string",
            "enum": "string",
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
            "required": [],
            "additionalProperties": False
        }
        
        # Add namespace as custom property
        if schema.namespace:
            json_schema["$id"] = f"{schema.namespace}/{schema.name}"
        
        # Build nested object structure from flat field names
        nested_properties = self._build_nested_properties(schema.fields)
        json_schema["properties"] = nested_properties["properties"]
        json_schema["required"] = nested_properties["required"]
        
        return json.dumps(json_schema, indent=2)
    
    def generate_multi_event(
        self,
        topic_name: str,
        event_schemas: Dict[str, 'InferredSchema'],
        discriminator_field: str,
    ) -> Dict[str, str]:
        """
        Generate JSON Schemas for multi-event topics.

        Produces individual sub-schemas for each event type and a main
        envelope schema using oneOf with $ref.

        Args:
            topic_name: Base topic name
            event_schemas: Dict mapping event type to InferredSchema
            discriminator_field: Field name used as discriminator

        Returns:
            Dict with keys:
              - "{topic_name}" -> main oneOf schema JSON string
              - "{topic_name}.{event_type}" -> sub-schema JSON string per type
        """
        result = {}

        # Generate individual sub-schemas
        refs = []
        for event_type, schema in sorted(event_schemas.items()):
            sub_schema_json = self.generate(schema)
            subject = f"{topic_name}-{event_type}"
            result[f"{topic_name}.{event_type}"] = sub_schema_json
            refs.append({"$ref": subject})

        # Generate main envelope schema with oneOf
        main_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": topic_name,
            "description": f"Multi-event schema for {topic_name} (discriminator: {discriminator_field})",
            "oneOf": refs,
        }
        result[topic_name] = json.dumps(main_schema, indent=2)

        return result

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
        array_child_fields = {}  # Fields like items[].sku

        for field in fields:
            if '[]' in field.name:
                # Array child field - group by array parent
                array_parent = field.name.split('[]')[0]
                child_path = field.name.split('[]', 1)[1]
                if child_path.startswith('.'):
                    child_path = child_path[1:]
                if array_parent not in array_child_fields:
                    array_child_fields[array_parent] = []
                if child_path:  # Only add if there's a child path
                    array_child_fields[array_parent].append((child_path, field))
            elif '.' in field.name:
                # Nested object field
                parts = field.name.split('.')
                top_level = parts[0]
                nested_path = '.'.join(parts[1:])
                if top_level not in nested_fields:
                    nested_fields[top_level] = []
                nested_fields[top_level].append((nested_path, field))
            else:
                top_level_fields[field.name] = field

        # Add top-level fields
        for field_name, field in top_level_fields.items():
            property_schema = self._convert_field_to_json_schema(field)

            # If this field has array children, enhance the items definition
            if field_name in array_child_fields and array_child_fields[field_name]:
                child_props = self._build_nested_structure(
                    [(p, f) for p, f in array_child_fields[field_name]]
                )
                property_schema = {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": child_props["properties"],
                        "required": child_props["required"]
                    }
                }
                if field.description:
                    property_schema["description"] = field.description

            properties[field_name] = property_schema
            if field.required:
                required.append(field_name)

        # Add nested fields
        for top_level, nested_list in nested_fields.items():
            if top_level not in properties:
                properties[top_level] = {
                    "type": "object",
                    "properties": {},
                    "required": []
                }

            nested_props = self._build_nested_structure(nested_list)
            properties[top_level]["properties"] = nested_props["properties"]
            properties[top_level]["required"] = nested_props["required"]

        # Add remaining array child groups that don't have a parent top-level field
        for array_parent, children in array_child_fields.items():
            if array_parent not in properties and children:
                child_props = self._build_nested_structure(children)
                properties[array_parent] = {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": child_props["properties"],
                        "required": child_props["required"]
                    }
                }

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
            leaf_entries = [(path, field) for path, field in field_list if path is None]
            nested_entries = [(path, field) for path, field in field_list if path is not None]

            if leaf_entries and not nested_entries:
                # Pure leaf field
                leaf_field = leaf_entries[0][1]
                property_schema = self._convert_field_to_json_schema(leaf_field)
                properties[field_name] = property_schema
                if leaf_field.required:
                    required.append(field_name)
            elif nested_entries:
                # Has children - create nested object (ignore leaf if exists)
                properties[field_name] = {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
                nested_props = self._build_nested_structure(nested_entries)
                properties[field_name]["properties"] = nested_props["properties"]
                properties[field_name]["required"] = nested_props["required"]
        
        return {
            "properties": properties,
            "required": required
        }
    
    def _convert_field_to_json_schema(self, field: SchemaField) -> Dict[str, Any]:
        """Convert a schema field to JSON Schema format."""
        
        property_schema = self._convert_type_to_json_schema(field.field_type)

        # Add format for datetime/date types
        if field.field_type.name == "datetime":
            property_schema["format"] = "date-time"
        elif field.field_type.name == "date":
            property_schema["format"] = "date"

        # Add description
        if field.description:
            property_schema["description"] = field.description
        
        # Add default value
        if field.default_value is not None:
            property_schema["default"] = field.default_value
        else:
            # Add type-appropriate default
            json_type = property_schema.get("type")
            if isinstance(json_type, str):
                defaults = {"string": "", "integer": 0, "number": 0.0, "boolean": False, "array": [], "object": {}}
                if json_type in defaults:
                    property_schema["default"] = defaults[json_type]

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
            "union": "string",
            "datetime": "string",
            "date": "string",
            "enum": "string",
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
