"""
Schema inference engine for analyzing data and generating schema definitions
"""

import json
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Set, Union

from ..utils.logger import get_logger


class FieldType:
    """Represents a field type in the schema."""
    
    def __init__(self, name: str, nullable: bool = False, array: bool = False):
        """
        Initialize field type.
        
        Args:
            name: Type name (string, int, float, boolean, object, array, null)
            nullable: Whether the field can be null
            array: Whether the field is an array
        """
        self.name = name
        self.nullable = nullable
        self.array = array
    
    def __str__(self) -> str:
        """String representation of the type."""
        result = self.name
        if self.array:
            result = f"array<{result}>"
        if self.nullable:
            result = f"nullable<{result}>"
        return result
    
    def __eq__(self, other) -> bool:
        """Check equality with another FieldType."""
        if not isinstance(other, FieldType):
            return False
        return (self.name == other.name and 
                self.nullable == other.nullable and 
                self.array == other.array)
    
    def __hash__(self) -> int:
        """Hash for use in sets and dictionaries."""
        return hash((self.name, self.nullable, self.array))


class SchemaField:
    """Represents a field in the schema."""
    
    def __init__(
        self, 
        name: str, 
        field_type: FieldType, 
        required: bool = True,
        default_value: Optional[Any] = None,
        description: Optional[str] = None,
        examples: Optional[List[Any]] = None
    ):
        """
        Initialize schema field.
        
        Args:
            name: Field name
            field_type: Field type
            required: Whether the field is required
            default_value: Default value for the field
            description: Field description
            examples: Example values for the field
        """
        self.name = name
        self.field_type = field_type
        self.required = required
        self.default_value = default_value
        self.description = description
        self.examples = examples or []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert field to dictionary representation."""
        return {
            "name": self.name,
            "type": str(self.field_type),
            "required": self.required,
            "default_value": self.default_value,
            "description": self.description,
            "examples": self.examples,
        }


class InferredSchema:
    """Represents an inferred schema."""
    
    def __init__(
        self, 
        name: str, 
        fields: List[SchemaField],
        description: Optional[str] = None,
        namespace: Optional[str] = None
    ):
        """
        Initialize inferred schema.
        
        Args:
            name: Schema name
            fields: List of schema fields
            description: Schema description
            namespace: Schema namespace
        """
        self.name = name
        self.fields = fields
        self.description = description
        self.namespace = namespace
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary representation."""
        return {
            "name": self.name,
            "namespace": self.namespace,
            "description": self.description,
            "fields": [field.to_dict() for field in self.fields],
        }


class SchemaInferrer:
    """Infers schemas from parsed data."""
    
    def __init__(
        self,
        confidence_threshold: float = 0.8,
        max_depth: int = 10,
        array_handling: str = "union",
        null_handling: str = "optional"
    ):
        """
        Initialize schema inferrer.
        
        Args:
            confidence_threshold: Minimum confidence for field type inference
            max_depth: Maximum nesting depth for objects
            array_handling: How to handle arrays (union, first, all)
            null_handling: How to handle null values (optional, required, ignore)
        """
        self.confidence_threshold = confidence_threshold
        self.max_depth = max_depth
        self.array_handling = array_handling
        self.null_handling = null_handling
        self.logger = get_logger(__name__)
    
    def infer_schema(self, parsed_data: List[Dict[str, Any]], schema_name: str) -> InferredSchema:
        """
        Infer schema from parsed data.
        
        Args:
            parsed_data: List of parsed data dictionaries
            schema_name: Name for the schema
            
        Returns:
            Inferred schema
        """
        
        if not parsed_data:
            raise ValueError("No data provided for schema inference")
        
        self.logger.info(f"Inferring schema for {len(parsed_data)} records")
        
        # Analyze all records to determine field types
        field_analysis = self._analyze_fields(parsed_data)
        
        # Create schema fields
        fields = []
        for field_name, analysis in field_analysis.items():
            field = self._create_schema_field(field_name, analysis)
            if field:
                fields.append(field)
        
        # Sort fields by name for consistency
        fields.sort(key=lambda f: f.name)
        
        return InferredSchema(
            name=schema_name,
            fields=fields,
            description=f"Auto-generated schema for {schema_name}",
            namespace="com.schema-infer.schema.infer"
        )
    
    def _analyze_fields(self, parsed_data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Analyze fields across all records.
        
        Args:
            parsed_data: List of parsed data dictionaries
            
        Returns:
            Dictionary mapping field names to their analysis
        """
        
        field_analysis = defaultdict(lambda: {
            "types": Counter(),
            "values": [],
            "null_count": 0,
            "total_count": 0,
            "examples": set(),
        })
        
        for record in parsed_data:
            self._analyze_record(record, field_analysis, depth=0)
        
        return dict(field_analysis)
    
    def _analyze_record(
        self, 
        record: Dict[str, Any], 
        field_analysis: Dict[str, Dict[str, Any]], 
        depth: int,
        field_prefix: str = ""
    ) -> None:
        """
        Analyze a single record.
        
        Args:
            record: Record to analyze
            field_analysis: Field analysis accumulator
            depth: Current nesting depth
            field_prefix: Prefix for nested field names
        """
        
        if depth > self.max_depth:
            self.logger.warning(f"Maximum depth {self.max_depth} reached, truncating analysis")
            return
        
        for key, value in record.items():
            # Create full field name with prefix for nested fields
            full_field_name = f"{field_prefix}.{key}" if field_prefix else key
            analysis = field_analysis[full_field_name]
            analysis["total_count"] += 1
            
            if value is None:
                analysis["null_count"] += 1
                analysis["types"]["null"] += 1
            else:
                # Determine value type
                value_type = self._get_value_type(value, depth)
                analysis["types"][value_type.name] += 1
                analysis["values"].append(value)
                
                # Collect examples (limit to 5)
                if len(analysis["examples"]) < 5:
                    analysis["examples"].add(self._get_example_value(value))
                
                # Recursively analyze nested objects
                if isinstance(value, dict) and depth < self.max_depth:
                    self._analyze_record(value, field_analysis, depth + 1, full_field_name)
                elif isinstance(value, list) and depth < self.max_depth:
                    # Analyze array elements for nested objects
                    for item in value:
                        if isinstance(item, dict):
                            self._analyze_record(item, field_analysis, depth + 1, f"{full_field_name}[]")
                        elif isinstance(item, list):
                            # Handle nested arrays
                            for nested_item in item:
                                if isinstance(nested_item, dict):
                                    self._analyze_record(nested_item, field_analysis, depth + 1, f"{full_field_name}[][]")
    
    def _get_value_type(self, value: Any, depth: int) -> FieldType:
        """
        Get the type of a value.
        
        Args:
            value: Value to analyze
            depth: Current nesting depth
            
        Returns:
            FieldType representing the value's type
        """
        
        if isinstance(value, bool):
            return FieldType("boolean")
        elif isinstance(value, int):
            return FieldType("int")
        elif isinstance(value, float):
            return FieldType("float")
        elif isinstance(value, str):
            return FieldType("string")
        elif isinstance(value, list):
            if not value:
                return FieldType("array", array=True)
            
            # Analyze array elements
            element_types = [self._get_value_type(item, depth + 1) for item in value]
            
            if self.array_handling == "union":
                # Find the most common type
                type_counts = Counter(str(t) for t in element_types)
                most_common_type = type_counts.most_common(1)[0][0]
                return FieldType(most_common_type, array=True)
            elif self.array_handling == "first":
                # Use the type of the first element
                return FieldType(str(element_types[0]), array=True)
            else:  # all
                # Use union of all types
                unique_types = set(str(t) for t in element_types)
                if len(unique_types) == 1:
                    return FieldType(list(unique_types)[0], array=True)
                else:
                    return FieldType("union", array=True)
        
        elif isinstance(value, dict):
            if depth >= self.max_depth:
                return FieldType("string")  # Truncate deep objects
            return FieldType("object")
        
        else:
            return FieldType("string")  # Default fallback
    
    def _get_example_value(self, value: Any) -> Any:
        """
        Get an example value for documentation.
        
        Args:
            value: Value to convert to example
            
        Returns:
            Example value suitable for documentation
        """
        
        if isinstance(value, (str, int, float, bool)):
            return value
        elif isinstance(value, list):
            # Return first few elements as a string (since lists are unhashable)
            return str(value[:3])
        elif isinstance(value, dict):
            # Return first few key-value pairs as a string (since dicts are unhashable)
            return str(dict(list(value.items())[:3]))
        else:
            return str(value)
    
    def _create_schema_field(self, field_name: str, analysis: Dict[str, Any]) -> Optional[SchemaField]:
        """
        Create a schema field from analysis.
        
        Args:
            field_name: Name of the field
            analysis: Field analysis data
            
        Returns:
            SchemaField or None if field should be excluded
        """
        
        total_count = analysis["total_count"]
        null_count = analysis["null_count"]
        type_counts = analysis["types"]
        
        if total_count == 0:
            return None
        
        # Determine if field is nullable
        nullable = null_count > 0
        
        # Determine the primary type
        non_null_types = {k: v for k, v in type_counts.items() if k != "null"}
        
        if not non_null_types:
            # All values are null
            field_type = FieldType("string", nullable=True)
        else:
            # Find the most common non-null type
            most_common_type = max(non_null_types.items(), key=lambda x: x[1])
            type_name = most_common_type[0]
            
            # Calculate confidence
            confidence = most_common_type[1] / (total_count - null_count)
            
            if confidence < self.confidence_threshold:
                # Low confidence, use union type
                all_types = list(non_null_types.keys())
                if len(all_types) == 1:
                    type_name = all_types[0]
                else:
                    type_name = "union"
            
            field_type = FieldType(type_name, nullable=nullable)
        
        # Determine if field is required
        required = null_count == 0 or (null_count / total_count) < 0.1
        
        # Get examples
        examples = list(analysis["examples"])[:3]
        
        return SchemaField(
            name=field_name,
            field_type=field_type,
            required=required,
            examples=examples,
            description=f"Field {field_name} with type {field_type}"
        )
