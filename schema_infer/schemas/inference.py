"""
Schema inference engine for analyzing data and generating schema definitions
"""

import json
import re
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
        max_depth: int = 20,
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

    # Datetime patterns (ISO 8601 and common formats)
    _DATETIME_PATTERNS = [
        re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'),   # 2025-12-01T10:00:00
        re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'),    # 2025-12-01 10:00:00
        re.compile(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'),    # 12/01/2025 10:00:00
        re.compile(r'^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}'),    # 01-12-2025 10:00:00
    ]
    _DATE_PATTERNS = [
        re.compile(r'^\d{4}-\d{2}-\d{2}$'),                      # 2025-12-01
        re.compile(r'^\d{2}/\d{2}/\d{4}$'),                      # 12/01/2025
        re.compile(r'^\d{2}-\d{2}-\d{4}$'),                      # 01-12-2025
    ]

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
    
    def detect_discriminator(self, parsed_data: List[Dict[str, Any]]) -> Optional[str]:
        """
        Auto-detect a discriminator field that separates event types.

        Looks for top-level string fields with low cardinality that appear
        in most records. Prioritizes well-known field names.

        Args:
            parsed_data: List of parsed data dictionaries

        Returns:
            Field name of the best discriminator, or None if not found
        """
        if len(parsed_data) < 5:
            return None

        # Well-known discriminator field names (higher priority)
        priority_names = {"event_type", "type", "eventType", "__type", "action", "kind", "event", "record_type", "message_type", "category"}

        candidates = []
        total_records = len(parsed_data)

        # Collect all field names across all records
        all_field_names = set()
        for record in parsed_data:
            all_field_names.update(record.keys())

        for field_name in all_field_names:
            values = []
            present_count = 0
            for record in parsed_data:
                if field_name in record and record[field_name] is not None:
                    val = record[field_name]
                    if isinstance(val, str):
                        values.append(val)
                        present_count += 1

            if not values:
                continue

            presence_ratio = present_count / total_records
            unique_values = set(values)
            cardinality = len(unique_values)

            # Criteria: present in >90% of records, string type, 2-20 unique values,
            # and cardinality is much less than record count (not a high-cardinality ID field)
            if (presence_ratio >= 0.9
                    and 2 <= cardinality <= 20
                    and cardinality < total_records * 0.3):
                # Score: heavily prioritize known names, then presence ratio, then lower cardinality
                is_priority = field_name in priority_names or field_name.lower() in priority_names
                score = (100 if is_priority else 0) + (presence_ratio * 10) + (1.0 / cardinality)
                candidates.append((field_name, score, cardinality, unique_values))

        if not candidates:
            return None

        # Sort by score descending
        candidates.sort(key=lambda x: x[1], reverse=True)

        # Validate top candidates: a real discriminator should produce groups
        # with meaningfully different field sets (not just different values)
        for candidate in candidates:
            field_name = candidate[0]

            # Group records by this candidate's values
            groups: Dict[str, set] = {}
            for record in parsed_data:
                val = record.get(field_name)
                if val is not None and isinstance(val, str):
                    if val not in groups:
                        groups[val] = set()
                    groups[val].update(record.keys())

            if len(groups) < 2:
                continue

            # Check if groups have different field sets
            field_sets = list(groups.values())
            all_same = all(fs == field_sets[0] for fs in field_sets[1:])
            if all_same:
                # All groups have identical fields — not a real discriminator
                self.logger.debug(
                    f"Candidate '{field_name}' rejected: all groups have identical fields"
                )
                continue

            # This candidate produces groups with different schemas — valid discriminator
            self.logger.info(
                f"Auto-detected discriminator field '{field_name}' "
                f"with {candidate[2]} event types: {candidate[3]}"
            )
            return field_name

        # No candidate produced groups with different field sets
        return None

    def infer_multi_event_schemas(
        self,
        parsed_data: List[Dict[str, Any]],
        discriminator_field: str,
        schema_name: str,
    ) -> Dict[str, 'InferredSchema']:
        """
        Infer separate schemas per event type based on a discriminator field.

        Groups records by the discriminator field value, then infers a schema
        for each group independently.

        Args:
            parsed_data: List of parsed data dictionaries
            discriminator_field: Field name used to separate event types
            schema_name: Base name for the schemas

        Returns:
            Dict mapping event type value to its InferredSchema
        """
        # Group records by discriminator value
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for record in parsed_data:
            event_type = record.get(discriminator_field)
            if event_type is not None:
                groups[str(event_type)] = groups.get(str(event_type), [])
                groups[str(event_type)].append(record)
            else:
                groups["_unknown"] = groups.get("_unknown", [])
                groups["_unknown"].append(record)

        # Infer schema per group
        event_schemas = {}
        for event_type, records in groups.items():
            if len(records) < 2:
                continue
            sub_name = f"{schema_name}-{event_type}"
            event_schemas[event_type] = self.infer_schema(records, sub_name)
            self.logger.info(
                f"Inferred schema for event type '{event_type}': "
                f"{len(event_schemas[event_type].fields)} fields from {len(records)} records"
            )

        return event_schemas

    def analyze_fields(self, parsed_data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Analyze fields across all records.

        Public API for use by incremental inference.

        Args:
            parsed_data: List of parsed data dictionaries

        Returns:
            Dictionary mapping field names to their analysis
        """
        return self._analyze_fields(parsed_data)

    def create_schema_field(self, field_name: str, analysis: Dict[str, Any]) -> Optional[SchemaField]:
        """
        Create a schema field from analysis data.

        Public API for use by incremental inference.

        Args:
            field_name: Name of the field
            analysis: Field analysis data

        Returns:
            SchemaField or None if field should be excluded
        """
        return self._create_schema_field(field_name, analysis)

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
                analysis["types"][str(value_type)] += 1
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
        elif isinstance(value, (int, float)):
            return FieldType("float")
        elif isinstance(value, str):
            # Check for datetime patterns
            for pattern in self._DATETIME_PATTERNS:
                if pattern.match(value):
                    return FieldType("datetime")
            for pattern in self._DATE_PATTERNS:
                if pattern.match(value):
                    return FieldType("date")
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
        
        # All inferred fields are nullable -- a sample can never guarantee
        # a field won't be null in future data
        nullable = True
        
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
            
            # Parse array type wrapper
            is_array = False
            base_type_name = type_name
            if type_name.startswith("array<") and type_name.endswith(">"):
                is_array = True
                base_type_name = type_name[6:-1]  # Extract inner type

            # Detect enum types: string fields with limited distinct values
            if base_type_name == "string" and not is_array:
                string_examples = [ex for ex in analysis.get("examples", set()) if isinstance(ex, str)]
                non_null_count = total_count - null_count
                if (2 <= len(string_examples) <= 10
                        and non_null_count >= 3
                        and len(string_examples) < non_null_count * 0.5):
                    base_type_name = "enum"

            field_type = FieldType(base_type_name, nullable=nullable, array=is_array)
        
        # All inferred fields are optional -- a sample of messages can never
        # guarantee a field is truly required in all future data
        required = False
        nullable = True
        
        # Get examples
        examples = list(analysis["examples"])[:3]
        
        return SchemaField(
            name=field_name,
            field_type=field_type,
            required=required,
            examples=examples,
            description=f"Field {field_name} with type {field_type}"
        )
