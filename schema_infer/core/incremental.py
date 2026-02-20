"""
Incremental schema state management and change detection for live consumer mode.
"""

import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..schemas.inference import (
    FieldType,
    InferredSchema,
    SchemaField,
    SchemaInferrer as SchemaAnalyzer,
)
from ..config import Config
from ..utils.logger import get_logger


@dataclass
class TypeChange:
    """Represents a type change for a single field."""

    field_name: str
    old_type: str
    new_type: str


@dataclass
class SchemaChangeReport:
    """Report of structural differences between two schemas."""

    added_fields: List[str] = field(default_factory=list)
    removed_fields: List[str] = field(default_factory=list)
    type_changes: List[TypeChange] = field(default_factory=list)
    nullability_changes: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(
            self.added_fields
            or self.removed_fields
            or self.type_changes
            or self.nullability_changes
        )

    def summary(self) -> str:
        """Human-readable summary of changes."""
        lines = []
        for f in self.added_fields:
            lines.append(f"  + {f}")
        for f in self.removed_fields:
            lines.append(f"  - {f}")
        for tc in self.type_changes:
            lines.append(f"  ~ {tc.field_name}: {tc.old_type} -> {tc.new_type}")
        for f in self.nullability_changes:
            lines.append(f"  ? {f} (nullability changed)")
        return "\n".join(lines)


class IncrementalSchemaState:
    """
    Maintains running field analysis state for incremental schema inference.

    The field_analysis dict uses the same structure as SchemaAnalyzer._analyze_fields():
      {
        "field_name": {
          "types": Counter({"string": 42, "int": 3}),
          "values": [],
          "null_count": 5,
          "total_count": 50,
          "examples": set(),
        }
      }

    On each batch, new records are analyzed and merged into the running state.
    After merging, type determination is re-run to produce the current schema.
    """

    MAX_VALUES = 50
    MAX_EXAMPLES = 10

    def __init__(self, topic_name: str, config: Config):
        self.topic_name = topic_name
        self.config = config
        self.field_analysis: Dict[str, Dict[str, Any]] = {}
        self.total_records_processed: int = 0
        self.last_schema: Optional[InferredSchema] = None
        self.detected_format: Optional[str] = None
        self.last_updated: float = time.time()
        self.dirty: bool = False
        self.logger = get_logger(__name__)

        self.schema_analyzer = SchemaAnalyzer(
            confidence_threshold=config.inference.confidence_threshold,
            max_depth=config.inference.max_depth,
            array_handling=config.inference.array_handling,
            null_handling=config.inference.null_handling,
        )

    @classmethod
    def seed_from_json_schema(
        cls,
        topic_name: str,
        schema_json: str,
        config: Config,
    ) -> "IncrementalSchemaState":
        """
        Create an IncrementalSchemaState seeded from an existing JSON Schema.

        This allows live mode to build on schemas previously registered via
        infer mode, preserving all known fields.

        Args:
            topic_name: Topic name
            schema_json: Existing JSON Schema string from Schema Registry
            config: Configuration object

        Returns:
            IncrementalSchemaState with field_analysis populated from the schema
        """
        import json

        state = cls(topic_name, config)

        try:
            schema = json.loads(schema_json)
            properties = schema.get("properties", {})

            for field_name, field_def in properties.items():
                field_type = field_def.get("type", "string")
                # Handle union types like ["string", "null"]
                if isinstance(field_type, list):
                    non_null = [t for t in field_type if t != "null"]
                    field_type = non_null[0] if non_null else "string"

                state.field_analysis[field_name] = {
                    "types": Counter({field_type: 1}),
                    "values": [],
                    "null_count": 1 if "null" in (field_def.get("type", []) if isinstance(field_def.get("type"), list) else []) else 0,
                    "total_count": 1,
                    "examples": set(),
                }

            state.total_records_processed = 1  # Mark as having some data
            state.dirty = True
            state.logger.info(
                f"Seeded state for '{topic_name}' from existing schema "
                f"({len(properties)} fields)"
            )
        except Exception as e:
            state.logger.warning(f"Failed to seed state from schema: {e}")

        return state

    def merge_batch(self, parsed_records: List[Dict[str, Any]]) -> InferredSchema:
        """
        Merge a new batch of parsed records into the running state and
        produce an updated schema.

        Args:
            parsed_records: List of parsed data dictionaries from the new batch.

        Returns:
            Updated InferredSchema reflecting all data seen so far.
        """
        if not parsed_records:
            if self.last_schema is not None:
                return self.last_schema
            raise ValueError("No records to merge and no existing schema")

        # Analyze the new batch using the existing analyzer
        batch_analysis = self.schema_analyzer.analyze_fields(parsed_records)

        # Merge into running state
        self._merge_field_analysis(batch_analysis)
        self.total_records_processed += len(parsed_records)
        self.last_updated = time.time()
        self.dirty = True

        # Re-derive schema from merged state
        # All fields are marked optional and nullable in live mode to
        # ensure backward-compatible schema evolution. In streaming data
        # you can never guarantee a field will always be present --
        # producers change, bugs happen, different versions coexist.
        fields = []
        for field_name, analysis in self.field_analysis.items():
            schema_field = self.schema_analyzer.create_schema_field(field_name, analysis)
            if schema_field:
                schema_field.required = False
                if not schema_field.field_type.nullable:
                    schema_field.field_type = FieldType(
                        schema_field.field_type.name,
                        nullable=True,
                        array=schema_field.field_type.array,
                    )
                fields.append(schema_field)

        fields.sort(key=lambda f: f.name)

        new_schema = InferredSchema(
            name=self.topic_name,
            fields=fields,
            description=f"Auto-generated schema for {self.topic_name}",
            namespace="com.schema-infer.schema.infer",
        )

        return new_schema

    def detect_changes(self, new_schema: InferredSchema) -> Optional[SchemaChangeReport]:
        """
        Compare a new schema against the last known schema.

        Returns None if no structural changes, otherwise a SchemaChangeReport.
        """
        if self.last_schema is None:
            # First schema -- report all fields as added
            report = SchemaChangeReport(
                added_fields=[f.name for f in new_schema.fields]
            )
            self.last_schema = new_schema
            return report

        old_fields = {f.name: f for f in self.last_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}

        old_names = set(old_fields.keys())
        new_names = set(new_fields.keys())

        added = sorted(new_names - old_names)
        removed = sorted(old_names - new_names)

        type_changes = []
        nullability_changes = []

        for name in sorted(old_names & new_names):
            old_f = old_fields[name]
            new_f = new_fields[name]

            if old_f.field_type != new_f.field_type:
                # Check if only nullability changed
                if (
                    old_f.field_type.name == new_f.field_type.name
                    and old_f.field_type.array == new_f.field_type.array
                    and old_f.field_type.nullable != new_f.field_type.nullable
                ):
                    nullability_changes.append(name)
                else:
                    type_changes.append(
                        TypeChange(
                            field_name=name,
                            old_type=str(old_f.field_type),
                            new_type=str(new_f.field_type),
                        )
                    )

        report = SchemaChangeReport(added, removed, type_changes, nullability_changes)

        if report.has_changes:
            self.last_schema = new_schema
            return report

        return None

    def _merge_field_analysis(self, batch_analysis: Dict[str, Dict[str, Any]]) -> None:
        """Merge batch field analysis into the running state."""
        for field_name, batch_data in batch_analysis.items():
            if field_name not in self.field_analysis:
                # New field -- copy the analysis, capping values/examples
                self.field_analysis[field_name] = {
                    "types": Counter(batch_data["types"]),
                    "values": list(batch_data["values"][-self.MAX_VALUES :]),
                    "null_count": batch_data["null_count"],
                    "total_count": batch_data["total_count"],
                    "examples": set(list(batch_data["examples"])[: self.MAX_EXAMPLES]),
                }
            else:
                existing = self.field_analysis[field_name]
                existing["types"] += Counter(batch_data["types"])
                existing["values"].extend(batch_data["values"])
                existing["values"] = existing["values"][-self.MAX_VALUES :]
                existing["null_count"] += batch_data["null_count"]
                existing["total_count"] += batch_data["total_count"]
                for ex in batch_data["examples"]:
                    if len(existing["examples"]) < self.MAX_EXAMPLES:
                        existing["examples"].add(ex)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize state to a dictionary for persistence."""
        serialized_analysis = {}
        for field_name, analysis in self.field_analysis.items():
            serialized_analysis[field_name] = {
                "types": dict(analysis["types"]),
                "null_count": analysis["null_count"],
                "total_count": analysis["total_count"],
                "examples": [
                    _make_json_serializable(ex) for ex in analysis["examples"]
                ],
                # values list is NOT persisted to save space
            }

        last_schema_dict = None
        if self.last_schema is not None:
            last_schema_dict = self.last_schema.to_dict()

        return {
            "version": 1,
            "topic_name": self.topic_name,
            "total_records_processed": self.total_records_processed,
            "detected_format": self.detected_format,
            "last_updated": self.last_updated,
            "field_analysis": serialized_analysis,
            "last_schema": last_schema_dict,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], config: Config) -> "IncrementalSchemaState":
        """Deserialize state from a dictionary."""
        state = cls(topic_name=data["topic_name"], config=config)
        state.total_records_processed = data.get("total_records_processed", 0)
        state.detected_format = data.get("detected_format")
        state.last_updated = data.get("last_updated", time.time())
        state.dirty = False

        # Reconstruct field_analysis
        for field_name, analysis_data in data.get("field_analysis", {}).items():
            state.field_analysis[field_name] = {
                "types": Counter(analysis_data.get("types", {})),
                "values": [],  # values are not persisted
                "null_count": analysis_data.get("null_count", 0),
                "total_count": analysis_data.get("total_count", 0),
                "examples": set(analysis_data.get("examples", [])),
            }

        # Reconstruct last_schema if present
        last_schema_data = data.get("last_schema")
        if last_schema_data:
            fields = []
            for field_dict in last_schema_data.get("fields", []):
                type_str = field_dict.get("type", "string")
                field_type = _parse_field_type_str(type_str)
                fields.append(
                    SchemaField(
                        name=field_dict.get("name", ""),
                        field_type=field_type,
                        required=field_dict.get("required", True),
                        default_value=field_dict.get("default_value"),
                        description=field_dict.get("description"),
                        examples=field_dict.get("examples", []),
                    )
                )
            state.last_schema = InferredSchema(
                name=last_schema_data.get("name", state.topic_name),
                fields=fields,
                description=last_schema_data.get("description"),
                namespace=last_schema_data.get("namespace"),
            )

        return state


def _parse_field_type_str(type_str: str) -> FieldType:
    """Parse a field type string like 'nullable<array<string>>' into a FieldType."""
    nullable = type_str.startswith("nullable<")
    if nullable:
        type_str = type_str[9:-1]  # strip "nullable<" and ">"

    array = type_str.startswith("array<")
    if array:
        type_str = type_str[6:-1]  # strip "array<" and ">"

    return FieldType(type_str, nullable=nullable, array=array)


def _make_json_serializable(value: Any) -> Any:
    """Convert a value to a JSON-serializable form."""
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    return str(value)
