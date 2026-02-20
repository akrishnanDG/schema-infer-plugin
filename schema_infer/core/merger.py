"""
Schema merger for combining existing registered schemas with newly inferred ones.

Ensures that schemas are additive — fields and event types from previous
inferences are preserved even when the current sample doesn't contain them.
"""

import json
from typing import Any, Dict, List, Optional

from ..utils.logger import get_logger


class SchemaMerger:
    """
    Merges an existing schema (from Schema Registry) with a newly inferred schema.

    Merge rules:
    - Field in both: keep the field, prefer the new type definition
    - Field only in existing: keep it (valid field not seen in this sample)
    - Field only in new: add it (newly discovered field)
    - Event type only in existing: keep the sub-schema
    - Event type only in new: add it
    """

    def __init__(self):
        self.logger = get_logger(__name__)

    def merge_flat_schemas(
        self,
        existing_schema_json: str,
        new_schema_json: str,
    ) -> str:
        """
        Merge an existing flat JSON Schema with a newly inferred one.

        Args:
            existing_schema_json: Existing schema JSON string from SR
            new_schema_json: Newly inferred schema JSON string

        Returns:
            Merged schema JSON string
        """
        try:
            existing = json.loads(existing_schema_json)
            new = json.loads(new_schema_json)
        except (json.JSONDecodeError, TypeError):
            # If we can't parse existing, just use new
            return new_schema_json

        # If existing is a oneOf (multi-event) schema, don't merge flat into it
        if "oneOf" in existing:
            return new_schema_json

        existing_props = existing.get("properties", {})
        new_props = new.get("properties", {})

        # Merge properties: union of all fields with deep merge
        merged_props = self._merge_properties(existing_props, new_props)

        # Build merged schema
        merged = dict(new)  # Use new schema as base (title, $schema, etc.)
        merged["properties"] = merged_props
        # Required is empty (all fields optional for safety)
        merged["required"] = []
        # Match additionalProperties from existing schema to avoid
        # ADDITIONAL_PROPERTIES_REMOVED/ADDED compatibility errors.
        # If existing has it, preserve it. If existing doesn't have it,
        # remove it from merged to keep the same content model.
        if "additionalProperties" in existing:
            merged["additionalProperties"] = existing["additionalProperties"]
        elif "additionalProperties" in merged:
            del merged["additionalProperties"]

        added = set(new_props.keys()) - set(existing_props.keys())
        preserved = set(existing_props.keys()) - set(new_props.keys())
        if preserved:
            self.logger.info(
                f"Merged schema: {len(added)} new fields, "
                f"{len(preserved)} preserved from existing"
            )

        return json.dumps(merged, indent=2)

    def _merge_properties(
        self,
        existing_props: Dict[str, Any],
        new_props: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Recursively merge two property sets.

        - Fields only in existing: preserved
        - Fields only in new: added
        - Fields in both with same type: use new definition
        - Fields in both with different types: keep existing (avoid compat errors)
        - Fields in both that are objects: recursively merge nested properties
        """
        merged = dict(existing_props)

        for field_name, new_def in new_props.items():
            if field_name not in merged:
                merged[field_name] = new_def
                continue

            existing_def = merged[field_name]

            # Check types
            existing_type = self._extract_primary_type(existing_def)
            new_type = self._extract_primary_type(new_def)

            # Different types: keep existing to avoid compatibility errors
            if existing_type and new_type and existing_type != new_type:
                continue

            # Both are objects with properties: deep merge
            if (existing_type == "object"
                    and isinstance(existing_def, dict) and "properties" in existing_def
                    and isinstance(new_def, dict) and "properties" in new_def):
                merged_nested = self._merge_properties(
                    existing_def.get("properties", {}),
                    new_def.get("properties", {}),
                )
                merged_def = dict(new_def)
                merged_def["properties"] = merged_nested
                merged[field_name] = merged_def
                continue

            # Same type: use new definition
            merged[field_name] = new_def

        return merged

    @staticmethod
    def _extract_primary_type(field_def: Any) -> str:
        """Extract the primary (non-null) type from a field definition."""
        if not isinstance(field_def, dict):
            return ""
        field_type = field_def.get("type", "")
        if isinstance(field_type, str):
            return field_type
        if isinstance(field_type, list):
            for t in field_type:
                if t != "null":
                    return t
        return ""

    def merge_multi_event_schemas(
        self,
        existing_main_json: str,
        new_event_schemas: Dict[str, str],
        new_main_json: str,
        topic_name: str,
        existing_sub_schemas: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """
        Merge existing multi-event schemas with newly inferred ones.

        Preserves event types from the existing schema that weren't seen in
        the current sample.

        Args:
            existing_main_json: Existing main oneOf schema from SR
            new_event_schemas: Dict mapping event type to new sub-schema JSON
            new_main_json: New main oneOf schema JSON
            topic_name: Topic name for subject generation
            existing_sub_schemas: Dict mapping event type to existing sub-schema JSON

        Returns:
            Dict with merged schemas:
              - "{topic_name}" -> merged main oneOf schema
              - "{topic_name}.{event_type}" -> merged sub-schema per type
        """
        result = {}

        # Parse existing main schema to find existing event types
        existing_event_types = set()
        try:
            existing_main = json.loads(existing_main_json)
            if "oneOf" in existing_main:
                for ref in existing_main["oneOf"]:
                    ref_name = ref.get("$ref", "")
                    if ref_name.startswith(f"{topic_name}-"):
                        event_type = ref_name[len(f"{topic_name}-"):]
                        existing_event_types.add(event_type)
        except (json.JSONDecodeError, TypeError):
            pass

        new_event_types = set(new_event_schemas.keys())
        all_event_types = existing_event_types | new_event_types

        # Merge sub-schemas
        for event_type in sorted(all_event_types):
            key = f"{topic_name}.{event_type}"
            new_sub = new_event_schemas.get(event_type)
            existing_sub = (existing_sub_schemas or {}).get(event_type)

            if new_sub and existing_sub:
                # Both exist: merge fields
                result[key] = self.merge_flat_schemas(existing_sub, new_sub)
            elif new_sub:
                # Only in new
                result[key] = new_sub
            elif existing_sub:
                # Only in existing: preserve
                result[key] = existing_sub
                self.logger.info(f"Preserved existing sub-schema for event type '{event_type}'")

        # Build merged main schema with all event types
        merged_main = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": topic_name,
            "description": f"Multi-event schema for {topic_name}",
            "oneOf": [
                {"$ref": f"{topic_name}-{et}"}
                for et in sorted(all_event_types)
            ],
        }
        result[topic_name] = json.dumps(merged_main, indent=2)

        preserved_types = existing_event_types - new_event_types
        if preserved_types:
            self.logger.info(
                f"Merged multi-event schema: {len(new_event_types)} current, "
                f"{len(preserved_types)} preserved from existing ({preserved_types})"
            )

        return result

    def fetch_existing_sub_schemas(
        self,
        registry,
        topic_name: str,
        event_types: List[str],
    ) -> Dict[str, str]:
        """
        Fetch existing sub-schemas from Schema Registry for known event types.

        Args:
            registry: SchemaRegistry instance
            topic_name: Topic name
            event_types: List of event type names to check

        Returns:
            Dict mapping event type to existing schema JSON string
        """
        existing = {}
        for event_type in event_types:
            subject = f"{topic_name}-{event_type}"
            try:
                result = registry.get_latest_schema(subject)
                if result and "schema" in result:
                    existing[event_type] = result["schema"]
            except Exception:
                pass
        return existing
