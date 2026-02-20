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

        # Merge properties: union of all fields
        merged_props = {}

        # Start with existing fields (preserves fields not in new sample)
        for field_name, field_def in existing_props.items():
            merged_props[field_name] = field_def

        # Override/add with new fields (newer inference is more accurate)
        for field_name, field_def in new_props.items():
            merged_props[field_name] = field_def

        # Build merged schema
        merged = dict(new)  # Use new schema as base (title, $schema, etc.)
        merged["properties"] = merged_props
        # Required is empty (all fields optional for safety)
        merged["required"] = []

        added = set(new_props.keys()) - set(existing_props.keys())
        preserved = set(existing_props.keys()) - set(new_props.keys())
        if preserved:
            self.logger.info(
                f"Merged schema: {len(added)} new fields, "
                f"{len(preserved)} preserved from existing"
            )

        return json.dumps(merged, indent=2)

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
