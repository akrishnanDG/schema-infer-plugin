"""
State persistence for live consumer mode.

Saves and loads IncrementalSchemaState to/from JSON files on disk.
"""

import json
import os
import re
import tempfile
from pathlib import Path
from typing import List, Optional

from ..config import Config
from ..utils.exceptions import LiveModeError
from ..utils.logger import get_logger


class StateStore:
    """
    Persists incremental schema state to disk.

    State files are JSON, stored at:
      {state_dir}/{safe_topic_name}.state.json
    """

    def __init__(self, state_dir: Path):
        self.state_dir = Path(state_dir).expanduser()
        self.logger = get_logger(__name__)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def save(self, state: "IncrementalSchemaState") -> None:
        """
        Persist state to disk atomically.

        Writes to a temporary file first, then renames to avoid corruption
        if the process crashes mid-write.
        """
        from .incremental import IncrementalSchemaState

        state_file = self._state_path(state.topic_name)
        state_dict = state.to_dict()

        try:
            # Write to temp file in same directory, then atomic rename
            fd, tmp_path = tempfile.mkstemp(
                dir=self.state_dir, suffix=".tmp", prefix="state_"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(state_dict, f, indent=2)
                os.replace(tmp_path, state_file)
            except Exception:
                # Clean up temp file on failure
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise

            self.logger.debug(f"Saved state for topic {state.topic_name}")
        except Exception as e:
            raise LiveModeError(f"Failed to save state for {state.topic_name}: {e}")

    def load(
        self, topic_name: str, config: Config
    ) -> Optional["IncrementalSchemaState"]:
        """
        Load state from disk.

        Returns None if no state file exists for the topic.
        """
        from .incremental import IncrementalSchemaState

        state_file = self._state_path(topic_name)

        if not state_file.exists():
            return None

        try:
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            state = IncrementalSchemaState.from_dict(data, config)
            self.logger.debug(
                f"Loaded state for topic {topic_name} "
                f"({state.total_records_processed} records)"
            )
            return state
        except Exception as e:
            self.logger.warning(
                f"Failed to load state for {topic_name}, starting fresh: {e}"
            )
            return None

    def delete(self, topic_name: str) -> None:
        """Remove state file for a topic."""
        state_file = self._state_path(topic_name)
        try:
            state_file.unlink(missing_ok=True)
        except Exception as e:
            self.logger.warning(f"Failed to delete state for {topic_name}: {e}")

    def list_topics(self) -> List[str]:
        """List all topics with persisted state."""
        topics = []
        for f in self.state_dir.glob("*.state.json"):
            # Reverse the safe-name encoding
            topic_name = f.stem.replace(".state", "")
            topics.append(topic_name)
        return sorted(topics)

    def _state_path(self, topic_name: str) -> Path:
        """Get the state file path for a topic.

        Uses a hash suffix to prevent collisions when different topic names
        (e.g., 'topic-a.b' and 'topic_a.b') sanitize to the same filename.
        """
        import hashlib

        safe_name = re.sub(r"[^\w\-.]", "_", topic_name)
        # Append short hash to prevent collisions from sanitization
        name_hash = hashlib.sha256(topic_name.encode()).hexdigest()[:8]
        return self.state_dir / f"{safe_name}_{name_hash}.state.json"
