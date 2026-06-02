"""Training-record model and JSONL writer for the downstream training-pipeline export.

The Phase 5 agent's ``emit_training_record`` node serialises one record per
alert into ``./training_data/harmonicmesh/{YYYY-MM-DD}.jsonl``. A downstream
training pipeline later reads the same file to train models.

``outcome_id`` is reserved for the deferred-grading job that will backfill
the field once a maintenance Outcome arrives; v1 always writes ``None``.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field

TRAINING_DATA_DIR = Path(
    os.getenv("TRAINING_DATA_DIR", "./training_data/harmonicmesh")
)

SCHEMA_VERSION = "1.0"


class TrainingRecord(BaseModel):
    """One row of the agent's training-data export."""

    record_id: str = Field(description="Unique id for this training record.")
    schema_version: str = Field(
        default=SCHEMA_VERSION,
        description="Schema version of this record; bump on breaking changes.",
    )
    machine_id: str = Field(description="Canonical machine id, e.g. 'Machine-03'.")
    pattern_name: str = Field(description="Pattern this alert was raised for.")
    detected_at: datetime = Field(
        description="Event-time of the underlying CEP pattern match."
    )
    alerted_at: datetime = Field(
        description="Wall-clock time at which the agent emitted the alert."
    )
    pattern_match: dict[str, Any] = Field(
        description="The raw pattern-match payload the agent reasoned over."
    )
    retrieved_episodes: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Snapshot of episodes retrieved from Graphiti.",
    )
    reasoning: str = Field(description="The LLM's underlying logic for the alert.")
    confidence_anchors: list[str] = Field(
        default_factory=list,
        description="Strings the LLM offered as anchors for its confidence.",
    )
    confidence_raw: float = Field(
        description="Confidence reported by the LLM before validation."
    )
    confidence_final: float = Field(
        description="Confidence after programmatic validation downgrades."
    )
    validation_flags: list[str] = Field(
        default_factory=list,
        description="Flags raised by validate_confidence.",
    )
    alert_text: str = Field(description="The natural-language alert sent on Kafka.")
    model: str = Field(description="LLM model id used to produce the reasoning.")
    outcome_id: Optional[str] = Field(
        default=None,
        description="Linked Outcome id, filled by the deferred-grading job. "
        "Always None in v1.",
    )


def _filename_for(when: datetime) -> Path:
    return TRAINING_DATA_DIR / f"{when.astimezone(timezone.utc).date().isoformat()}.jsonl"


def append_record(record: TrainingRecord) -> Path:
    """Append one ``TrainingRecord`` to today's JSONL file. Returns the path."""
    TRAINING_DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = _filename_for(record.alerted_at)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(record.model_dump_json() + "\n")
    return path
