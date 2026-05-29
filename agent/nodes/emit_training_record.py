"""Node: append one TrainingRecord JSONL row for SovereignMesh."""
from __future__ import annotations

import logging
import uuid as uuid_mod
from datetime import datetime, timezone

from graphiti_layer import Episode

from ..llm import GENERATION_MODEL
from ..state import AgentState
from ..training_record import TrainingRecord, append_record

log = logging.getLogger(__name__)


def _episode_to_dict(ep: Episode) -> dict:
    return {
        "name": ep.name,
        "content": ep.content,
        "episode_type": ep.episode_type,
        "occurred_at": ep.occurred_at.isoformat() if ep.occurred_at else None,
        "reranker_score": ep.reranker_score,
        "retrieval_rank": ep.retrieval_rank,
        "metadata": ep.metadata,
    }


async def emit_training_record(state: AgentState) -> AgentState:
    pm = state["pattern_match"]
    machine_id = pm["machine_id"]
    detected_at = pm["detected_at"]
    if isinstance(detected_at, str):
        detected_at = datetime.fromisoformat(detected_at.replace("Z", "+00:00"))

    alerted_at = datetime.now(timezone.utc)
    record = TrainingRecord(
        record_id=f"hm-train-{machine_id.lower()}-{uuid_mod.uuid4().hex[:12]}",
        machine_id=machine_id,
        pattern_name=pm["pattern_name"],
        detected_at=detected_at,
        alerted_at=alerted_at,
        pattern_match={
            "machine_id": machine_id,
            "pattern_name": pm["pattern_name"],
            "severity": pm.get("severity"),
            "detected_at": detected_at.isoformat(),
            "source_events": pm.get("source_events", []),
        },
        retrieved_episodes=[
            _episode_to_dict(ep) for ep in (state.get("retrieved_episodes") or [])
        ],
        reasoning=state.get("reasoning", ""),
        confidence_anchors=list(state.get("confidence_anchors") or []),
        confidence_raw=float(state.get("confidence_raw", 0.0)),
        confidence_final=float(state.get("confidence_final", 0.0)),
        validation_flags=list(state.get("validation_flags") or []),
        alert_text=state.get("alert_text", ""),
        model=GENERATION_MODEL,
        outcome_id=None,
    )
    path = append_record(record)
    log.info("emit_training_record: appended to %s", path)
    return {}
