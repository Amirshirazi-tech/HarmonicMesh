"""Node: write the agent's alert back to Graphiti as Layer 2 memory."""
from __future__ import annotations

import logging
import uuid as uuid_mod
from datetime import datetime, timezone

from graphiti_layer import add_alert_episode

from ..state import AgentState

log = logging.getLogger(__name__)

MAX_SUMMARY_WORDS = 100


def _truncate_to_words(text: str, max_words: int) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words])


def _format_summary(state: AgentState) -> str:
    pm = state["pattern_match"]
    confidence = float(state.get("confidence_final", 0.0))
    flags = state.get("validation_flags") or []
    alert_text = state.get("alert_text") or ""
    machine_id = pm["machine_id"]
    pattern_name = pm["pattern_name"]
    detected_at = pm["detected_at"]
    if hasattr(detected_at, "isoformat"):
        detected_at_iso = detected_at.isoformat()
    else:
        detected_at_iso = str(detected_at)

    summary = (
        f"Agent alert on {machine_id} at {detected_at_iso}: the {pattern_name} "
        f"pattern was raised with confidence {confidence:.2f}. {alert_text}"
    )
    if flags:
        summary += f" Validation flags: {', '.join(flags)}."

    return _truncate_to_words(summary, MAX_SUMMARY_WORDS)


async def reify_memory(state: AgentState) -> AgentState:
    pm = state["pattern_match"]
    machine_id = pm["machine_id"]
    detected_at = pm["detected_at"]
    if hasattr(detected_at, "isoformat"):
        detected_at_iso = detected_at.isoformat()
    else:
        detected_at_iso = str(detected_at)

    alert_id = f"hm-alert-{machine_id.lower()}-{uuid_mod.uuid4().hex[:12]}"

    await add_alert_episode(
        {
            "alert_id": alert_id,
            "machine_id": machine_id,
            "pattern_name": pm["pattern_name"],
            "detected_at": detected_at_iso,
            "alerted_at": datetime.now(timezone.utc),
            "severity": pm.get("severity"),
            "confidence": float(state.get("confidence_final", 0.0)),
            "confidence_raw": float(state.get("confidence_raw", 0.0)),
            "validation_flags": list(state.get("validation_flags") or []),
            "reasoning": state.get("reasoning", ""),
            "summary": _format_summary(state),
        }
    )
    log.info("reify_memory: wrote %s to Graphiti", alert_id)
    return {}
