"""Node: call the LLM with the anchored confidence scale prompt."""
from __future__ import annotations

import json
import logging
from pathlib import Path

from graphiti_layer import Episode

from ..errors import LLMReasoningError
from ..llm import generate_json
from ..state import AgentState

log = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "reasoning_prompt.txt"
# Read once at import time; the template never changes during process lifetime.
_SYSTEM_PROMPT = _PROMPT_PATH.read_text(encoding="utf-8")


def _format_episode(ep: Episode) -> dict:
    return {
        "name": ep.name,
        "content": ep.content,
        "episode_type": ep.episode_type,
        "occurred_at": ep.occurred_at.isoformat() if ep.occurred_at else None,
        "reranker_score": ep.reranker_score,
    }


def _build_user_prompt(state: AgentState) -> str:
    pm = state["pattern_match"]
    episodes = state.get("retrieved_episodes") or []

    payload = {
        "pattern_match": {
            "machine_id": pm["machine_id"],
            "pattern_name": pm["pattern_name"],
            "severity": pm.get("severity"),
            "detected_at": pm["detected_at"].isoformat()
            if hasattr(pm["detected_at"], "isoformat")
            else pm["detected_at"],
            "source_events": pm.get("source_events", []),
        },
        "retrieved_history": [_format_episode(ep) for ep in episodes],
    }
    return json.dumps(payload, indent=2, default=str)


async def reason(state: AgentState) -> AgentState:
    """Call the LLM and parse out reasoning, anchors, confidence, alert text."""
    response = await generate_json(
        system_prompt=_SYSTEM_PROMPT,
        user_prompt=_build_user_prompt(state),
    )

    try:
        reasoning = str(response["reasoning"]).strip()
        anchors_raw = response.get("confidence_anchors", []) or []
        anchors = [str(a).strip() for a in anchors_raw if str(a).strip()]
        confidence_raw = float(response["confidence"])
        alert_text = str(response["alert_text"]).strip()
    except (KeyError, TypeError, ValueError) as exc:
        raise LLMReasoningError(
            f"LLM JSON response missing required fields: {exc}; got {response}"
        ) from exc

    # Clamp to [0,1] defensively — the prompt requests this but we never trust
    # the model output to stay in range.
    confidence_raw = max(0.0, min(1.0, confidence_raw))

    log.info(
        "reason: confidence_raw=%.2f anchors=%d", confidence_raw, len(anchors)
    )

    return {
        "reasoning": reasoning,
        "confidence_anchors": anchors,
        "confidence_raw": confidence_raw,
        "alert_text": alert_text,
    }
