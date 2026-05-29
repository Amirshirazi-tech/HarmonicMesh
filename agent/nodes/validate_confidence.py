"""Node: programmatic confidence validation gate.

Defence in depth against an over-confident LLM. The LLM's raw value is
preserved as ``confidence_raw``; the downgraded value becomes
``confidence_final``. Validation NEVER halts the graph — failed checks add a
flag and cap the score, and downstream nodes still emit the alert.

Three checks (anchor verifiability is deferred to v2):

  1. Anchor presence:  no anchors -> cap at 0.3, flag "no_anchors"
  2. >=0.7 tier needs >=3 anchors -> else cap at 0.6, flag
                                     "insufficient_anchors_for_tier"
  3. >=0.9 tier needs an outcome episode in retrieved memory -> else cap at
                                     0.8, flag "missing_outcome_for_tier"
"""
from __future__ import annotations

import logging

from ..state import AgentState

log = logging.getLogger(__name__)


def validate_confidence(state: AgentState) -> AgentState:
    confidence = float(state.get("confidence_raw", 0.0))
    anchors = state.get("confidence_anchors") or []
    episodes = state.get("retrieved_episodes") or []
    flags: list[str] = []

    # Check 1: anchor presence.
    if not anchors:
        confidence = min(confidence, 0.3)
        flags.append("no_anchors")

    # Check 2: scale compliance for the 0.7-0.8 tier.
    if confidence >= 0.7 and len(anchors) < 3:
        confidence = min(confidence, 0.6)
        flags.append("insufficient_anchors_for_tier")

    # Check 3: scale compliance for the 0.9+ tier.
    if confidence >= 0.9:
        has_outcome = any(
            ep.episode_type == "outcome" for ep in episodes
        )
        if not has_outcome:
            confidence = min(confidence, 0.8)
            flags.append("missing_outcome_for_tier")

    if flags:
        log.info(
            "validate_confidence: capped %.2f -> %.2f, flags=%s",
            state.get("confidence_raw", 0.0),
            confidence,
            flags,
        )

    return {
        "confidence_final": confidence,
        "validation_flags": flags,
    }
