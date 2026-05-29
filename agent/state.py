"""Shared state for the Phase 5 LangGraph agent.

LangGraph drives the workflow by threading a single TypedDict through every
node. Each node returns a partial dict; LangGraph merges it into the running
state. Fields are populated in the order the nodes run:

  parse_pattern_match     -> pattern_match
  retrieve_history        -> retrieved_episodes
  reason                  -> reasoning, confidence_anchors, confidence_raw,
                             alert_text
  validate_confidence     -> confidence_final, validation_flags
  emit_alert              -> (no state change; side effect only)
  reify_memory            -> (no state change; side effect only)
  emit_training_record    -> (no state change; side effect only)
"""
from __future__ import annotations

from typing import TypedDict

from graphiti_layer import Episode


class AgentState(TypedDict, total=False):
    """Workflow state for one pattern-match -> alert reasoning trace.

    ``total=False`` because LangGraph populates fields incrementally as nodes
    run. The consumer seeds the state with ``pattern_match`` and lets the
    graph fill the rest.
    """

    pattern_match: dict
    retrieved_episodes: list[Episode]
    reasoning: str
    confidence_anchors: list[str]
    confidence_raw: float
    confidence_final: float
    validation_flags: list[str]
    alert_text: str
