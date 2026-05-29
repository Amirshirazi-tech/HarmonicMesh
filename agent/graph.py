"""Assemble the Phase 5 LangGraph reasoning agent.

The graph is linear in v1 — no conditional routing. Order matches the
directive:

    parse -> retrieve -> reason -> validate_confidence
          -> emit_alert -> reify_memory -> emit_training_record

``build_agent()`` compiles and returns the graph. The Kafka consumer calls
this exactly once at startup and reuses the compiled object across messages;
re-compiling per message is forbidden.
"""
from __future__ import annotations

from langgraph.graph import END, START, StateGraph

from .nodes import (
    emit_alert,
    emit_training_record,
    parse_pattern_match,
    reason,
    reify_memory,
    retrieve_history,
    validate_confidence,
)
from .state import AgentState


def build_agent():
    """Construct and compile the agent's LangGraph. Returns the compiled graph."""
    builder = StateGraph(AgentState)

    builder.add_node("parse_pattern_match", parse_pattern_match)
    builder.add_node("retrieve_history", retrieve_history)
    builder.add_node("reason", reason)
    builder.add_node("validate_confidence", validate_confidence)
    builder.add_node("emit_alert", emit_alert)
    builder.add_node("reify_memory", reify_memory)
    builder.add_node("emit_training_record", emit_training_record)

    builder.add_edge(START, "parse_pattern_match")
    builder.add_edge("parse_pattern_match", "retrieve_history")
    builder.add_edge("retrieve_history", "reason")
    builder.add_edge("reason", "validate_confidence")
    builder.add_edge("validate_confidence", "emit_alert")
    builder.add_edge("emit_alert", "reify_memory")
    builder.add_edge("reify_memory", "emit_training_record")
    builder.add_edge("emit_training_record", END)

    return builder.compile()
