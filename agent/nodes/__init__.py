"""LangGraph nodes for the HarmonicMesh Phase 5 reasoning agent.

Each module exports a single callable (sync or async) that takes an
``AgentState`` and returns a partial state dict. ``agent/graph.py`` wires
them in order; nodes never know about Kafka or each other directly.
"""
from .emit_alert import emit_alert
from .emit_training_record import emit_training_record
from .parse_pattern_match import parse_pattern_match
from .reason import reason
from .reify_memory import reify_memory
from .retrieve_history import retrieve_history
from .validate_confidence import validate_confidence

__all__ = [
    "emit_alert",
    "emit_training_record",
    "parse_pattern_match",
    "reason",
    "reify_memory",
    "retrieve_history",
    "validate_confidence",
]
