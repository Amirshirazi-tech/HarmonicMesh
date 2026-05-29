"""Exceptions specific to the Phase 5 agent.

Separated from the node modules so the Kafka consumer can import the exception
types without pulling in any graph or LLM machinery.
"""
from __future__ import annotations


class InvalidPatternMatchError(ValueError):
    """Raised by ``parse_pattern_match`` when a Kafka payload is malformed.

    The consumer treats this as a *poison message*: it logs and commits the
    offset so a single bad record does not wedge the partition. It is a
    subclass of :class:`ValueError` so legacy ``except ValueError`` blocks
    keep behaving the same way.
    """


class LLMReasoningError(RuntimeError):
    """Raised by ``reason`` when the LLM call fails or returns invalid JSON.

    Treated as a *transient* failure by the consumer: the offset is not
    committed and the message is re-delivered after an exponential backoff.
    """
