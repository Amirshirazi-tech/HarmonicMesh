"""Node: validate and normalise the incoming pattern-match payload."""
from __future__ import annotations

from datetime import datetime

from ..errors import InvalidPatternMatchError
from ..state import AgentState

REQUIRED_FIELDS = (
    "machine_id",
    "pattern_name",
    "detected_at",
    "severity",
    "source_events",
)


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_pattern_match(state: AgentState) -> AgentState:
    """Ensure the pattern match has every required field and a valid timestamp.

    Raises:
        InvalidPatternMatchError: any required field is missing, empty, or
            ``detected_at`` is not parseable. The consumer treats this as a
            poison message.
    """
    pm = state.get("pattern_match")
    if not isinstance(pm, dict):
        raise InvalidPatternMatchError(
            f"pattern_match must be a dict, got {type(pm).__name__}"
        )

    missing = [f for f in REQUIRED_FIELDS if f not in pm or pm[f] in (None, "", [])]
    if missing:
        raise InvalidPatternMatchError(
            f"pattern_match missing required fields: {missing}"
        )

    try:
        detected_at = _parse_iso(str(pm["detected_at"]))
    except ValueError as exc:
        raise InvalidPatternMatchError(
            f"detected_at is not a valid ISO 8601 timestamp: {pm['detected_at']}"
        ) from exc

    # Normalise into a copy so we don't mutate the consumer's payload.
    normalised = dict(pm)
    normalised["detected_at"] = detected_at

    return {"pattern_match": normalised}
