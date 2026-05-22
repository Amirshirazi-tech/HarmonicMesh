"""Turn structured events into natural-language Graphiti episodes.

Every HarmonicMesh episode is ingested as ``EpisodeType.text`` — a short
natural-language narrative — never as raw JSON. Graphiti's extraction LLM reads
the narrative and pulls out the prescribed-ontology entities. These formatters
produce that narrative.

Episodes are kept under 100 words: long enough to carry canonical entity
references, ISO timestamps and peak values with units; short enough that
extraction stays cheap and precise.
"""
from __future__ import annotations

from datetime import datetime

from .ontology import Intervention

# Word ceiling for a single episode. Enforced so a malformed input that would
# balloon the narrative is caught here rather than silently degrading
# extraction quality downstream.
MAX_EPISODE_WORDS = 100

# Maps a pattern name to its family. New CEP patterns (Phase 6) add an entry.
_PATTERN_FAMILY: dict[str, str] = {
    "ThermalVibrationCascade": "bearing_degradation",
}


def _parse_iso(value: str) -> datetime:
    """Parse an ISO 8601 timestamp, tolerating a trailing 'Z'."""
    if not value:
        raise ValueError("empty timestamp")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def occurrence_id(machine_id: str, detected_at: str) -> str:
    """Deterministic id for a pattern occurrence.

    Deterministic so the same CEP match always maps to the same occurrence_id
    — re-ingesting a redelivered Kafka message updates rather than duplicates.
    """
    compact = _parse_iso(detected_at).strftime("%Y%m%dT%H%M%SZ")
    return f"hm-occ-{machine_id.lower()}-{compact}"


def _check_length(text: str) -> str:
    words = len(text.split())
    if words > MAX_EPISODE_WORDS:
        raise ValueError(
            f"episode is {words} words, exceeds the {MAX_EPISODE_WORDS}-word ceiling"
        )
    return text


def format_pattern_match_episode(pattern_match_json: dict) -> str:
    """Render a Flink CEP pattern-match event as a natural-language episode.

    Computes peak sensor values across the match's source events and the
    cascade duration (event-time span from the first to the last source event),
    then produces a <=100-word narrative referencing the canonical Machine,
    Pattern and PatternOccurrence so Graphiti can extract all three.

    Raises:
        ValueError: the pattern match is missing required fields or is empty —
            treated as a poison message by the consumer.
    """
    try:
        pattern_name = pattern_match_json["pattern_name"]
        machine_id = pattern_match_json["machine_id"]
        detected_at = pattern_match_json["detected_at"]
        events = pattern_match_json["source_events"]
    except KeyError as exc:
        raise ValueError(f"pattern match missing required field: {exc}") from exc

    if not events:
        raise ValueError("pattern match has no source_events")

    severity = pattern_match_json.get("severity", "UNKNOWN")
    family = _PATTERN_FAMILY.get(pattern_name, "unclassified")
    machine_type = str(events[0].get("machine_type", "machine")).replace("_", " ")

    try:
        peak_temp = max(e["sensors"]["temperature_c"] for e in events)
        peak_vib = max(e["sensors"]["vibration_rms_mm_s"] for e in events)
        peak_cur = max(e["sensors"]["current_a"] for e in events)
    except (KeyError, TypeError) as exc:
        raise ValueError(f"pattern match source event missing sensor data: {exc}") from exc

    start_iso = events[0]["event_time"]
    end_iso = events[-1]["event_time"]
    duration_s = (_parse_iso(end_iso) - _parse_iso(start_iso)).total_seconds()

    occ_id = occurrence_id(machine_id, detected_at)

    text = (
        f"Pattern occurrence {occ_id}: the {pattern_name} pattern "
        f"(family {family}) was detected on {machine_id}, a {machine_type}, "
        f"at {detected_at} with {severity} severity. The cascade developed "
        f"over {duration_s:.0f} seconds, beginning {start_iso}. Peak sensor "
        f"readings during the cascade were temperature {peak_temp:.1f} °C, "
        f"vibration RMS {peak_vib:.2f} mm/s, and current draw {peak_cur:.1f} A."
    )
    return _check_length(text)


def format_intervention_episode(intervention: Intervention) -> str:
    """Render a validated Intervention as a natural-language episode.

    Takes the Pydantic model (already validated by add_intervention) rather
    than a raw dict, so timestamps and the intervention_type enum are
    guaranteed well-formed before the narrative is built.
    """
    iv = intervention
    action = (iv.intervention_type or "maintenance").replace("_", " ")
    performed_at = iv.performed_at.isoformat() if iv.performed_at else "an unknown time"
    performed_by = iv.performed_by or "an unknown technician"

    text = (
        f"Intervention {iv.intervention_id}: a {action} intervention was "
        f"performed on {iv.machine_id} at {performed_at} by {performed_by}."
    )
    if iv.target_pattern_name:
        text += f" It targeted the {iv.target_pattern_name} pattern."
    if iv.notes:
        text += f" Notes: {iv.notes}"
    return _check_length(text)
