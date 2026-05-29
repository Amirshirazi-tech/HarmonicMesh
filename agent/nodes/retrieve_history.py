"""Node: pull prior memory from Graphiti in two episode-typed tiers."""
from __future__ import annotations

import logging

from graphiti_layer import Episode, search_history

from ..state import AgentState

log = logging.getLogger(__name__)

TIER1_TYPES = ["pattern_occurrence", "intervention", "outcome"]
TIER1_NUM_RESULTS = 4
TIER2_TYPES = ["agent_alert"]
TIER2_NUM_RESULTS = 1


def _peak_temperature(pattern_match: dict) -> float | None:
    events = pattern_match.get("source_events") or []
    temps: list[float] = []
    for event in events:
        sensors = event.get("sensors") or {}
        temp = sensors.get("temperature_c")
        if isinstance(temp, (int, float)):
            temps.append(float(temp))
    return max(temps) if temps else None


def _build_query(pattern_match: dict) -> str:
    """Compose a retrieval query with pattern, machine, and one numeric anchor.

    The numeric anchor is required by the directive — peak temperature is the
    canonical one for ThermalVibrationCascade. Falls back to severity if
    sensor data is unavailable for some reason.
    """
    pattern_name = pattern_match["pattern_name"]
    machine_id = pattern_match["machine_id"]
    severity = pattern_match.get("severity", "UNKNOWN")
    peak_temp = _peak_temperature(pattern_match)

    if peak_temp is not None:
        anchor = f"peak temperature {peak_temp:.1f} C"
    else:
        anchor = f"severity {severity}"
    return f"{pattern_name} on {machine_id} with {anchor}"


async def retrieve_history(state: AgentState) -> AgentState:
    """Two-tier Graphiti retrieval. Tier 1: occurrences + interventions +
    outcomes. Tier 2: prior agent alerts. Results are concatenated in order.

    Raises:
        ValueError: ``graphiti_layer.search_history`` did not accept the
            ``episode_types`` parameter — surfaced cleanly rather than silently
            falling back to unfiltered search.
    """
    pattern_match = state["pattern_match"]
    machine_id = pattern_match["machine_id"]
    query = _build_query(pattern_match)

    try:
        tier1: list[Episode] = await search_history(
            query=query,
            machine_id=machine_id,
            num_results=TIER1_NUM_RESULTS,
            episode_types=TIER1_TYPES,
        )
        tier2: list[Episode] = await search_history(
            query=query,
            machine_id=machine_id,
            num_results=TIER2_NUM_RESULTS,
            episode_types=TIER2_TYPES,
        )
    except TypeError as exc:
        raise ValueError(
            "graphiti_layer.search_history does not accept episode_types. "
            "Phase 5 requires the episode_types filter; refusing to silently "
            "fall back to unfiltered search."
        ) from exc

    retrieved = list(tier1) + list(tier2)
    log.info(
        "retrieve_history: %d episodes (%d tier1, %d tier2) for %s",
        len(retrieved),
        len(tier1),
        len(tier2),
        machine_id,
    )
    return {"retrieved_episodes": retrieved}
