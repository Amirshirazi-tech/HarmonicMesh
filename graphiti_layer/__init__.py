"""HarmonicMesh Graphiti layer — the module-public API.

This package is the *only* code path that touches Graphiti or Neo4j. The
Phase 5 LangGraph agent calls the four coroutines exported here; it never
imports graphiti_core directly.

    ingest_pattern_match  — write a Flink CEP pattern match into the graph
    search_history        — reranked hybrid retrieval of prior memory
    add_intervention      — write a maintenance action into the graph
    add_alert_episode     — write an agent reasoning alert into the graph (Phase 5)

``Episode`` is the project-defined return type of ``search_history``.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from .client import Episode, get_graphiti, get_reranker
from .episode_formatter import (
    _check_length,
    _parse_iso,
    format_intervention_episode,
    format_pattern_match_episode,
)
from .ontology import ENTITY_TYPES, AgentAlert, Intervention

log = logging.getLogger(__name__)

__all__ = [
    "ingest_pattern_match",
    "search_history",
    "add_intervention",
    "add_alert_episode",
    "Episode",
]

# Number of candidates pulled from Graphiti hybrid search before reranking.
# The bge cross-encoder scores all of them — see search_history.
_HYBRID_CANDIDATES = 30

_SOURCE_DESCRIPTION = "HarmonicMesh CEP pattern match"
_INTERVENTION_SOURCE_DESCRIPTION = "HarmonicMesh maintenance intervention"
_ALERT_SOURCE_DESCRIPTION = "HarmonicMesh agent alert"
_OUTCOME_SOURCE_DESCRIPTION = "HarmonicMesh intervention outcome"

# Logical episode-type names (used by the Phase 5 agent) → the exact
# ``source_description`` strings written into Graphiti on ingest.
#
# IMPORTANT — These ``source_description`` strings are *public identifiers* used
# for episode-type discrimination across the system. Changing a value here
# silently breaks retrieval against existing episodes already written to Neo4j.
# Do not change any string in this map without migrating existing episode data
# (or planning for a clean Neo4j volume).
EPISODE_TYPE_TO_SOURCE_DESCRIPTION: dict[str, str] = {
    "pattern_occurrence": _SOURCE_DESCRIPTION,
    "intervention": _INTERVENTION_SOURCE_DESCRIPTION,
    "agent_alert": _ALERT_SOURCE_DESCRIPTION,
    "outcome": _OUTCOME_SOURCE_DESCRIPTION,
}

_SOURCE_DESCRIPTION_TO_EPISODE_TYPE: dict[str, str] = {
    v: k for k, v in EPISODE_TYPE_TO_SOURCE_DESCRIPTION.items()
}


async def ingest_pattern_match(pattern_match_json: dict) -> None:
    """Ingest one Flink CEP pattern-match event as a Graphiti episode.

    The match is rendered to a natural-language episode (peaks and cascade
    duration computed from its source events) and added with EpisodeType.text
    so Graphiti's extraction LLM populates the prescribed ontology. The graph
    is partitioned by machine: group_id is the machine_id.

    Raises:
        ValueError: the pattern match is malformed (a poison message) — raised
            before any Graphiti call so the consumer can skip it cheaply.
    """
    from graphiti_core.nodes import EpisodeType

    # Format first: this validates the payload without a Neo4j round trip.
    episode_text = format_pattern_match_episode(pattern_match_json)

    machine_id = pattern_match_json["machine_id"]
    pattern_name = pattern_match_json["pattern_name"]
    detected_at = pattern_match_json["detected_at"]

    graphiti = await get_graphiti()
    await graphiti.add_episode(
        name=f"{pattern_name} on {machine_id} @ {detected_at}",
        episode_body=episode_text,
        source=EpisodeType.text,
        source_description=_SOURCE_DESCRIPTION,
        reference_time=_parse_iso(detected_at),
        group_id=machine_id,
        entity_types=ENTITY_TYPES,
    )
    log.info("Ingested pattern match: %s on %s", pattern_name, machine_id)


async def search_history(
    query: str,
    machine_id: str | None = None,
    num_results: int = 5,
    episode_types: Optional[list[str]] = None,
) -> list[Episode]:
    """Retrieve prior memory relevant to ``query``, reranked by a cross-encoder.

    Pipeline:
      1. Graphiti hybrid search (semantic + BM25 + graph) for 30 candidates.
      2. If ``episode_types`` is set, batch-fetch each candidate's source
         EpisodicNodes in one Neo4j round trip and drop edges whose source
         episode's ``source_description`` is not in the requested set.
      3. Score every (query, surviving-candidate) pair with the bge-reranker
         and return the top ``num_results`` by reranker score.

    Args:
        query: natural-language query string.
        machine_id: scope to a single machine's graph partition when given.
        num_results: number of post-rerank results to return.
        episode_types: optional list of logical episode-type names from
            ``EPISODE_TYPE_TO_SOURCE_DESCRIPTION`` (e.g. ``["pattern_occurrence",
            "intervention", "outcome"]``). When ``None`` no type filter is applied
            (Phase 4 behaviour). When supplied, an unknown type raises ValueError
            rather than silently widening the search.

    Each returned ``Episode`` carries ``episode_type`` whenever the filter is on
    (or whenever the source episode could be resolved during unfiltered search).
    """
    if episode_types is not None:
        unknown = set(episode_types) - set(EPISODE_TYPE_TO_SOURCE_DESCRIPTION)
        if unknown:
            raise ValueError(
                f"Unknown episode_types: {sorted(unknown)}. "
                f"Known: {sorted(EPISODE_TYPE_TO_SOURCE_DESCRIPTION)}."
            )

    graphiti = await get_graphiti()
    group_ids = [machine_id] if machine_id else None

    edges = await graphiti.search(
        query=query, group_ids=group_ids, num_results=_HYBRID_CANDIDATES
    )
    if not edges:
        return []

    # Resolve source episodes for every candidate edge in a single batched
    # Neo4j round trip. Edges can reference multiple source episodes; we keep
    # the first one with a known source_description, which is what determines
    # the edge's logical episode_type.
    edge_episode_type: dict[int, str | None] = {}
    all_uuids: set[str] = set()
    for edge in edges:
        for uuid in getattr(edge, "episodes", []) or []:
            all_uuids.add(uuid)

    uuid_to_source_desc: dict[str, str] = {}
    if all_uuids:
        from graphiti_core.nodes import EpisodicNode

        episodic_nodes = await EpisodicNode.get_by_uuids(
            graphiti.driver, list(all_uuids)
        )
        uuid_to_source_desc = {
            n.uuid: n.source_description for n in episodic_nodes
        }

    for idx, edge in enumerate(edges):
        edge_episode_type[idx] = None
        for uuid in getattr(edge, "episodes", []) or []:
            desc = uuid_to_source_desc.get(uuid)
            if desc and desc in _SOURCE_DESCRIPTION_TO_EPISODE_TYPE:
                edge_episode_type[idx] = _SOURCE_DESCRIPTION_TO_EPISODE_TYPE[desc]
                break

    if episode_types is not None:
        wanted = set(episode_types)
        filtered = [
            (idx, edge)
            for idx, edge in enumerate(edges)
            if edge_episode_type.get(idx) in wanted
        ]
    else:
        filtered = list(enumerate(edges))

    if not filtered:
        return []

    candidate_texts = [edge.fact for _, edge in filtered]
    reranker = get_reranker()
    # Reranking is CPU-bound; run it off the event loop.
    ranked = await asyncio.to_thread(reranker.rerank, query, candidate_texts)

    results: list[Episode] = []
    for filtered_index, score in ranked[:num_results]:
        original_index, edge = filtered[filtered_index]
        results.append(
            Episode(
                content=edge.fact,
                name=getattr(edge, "name", "") or "fact",
                occurred_at=getattr(edge, "valid_at", None)
                or getattr(edge, "created_at", None),
                reranker_score=score,
                retrieval_rank=original_index,
                episode_type=edge_episode_type.get(original_index),
                metadata={
                    "edge_uuid": getattr(edge, "uuid", None),
                    "source_episode_uuids": list(getattr(edge, "episodes", []) or []),
                },
            )
        )
    return results


async def add_intervention(intervention_data: dict) -> None:
    """Ingest a maintenance Intervention as a Graphiti episode.

    The function and its signature exist for Phase 4; its caller (simulator
    intervention injection, or a future CMMS connector) is Phase 5+ scope.

    Raises:
        pydantic.ValidationError: intervention_type is not one of the allowed
            values, or another field fails validation.
        ValueError: machine_id is missing — it is the graph partition key.
    """
    from graphiti_core.nodes import EpisodeType

    # Validate against the ontology — this is where garbage is rejected.
    intervention = Intervention(**intervention_data)
    if not intervention.machine_id:
        raise ValueError("intervention_data must include machine_id (partition key)")

    episode_text = format_intervention_episode(intervention)
    reference_time = intervention.performed_at
    if reference_time is None:
        reference_time = datetime.now(timezone.utc)

    graphiti = await get_graphiti()
    await graphiti.add_episode(
        name=f"Intervention {intervention.intervention_id} on {intervention.machine_id}",
        episode_body=episode_text,
        source=EpisodeType.text,
        source_description=_INTERVENTION_SOURCE_DESCRIPTION,
        reference_time=reference_time,
        group_id=intervention.machine_id,
        entity_types=ENTITY_TYPES,
    )
    log.info(
        "Ingested intervention %s on %s",
        intervention.intervention_id,
        intervention.machine_id,
    )


async def add_alert_episode(alert_data: dict) -> None:
    """Ingest a Phase 5 agent alert as a Graphiti episode (Layer 2 memory).

    The alert is validated against the ``AgentAlert`` ontology entry and its
    ``summary`` field (already constrained to <=100 words by the caller) is
    written as the episode body. ``source_description`` is set to the stable
    public identifier for agent alerts so future ``search_history`` calls can
    filter for ``episode_types=["agent_alert"]``.

    Raises:
        pydantic.ValidationError: alert_data fails AgentAlert validation.
        ValueError: machine_id is missing (partition key) or summary is missing.
    """
    from graphiti_core.nodes import EpisodeType

    alert = AgentAlert(**alert_data)
    if not alert.machine_id:
        raise ValueError("alert_data must include machine_id (partition key)")
    if not alert.summary:
        raise ValueError("alert_data must include a non-empty summary")

    episode_text = _check_length(alert.summary)
    reference_time = alert.alerted_at or alert.detected_at or datetime.now(timezone.utc)

    graphiti = await get_graphiti()
    await graphiti.add_episode(
        name=f"AgentAlert {alert.alert_id} on {alert.machine_id}",
        episode_body=episode_text,
        source=EpisodeType.text,
        source_description=_ALERT_SOURCE_DESCRIPTION,
        reference_time=reference_time,
        group_id=alert.machine_id,
        entity_types=ENTITY_TYPES,
    )
    log.info(
        "Ingested agent alert %s on %s",
        alert.alert_id,
        alert.machine_id,
    )
