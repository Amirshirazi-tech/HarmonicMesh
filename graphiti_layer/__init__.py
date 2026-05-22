"""HarmonicMesh Graphiti layer — the module-public API.

This package is the *only* code path that touches Graphiti or Neo4j. The
Phase 5 LangGraph agent calls the three coroutines exported here; it never
imports graphiti_core directly.

    ingest_pattern_match  — write a Flink CEP pattern match into the graph
    search_history        — reranked hybrid retrieval of prior memory
    add_intervention      — write a maintenance action into the graph

``Episode`` is the project-defined return type of ``search_history``.
"""
from __future__ import annotations

import asyncio
import logging

from .client import Episode, get_graphiti, get_reranker
from .episode_formatter import (
    _parse_iso,
    format_intervention_episode,
    format_pattern_match_episode,
)
from .ontology import ENTITY_TYPES, Intervention

log = logging.getLogger(__name__)

__all__ = ["ingest_pattern_match", "search_history", "add_intervention", "Episode"]

# Number of candidates pulled from Graphiti hybrid search before reranking.
# The bge cross-encoder scores all of them — see search_history.
_HYBRID_CANDIDATES = 30

_SOURCE_DESCRIPTION = "HarmonicMesh CEP pattern match"


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
    query: str, machine_id: str | None = None, num_results: int = 5
) -> list[Episode]:
    """Retrieve prior memory relevant to ``query``, reranked by a cross-encoder.

    Pipeline:
      1. Graphiti hybrid search (semantic + BM25 + graph) for 30 candidates.
      2. Score every (query, candidate) pair with the bge-reranker — the
         reranker runs on the full candidate set, not a top-K slice.
      3. Return the top ``num_results`` by reranker score.

    When ``machine_id`` is given the search is scoped to that machine's graph
    partition; otherwise it spans every machine.
    """
    graphiti = await get_graphiti()
    group_ids = [machine_id] if machine_id else None

    edges = await graphiti.search(
        query=query, group_ids=group_ids, num_results=_HYBRID_CANDIDATES
    )
    if not edges:
        return []

    candidate_texts = [edge.fact for edge in edges]
    reranker = get_reranker()
    # Reranking is CPU-bound; run it off the event loop.
    ranked = await asyncio.to_thread(reranker.rerank, query, candidate_texts)

    results: list[Episode] = []
    for original_index, score in ranked[:num_results]:
        edge = edges[original_index]
        results.append(
            Episode(
                content=edge.fact,
                name=getattr(edge, "name", "") or "fact",
                occurred_at=getattr(edge, "valid_at", None)
                or getattr(edge, "created_at", None),
                reranker_score=score,
                retrieval_rank=original_index,
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
        from datetime import datetime, timezone

        reference_time = datetime.now(timezone.utc)

    graphiti = await get_graphiti()
    await graphiti.add_episode(
        name=f"Intervention {intervention.intervention_id} on {intervention.machine_id}",
        episode_body=episode_text,
        source=EpisodeType.text,
        source_description="HarmonicMesh maintenance intervention",
        reference_time=reference_time,
        group_id=intervention.machine_id,
        entity_types=ENTITY_TYPES,
    )
    log.info(
        "Ingested intervention %s on %s",
        intervention.intervention_id,
        intervention.machine_id,
    )
