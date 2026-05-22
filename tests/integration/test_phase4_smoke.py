"""Phase 4 retrieval smoke test.

Verifies the graphiti_layer end to end: pattern matches ingest as episodes,
search_history retrieves relevant memory, and the bge-reranker reorders
candidates rather than passing them through.

NOTE: the retrieval test ingests episodes into the *configured* Neo4j and does
not clean up after itself — run it against a dev / throwaway instance (a fresh
`docker compose up`), not a warm-up snapshot you want to keep.

What runs without live services:
  - test_reranker_reorders_candidates  (needs only the bge model)
  - test_add_intervention_rejects_bad_type  (pure ontology validation)

What needs live services (skipped otherwise):
  - test_phase4_retrieval_smoke  (needs Neo4j + OPENROUTER_API_KEY)
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta

import pytest

# Phase 4 dependencies — skip the whole module cleanly if they are absent.
pytest.importorskip("graphiti_core")
pytest.importorskip("sentence_transformers")


# --------------------------------------------------------------------------
# Fixtures / helpers
# --------------------------------------------------------------------------

# (detected_at, peak_temp_c, peak_vib_mm_s, peak_current_a)
# The last two occurrences peak well above 410 C — the targets of query 2.
_EPISODES = [
    ("2026-01-12T08:15:00.000Z", 392.0, 4.8, 471.0),
    ("2026-02-03T19:40:00.000Z", 401.5, 5.1, 478.0),
    ("2026-02-28T03:22:00.000Z", 398.0, 4.9, 472.0),
    ("2026-03-19T11:05:00.000Z", 405.0, 5.0, 480.0),
    ("2026-04-15T14:23:00.000Z", 422.6, 5.6, 495.0),
    ("2026-05-09T21:50:00.000Z", 431.2, 5.9, 503.0),
]


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _make_pattern_match(
    detected_at: str, peak_temp: float, peak_vib: float, peak_cur: float
) -> dict:
    """Build a ThermalVibrationCascade match in the Phase 3 output schema."""
    end = datetime.fromisoformat(detected_at.replace("Z", "+00:00"))
    start = end - timedelta(seconds=300)
    mid = end - timedelta(seconds=150)

    def event(ts: datetime, temp: float, vib: float, cur: float) -> dict:
        return {
            "machine_id": "Machine-03",
            "machine_type": "rolling_mill",
            "event_time": _iso(ts),
            "event_type": "telemetry",
            "sensors": {
                "temperature_c": temp,
                "vibration_rms_mm_s": vib,
                "current_a": cur,
            },
            "meta": {"sim_seed": 42, "injected_fault": "thermal_vibration_cascade"},
        }

    return {
        "schema_version": "1.0",
        "pattern_name": "ThermalVibrationCascade",
        "machine_id": "Machine-03",
        "detected_at": detected_at,
        "severity": "CRITICAL",
        "source_events": [
            event(start, 384.0, 4.7, 470.0),
            event(mid, peak_temp - 6.0, peak_vib - 0.4, peak_cur - 12.0),
            event(end, peak_temp, peak_vib, peak_cur),
        ],
    }


def _neo4j_reachable() -> bool:
    try:
        from neo4j import GraphDatabase
    except ImportError:
        return False
    password = os.getenv("NEO4J_PASSWORD", "")
    if not password:
        return False
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        driver.verify_connectivity()
        driver.close()
        return True
    except Exception:  # noqa: BLE001
        return False


# --------------------------------------------------------------------------
# Pure tests — no live services required
# --------------------------------------------------------------------------

def test_reranker_reorders_candidates():
    """The cross-encoder must rank by relevance, not by order of arrival."""
    from graphiti_layer.reranker import BGEReranker

    reranker = BGEReranker()
    query = "thermal vibration cascade preceding bearing failure on a rolling mill"
    passages = [
        "The quarterly EDI invoice from the supplier was approved by accounting.",
        "The cafeteria lunch menu was updated for next week.",
        "Machine-03 exhibited a thermal-vibration cascade with rising temperature, "
        "vibration and current draw — a classic bearing-degradation signature.",
    ]

    ranked = reranker.rerank(query, passages)
    order = [idx for idx, _ in ranked]
    scores = [score for _, score in ranked]

    # The relevant passage arrived last but must rank first.
    assert order[0] == 2, f"expected relevant passage first, got order {order}"
    # The reranker must have changed the order of arrival.
    assert order != [0, 1, 2], "reranker left candidates in order of arrival"
    # Output is sorted by descending score.
    assert scores == sorted(scores, reverse=True)


def test_add_intervention_rejects_bad_type():
    """Garbage is rejected at the ontology layer before any Graphiti call."""
    from pydantic import ValidationError

    from graphiti_layer import add_intervention

    bad = {
        "intervention_id": "hm-iv-test-0001",
        "machine_id": "Machine-03",
        "intervention_type": "frobnicate",  # not an allowed InterventionType
        "performed_at": "2026-04-16T09:00:00Z",
        "performed_by": "test-tech",
    }
    with pytest.raises(ValidationError):
        asyncio.run(add_intervention(bad))


# --------------------------------------------------------------------------
# Live retrieval smoke test
# --------------------------------------------------------------------------

async def _run_retrieval_smoke():
    from graphiti_layer import ingest_pattern_match, search_history
    from graphiti_layer.client import close_graphiti

    try:
        for detected_at, temp, vib, cur in _EPISODES:
            await ingest_pattern_match(
                _make_pattern_match(detected_at, temp, vib, cur)
            )

        # Query 1 — recurrence: prior occurrences on a specific machine.
        recurrence = await search_history(
            "recent ThermalVibrationCascade on Machine-03",
            machine_id="Machine-03",
            num_results=5,
        )
        assert recurrence, "recurrence query returned no results"
        joined = " ".join(e.content for e in recurrence).lower()
        assert "machine-03" in joined, "recurrence results do not mention Machine-03"
        assert "thermal" in joined, "recurrence results are not topically relevant"
        r_scores = [e.reranker_score for e in recurrence]
        assert r_scores == sorted(r_scores, reverse=True), "results not score-sorted"

        # Query 2 — similarity: cross-machine semantic match.
        similarity = await search_history(
            "cascades with peak temperature above 410 degrees",
            num_results=5,
        )
        assert similarity, "similarity query returned no results"
        s_joined = " ".join(e.content for e in similarity).lower()
        assert "cascade" in s_joined or "temperature" in s_joined

        # The reranker should have moved candidates off their arrival order for
        # at least one query (the deterministic proof is the pure test above).
        all_ranks = [e.retrieval_rank for e in recurrence] + [
            e.retrieval_rank for e in similarity
        ]
        reordered = all_ranks != sorted(all_ranks)
        print(
            f"\nrecurrence ranks={[e.retrieval_rank for e in recurrence]} "
            f"scores={[round(s, 3) for s in r_scores]}\n"
            f"similarity ranks={[e.retrieval_rank for e in similarity]}\n"
            f"reranker reordered candidates: {reordered}"
        )
    finally:
        await close_graphiti()


def test_phase4_retrieval_smoke():
    """End-to-end: ingest episodes, retrieve them, confirm rerank pipeline runs."""
    if not os.getenv("OPENROUTER_API_KEY"):
        pytest.skip("OPENROUTER_API_KEY not set — skipping live retrieval smoke test")
    if not _neo4j_reachable():
        pytest.skip("Neo4j not reachable — skipping live retrieval smoke test")
    asyncio.run(_run_retrieval_smoke())
