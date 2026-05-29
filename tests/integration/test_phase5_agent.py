"""Phase 5 agent smoke tests.

Three tests, in order of escalating dependencies:

  1. test_validate_confidence_caps_when_no_anchors — pure unit, no I/O.
  2. test_training_record_schema_roundtrip — pydantic + filesystem only.
  3. test_agent_end_to_end_invoke — compiles the LangGraph once and invokes
     it on a synthetic pattern match. Gated on OPENROUTER_API_KEY + live
     Neo4j; skipped otherwise.

Run:
    pytest tests/integration/test_phase5_agent.py -v
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import pytest

# graphiti_layer is imported by agent.state (Episode type). The other two
# heavyweights (langgraph, httpx) are needed only for the live end-to-end test
# — pull them in inside that test so the unit checks can run on a minimal env.
pytest.importorskip("graphiti_core")
pytest.importorskip("sentence_transformers")

from agent.nodes.validate_confidence import validate_confidence  # noqa: E402
from agent.training_record import TrainingRecord, append_record  # noqa: E402


# --------------------------------------------------------------------------
# Test 1 — confidence validation behaviour (anchor presence + tier caps)
# --------------------------------------------------------------------------

def test_validate_confidence_caps_when_no_anchors() -> None:
    """No anchors -> cap at 0.3 and flag 'no_anchors'."""
    state = {
        "confidence_raw": 0.85,
        "confidence_anchors": [],
        "retrieved_episodes": [],
    }
    delta = validate_confidence(state)
    assert delta["confidence_final"] == pytest.approx(0.3)
    assert "no_anchors" in delta["validation_flags"]


def test_validate_confidence_caps_insufficient_anchors_for_high_tier() -> None:
    """High confidence (>=0.7) with fewer than 3 anchors caps at 0.6."""
    state = {
        "confidence_raw": 0.8,
        "confidence_anchors": ["anchor-one", "anchor-two"],
        "retrieved_episodes": [],
    }
    delta = validate_confidence(state)
    assert delta["confidence_final"] == pytest.approx(0.6)
    assert "insufficient_anchors_for_tier" in delta["validation_flags"]


def test_validate_confidence_caps_top_tier_without_outcome() -> None:
    """>=0.9 with no outcome episode caps at 0.8."""

    class _StubEp:
        episode_type = "pattern_occurrence"

    state = {
        "confidence_raw": 0.95,
        "confidence_anchors": ["a", "b", "c", "d"],
        "retrieved_episodes": [_StubEp(), _StubEp()],
    }
    delta = validate_confidence(state)
    assert delta["confidence_final"] == pytest.approx(0.8)
    assert "missing_outcome_for_tier" in delta["validation_flags"]


def test_validate_confidence_passes_when_evidence_supports_score() -> None:
    """4 anchors + outcome episode -> 0.9 confidence survives unchanged."""

    class _Outcome:
        episode_type = "outcome"

    state = {
        "confidence_raw": 0.9,
        "confidence_anchors": ["a", "b", "c", "d"],
        "retrieved_episodes": [_Outcome()],
    }
    delta = validate_confidence(state)
    assert delta["confidence_final"] == pytest.approx(0.9)
    assert delta["validation_flags"] == []


# --------------------------------------------------------------------------
# Test 2 — training record pydantic schema + JSONL writer
# --------------------------------------------------------------------------

def test_training_record_schema_roundtrip(tmp_path, monkeypatch) -> None:
    """TrainingRecord serialises to JSONL and round-trips through json.loads."""
    monkeypatch.setattr(
        "agent.training_record.TRAINING_DATA_DIR", tmp_path / "harmonicmesh"
    )

    record = TrainingRecord(
        record_id="hm-train-machine-03-test",
        machine_id="Machine-03",
        pattern_name="ThermalVibrationCascade",
        detected_at=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
        alerted_at=datetime(2026, 5, 1, 12, 0, 1, tzinfo=timezone.utc),
        pattern_match={"machine_id": "Machine-03", "pattern_name": "ThermalVibrationCascade"},
        retrieved_episodes=[
            {"name": "prior", "content": "prior fact", "episode_type": "pattern_occurrence"}
        ],
        reasoning="historical context supports the signature",
        confidence_anchors=["prior bearing failure on 2026-03-12"],
        confidence_raw=0.65,
        confidence_final=0.6,
        validation_flags=["insufficient_anchors_for_tier"],
        alert_text="Likely bearing degradation; recommend inspection.",
        model="anthropic/claude-haiku-4-5",
        outcome_id=None,
    )

    path = append_record(record)
    assert path.exists()
    contents = path.read_text(encoding="utf-8").strip()
    parsed = json.loads(contents)
    assert parsed["record_id"] == "hm-train-machine-03-test"
    assert parsed["schema_version"] == "1.0"
    assert parsed["model"] == "anthropic/claude-haiku-4-5"
    assert parsed["outcome_id"] is None


# --------------------------------------------------------------------------
# Test 3 — end-to-end agent invocation against live OpenRouter + Neo4j
# --------------------------------------------------------------------------

def _live_services_available() -> bool:
    return bool(os.getenv("OPENROUTER_API_KEY")) and bool(os.getenv("NEO4J_PASSWORD"))


@pytest.mark.skipif(
    not _live_services_available(),
    reason="OPENROUTER_API_KEY and NEO4J_PASSWORD must be set for the live test.",
)
def test_agent_end_to_end_invoke(tmp_path, monkeypatch) -> None:
    """Compile the graph once and invoke it on a synthetic pattern match."""
    import asyncio

    monkeypatch.setattr(
        "agent.training_record.TRAINING_DATA_DIR", tmp_path / "harmonicmesh"
    )

    # The Kafka producer in emit_alert requires SASL creds. If the broker is
    # unreachable in the test environment, this test will fail loudly at that
    # node — that is intentional for an end-to-end smoke test.
    if not (os.getenv("KAFKA_SASL_USERNAME") and os.getenv("KAFKA_SASL_PASSWORD")):
        pytest.skip("Kafka SASL credentials required for the live end-to-end test.")

    pytest.importorskip("langgraph")
    pytest.importorskip("httpx")
    from agent.graph import build_agent

    pattern_match = {
        "machine_id": "Machine-03",
        "pattern_name": "ThermalVibrationCascade",
        "detected_at": "2026-05-29T10:15:00.000Z",
        "severity": "CRITICAL",
        "source_events": [
            {
                "event_time": "2026-05-29T10:14:30.000Z",
                "machine_type": "rolling_mill",
                "sensors": {
                    "temperature_c": 412.3,
                    "vibration_rms_mm_s": 5.2,
                    "current_a": 478.0,
                },
            },
            {
                "event_time": "2026-05-29T10:15:00.000Z",
                "machine_type": "rolling_mill",
                "sensors": {
                    "temperature_c": 418.7,
                    "vibration_rms_mm_s": 5.6,
                    "current_a": 485.0,
                },
            },
        ],
    }

    agent = build_agent()
    final_state = asyncio.run(agent.ainvoke({"pattern_match": pattern_match}))

    assert 0.0 <= final_state["confidence_final"] <= 1.0
    assert final_state["alert_text"]
    assert "reasoning" in final_state
    assert isinstance(final_state["validation_flags"], list)
