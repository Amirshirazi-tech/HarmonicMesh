"""Phase 6 pattern smoke tests.

Three tests in order of escalating dependencies:

  1. test_edi_event_simulator_produces_valid_events
     Pure unit — no Kafka, no Flink, no Neo4j.
     Exercises the simulator's event-generation logic directly.

  2. test_missing_heartbeat_detection
     Integration — requires the full stack + MissingHeartbeatJob running.
     Injects a 65-second heartbeat gap for Machine-04 and asserts one
     MissingHeartbeat match arrives in harmonicmesh.patterns.machine-04.

  3. test_edi_sequence_violation_shipment_without_order
     Integration — requires the full stack + EDISequenceViolationJob running.
     Directly produces a shipment event (no preceding order) and asserts one
     EDISequenceViolation / shipment_without_order match arrives in
     harmonicmesh.patterns.edi.

Run:
    pytest tests/integration/test_phase6_patterns.py -v
"""
from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

# ── Skip conditions ────────────────────────────────────────────────────────────

def _kafka_reachable() -> bool:
    """True if Kafka is running and credentials are available."""
    try:
        from confluent_kafka import Producer, KafkaException
    except ImportError:
        return False
    if not os.getenv("KAFKA_SASL_USERNAME"):
        return False
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9192")
    try:
        p = Producer({
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.getenv("KAFKA_SASL_USERNAME", ""),
            "sasl.password": os.getenv("KAFKA_SASL_PASSWORD", ""),
        })
        p.flush(timeout=5.0)
        return True
    except Exception:
        return False


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _produce_message(topic: str, key: str, value: dict) -> None:
    """Direct Kafka producer helper for test injection."""
    from confluent_kafka import Producer
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9192")
    p = Producer({
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.getenv("KAFKA_SASL_USERNAME", ""),
        "sasl.password": os.getenv("KAFKA_SASL_PASSWORD", ""),
    })
    p.produce(topic, key=key.encode(), value=json.dumps(value).encode())
    p.flush(timeout=10.0)


def _consume_one(
    topic: str,
    group_id: str,
    timeout_s: float = 90.0,
    filter_fn=None,
) -> Optional[dict]:
    """Poll a topic until one message passes filter_fn (or timeout)."""
    from confluent_kafka import Consumer, KafkaError
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9192")
    c = Consumer({
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.getenv("KAFKA_SASL_USERNAME", ""),
        "sasl.password": os.getenv("KAFKA_SASL_PASSWORD", ""),
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    c.subscribe([topic])
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                payload = json.loads(msg.value())
            except json.JSONDecodeError:
                continue
            if filter_fn is None or filter_fn(payload):
                return payload
    finally:
        c.close()
    return None


# ── Test 1: pure unit — no services required ──────────────────────────────────

def test_edi_event_simulator_produces_valid_events() -> None:
    """make_edi_event and plan_transaction honour the spec's output schema."""
    from simulators.edi_simulator.__main__ import make_edi_event, plan_transaction

    rng = random.Random(42)
    sim_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    order_id = f"order-{uuid.uuid4()}"

    # ── schema check per event type ────────────────────────────────────────
    for ev_type in ("order", "shipment", "invoice"):
        evt = make_edi_event(ev_type, order_id, sim_time, rng)

        assert evt["event_id"].startswith("edi-"), f"bad event_id: {evt['event_id']}"
        assert evt["event_type"] == ev_type
        assert evt["order_id"] == order_id
        # ISO 8601 with milliseconds and Z suffix
        assert evt["event_time"].endswith("Z")
        assert "T" in evt["event_time"]
        # payload is a JSON string (not a nested dict)
        assert isinstance(evt["payload"], str), "payload must be a JSON string"
        payload_obj = json.loads(evt["payload"])
        assert isinstance(payload_obj, dict), "payload JSON must decode to a dict"

    # ── plan_transaction produces the right violation shape ────────────────
    # Normal (no skips)
    events = plan_transaction(order_id, sim_time, rng, 0.0, 0.0, 0.0)
    types = [t for _, t, _ in events]
    assert types == ["order", "shipment", "invoice"], f"unexpected event sequence: {types}"

    # Order skip → shipment_without_order scenario
    events = plan_transaction(order_id, sim_time, rng, 1.0, 0.0, 0.0)
    types = [t for _, t, _ in events]
    assert "order" not in types
    assert "shipment" in types

    # Shipment skip → order_unfulfilled + invoice_without_shipment scenario
    events = plan_transaction(order_id, sim_time, rng, 0.0, 1.0, 0.0)
    types = [t for _, t, _ in events]
    assert "shipment" not in types
    assert "order" in types
    assert "invoice" in types


# ── Test 2: integration — requires Kafka + MissingHeartbeatJob ────────────────

@pytest.mark.skipif(not _kafka_reachable(), reason="Kafka not reachable")
def test_missing_heartbeat_detection() -> None:
    """Inject a 65-second heartbeat gap; assert one MissingHeartbeat match fires.

    Requires:
      - Kafka running at KAFKA_BOOTSTRAP_SERVERS (default localhost:9192)
      - MissingHeartbeatJob submitted and running against the cluster
      - KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD set

    Gap semantics: Machine-04 emits heartbeats every ~60 simulated seconds.
    MissingHeartbeatJob fires when consecutive gap > GAP_THRESHOLD_SECONDS (63 s).
    We inject two heartbeats 65 seconds apart to trigger exactly one detection.
    """
    # Use real wall-clock time as event-time (compression=1 for this test)
    now = datetime.now(tz=timezone.utc)
    machine_id = "Machine-04"
    topic_in  = "harmonicmesh.heartbeats.machine-04"
    topic_out = "harmonicmesh.patterns.machine-04"

    # First heartbeat — establishes the baseline
    hb1 = {
        "machine_id": machine_id,
        "event_time": _iso(now - timedelta(seconds=70)),
        "event_type": "heartbeat",
        "sequence": 1,
    }
    # Second heartbeat — 65 seconds after the first → gap > 63 s threshold
    hb2 = {
        "machine_id": machine_id,
        "event_time": _iso(now - timedelta(seconds=5)),
        "event_type": "heartbeat",
        "sequence": 2,
    }

    _produce_message(topic_in, machine_id, hb1)
    _produce_message(topic_in, machine_id, hb2)

    group = f"test-missing-hb-{uuid.uuid4()}"
    match = _consume_one(
        topic_out,
        group_id=group,
        timeout_s=90.0,
        filter_fn=lambda m: (
            m.get("pattern_name") == "MissingHeartbeat"
            and m.get("machine_id") == machine_id
        ),
    )

    assert match is not None, (
        "No MissingHeartbeat match received within 90 s — "
        "is MissingHeartbeatJob submitted and running?"
    )
    assert match["schema_version"] == "1.0"
    assert match["severity"] == "CRITICAL"
    assert len(match["source_events"]) >= 1, "source_events should contain the prior heartbeat"
    prior_hb = match["source_events"][0]
    assert prior_hb.get("event_type") == "heartbeat"
    assert prior_hb.get("sequence") == 1


# ── Test 3: integration — requires Kafka + EDISequenceViolationJob ─────────────

@pytest.mark.skipif(not _kafka_reachable(), reason="Kafka not reachable")
def test_edi_sequence_violation_shipment_without_order() -> None:
    """Inject an orphan shipment; assert one EDISequenceViolation/shipment_without_order fires.

    Requires:
      - Kafka running
      - EDISequenceViolationJob submitted and running against the cluster
      - KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD set

    Strategy: use a fresh order_id that has never appeared in the stream so
    PREV(event_type) IS NULL fires on the shipment.
    """
    topic_in  = "harmonicmesh.edi.events"
    topic_out = "harmonicmesh.patterns.edi"

    order_id = f"order-{uuid.uuid4()}"   # fresh — no prior order for this id
    now = datetime.now(tz=timezone.utc)

    orphan_shipment = {
        "event_id":   f"edi-{uuid.uuid4()}",
        "event_type": "shipment",
        "order_id":   order_id,
        "event_time": _iso(now),
        "payload":    json.dumps({
            "tracking_id": "TRK-TEST-001",
            "carrier":     "FedEx",
            "expected_delivery": _iso(now + timedelta(days=3)),
        }),
    }

    _produce_message(topic_in, order_id, orphan_shipment)

    group = f"test-swo-{uuid.uuid4()}"
    match = _consume_one(
        topic_out,
        group_id=group,
        timeout_s=90.0,
        filter_fn=lambda m: (
            m.get("pattern_name")  == "EDISequenceViolation"
            and m.get("violation_type") == "shipment_without_order"
            and any(e.get("order_id") == order_id
                    for e in m.get("source_events", []))
        ),
    )

    assert match is not None, (
        "No EDISequenceViolation/shipment_without_order match received within 90 s — "
        "is EDISequenceViolationJob submitted and running?"
    )
    assert match["schema_version"] == "1.0"
    assert match["machine_id"]     == "EDI-System"
    assert match["severity"]       == "HIGH"
    se = match["source_events"]
    assert len(se) >= 1
    assert se[0]["event_type"] == "shipment"
    assert se[0]["order_id"]   == order_id
