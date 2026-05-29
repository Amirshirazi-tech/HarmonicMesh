"""Node: publish the validated alert to Kafka.

Publishing happens inside a graph node by design (per the directive). The
upstream Kafka *consumer* is the outer loop; this is a separate downstream
producer that the agent owns.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Producer

from ..state import AgentState

log = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0"
DELIVERY_TIMEOUT_SECONDS = float(os.getenv("ALERT_DELIVERY_TIMEOUT_SECONDS", "10"))

_producer: Producer | None = None


def _build_producer() -> Producer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    username = os.getenv("KAFKA_SASL_USERNAME", "")
    password = os.getenv("KAFKA_SASL_PASSWORD", "")
    if not username or not password:
        raise RuntimeError(
            "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD must be set"
        )
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": username,
            "sasl.password": password,
            "enable.idempotence": True,
            "linger.ms": 5,
        }
    )


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = _build_producer()
    return _producer


def _alert_topic(machine_id: str) -> str:
    return f"harmonicmesh.alerts.{machine_id.lower()}"


async def emit_alert(state: AgentState) -> AgentState:
    pm = state["pattern_match"]
    machine_id = pm["machine_id"]
    detected_at = pm["detected_at"]
    if hasattr(detected_at, "isoformat"):
        detected_at_iso = detected_at.isoformat()
    else:
        detected_at_iso = str(detected_at)

    alert = {
        "schema_version": SCHEMA_VERSION,
        "machine_id": machine_id,
        "pattern_name": pm["pattern_name"],
        "detected_at": detected_at_iso,
        "alerted_at": datetime.now(timezone.utc).isoformat(),
        "severity": pm.get("severity"),
        "alert_text": state.get("alert_text", ""),
        "reasoning": state.get("reasoning", ""),
        "confidence": float(state.get("confidence_final", 0.0)),
        "confidence_raw": float(state.get("confidence_raw", 0.0)),
        "validation_flags": list(state.get("validation_flags") or []),
        "retrieved_episode_count": len(state.get("retrieved_episodes") or []),
    }

    producer = _get_producer()
    topic = _alert_topic(machine_id)
    producer.produce(
        topic,
        value=json.dumps(alert).encode("utf-8"),
        key=machine_id.encode("utf-8"),
    )
    producer.flush(DELIVERY_TIMEOUT_SECONDS)
    log.info(
        "emit_alert: %s confidence=%.2f -> %s",
        pm["pattern_name"],
        alert["confidence"],
        topic,
    )
    return {}
