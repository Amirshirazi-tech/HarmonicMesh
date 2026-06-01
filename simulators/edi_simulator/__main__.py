"""CLI entry point for the HarmonicMesh EDI event simulator.

Produces synthetic EDI order/shipment/invoice events to
``harmonicmesh.edi.events``.  Events are linked by ``order_id`` and carry
simulated event-time stamps advanced by the same SimulationClock used in the
machine simulator.

Default violation injection (~3 per simulated hour):
  - order_skip_prob=0.10  → ~1 shipment_without_order per hour
  - shipment_skip_prob=0.10 → ~1 order_unfulfilled + ~0.95 invoice_without_shipment per hour

Run:
    python -m simulators.edi_simulator [options]
    python -m simulators.edi_simulator --help
"""
from __future__ import annotations

import argparse
import heapq
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    _here = Path(__file__).resolve().parent
    _repo_root = _here.parent.parent
    for _env_path in [_repo_root / ".env", Path.cwd() / ".env"]:
        if _env_path.exists():
            load_dotenv(dotenv_path=_env_path, override=False)
            break
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("edi_simulator")

from ..machine_simulator.clock import SimulationClock
from ..machine_simulator.producer import KafkaProducerWrapper
from ..machine_simulator.scheduler import parse_duration

EDI_EVENTS_TOPIC = "harmonicmesh.edi.events"

_PARTNERS = ["SUPPLIER-A", "SUPPLIER-B", "SUPPLIER-C", "SUPPLIER-D"]
_CARRIERS = ["FedEx", "UPS", "DHL", "USPS"]

_shutdown_event = __import__("threading").Event()


# ---------------------------------------------------------------------------
# Event generation (pure functions — importable for tests)
# ---------------------------------------------------------------------------

def make_order_payload(rng: random.Random) -> dict:
    return {
        "partner_code": rng.choice(_PARTNERS),
        "line_items": rng.randint(1, 20),
        "total_value_usd": round(rng.uniform(500.0, 50000.0), 2),
    }


def make_shipment_payload(rng: random.Random, expected_delivery_dt: datetime) -> dict:
    return {
        "tracking_id": f"TRK-{rng.randint(100000, 999999)}",
        "carrier": rng.choice(_CARRIERS),
        "expected_delivery": expected_delivery_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    }


def make_invoice_payload(rng: random.Random, due_dt: datetime) -> dict:
    return {
        "invoice_number": f"INV-{rng.randint(10000, 99999)}",
        "amount_usd": round(rng.uniform(500.0, 50000.0), 2),
        "due_date": due_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    }


def make_edi_event(
    event_type: str,
    order_id: str,
    sim_time: datetime,
    rng: random.Random,
) -> dict:
    """Generate one EDI event dict ready for Kafka serialisation.

    ``payload`` is JSON-encoded as a string so that the Flink SQL DDL can
    declare the column as STRING and read it without a nested-type declaration.
    """
    if event_type == "order":
        payload = make_order_payload(rng)
    elif event_type == "shipment":
        payload = make_shipment_payload(
            rng, sim_time + timedelta(days=rng.randint(2, 7))
        )
    else:
        payload = make_invoice_payload(
            rng, sim_time + timedelta(days=30)
        )

    return {
        "event_id": f"edi-{uuid.uuid4()}",
        "event_type": event_type,
        "order_id": order_id,
        "event_time": sim_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        # payload is stored as a JSON string so Flink SQL can read it as STRING
        "payload": json.dumps(payload),
    }


def plan_transaction(
    order_id: str,
    start_sim_time: datetime,
    rng: random.Random,
    order_skip_prob: float,
    shipment_skip_prob: float,
    invoice_skip_prob: float,
) -> List[Tuple[datetime, str, str]]:
    """Return a list of (emit_sim_time, event_type, order_id) for one transaction.

    Violations are injected by skipping events with the given probabilities:
      - skip order  → shipment_without_order (if shipment is still emitted)
      - skip shipment → order_unfulfilled + invoice_without_shipment
      - skip invoice  → incomplete flow (no EDI violation per spec)
    """
    emit_order = rng.random() >= order_skip_prob
    emit_shipment = rng.random() >= shipment_skip_prob
    emit_invoice = rng.random() >= invoice_skip_prob

    # Delays between events (in simulated seconds)
    ship_delay = timedelta(seconds=rng.uniform(300, 1800))    # 5–30 min after order
    inv_delay  = timedelta(seconds=rng.uniform(600, 3600))    # 10–60 min after shipment

    order_time    = start_sim_time
    shipment_time = order_time + ship_delay
    invoice_time  = shipment_time + inv_delay

    events: List[Tuple[datetime, str, str]] = []
    if emit_order:
        events.append((order_time, "order", order_id))
    if emit_shipment:
        events.append((shipment_time, "shipment", order_id))
    if emit_invoice:
        events.append((invoice_time, "invoice", order_id))
    return events


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m simulators.edi_simulator",
        description="HarmonicMesh Phase 6 — EDI event simulator",
    )
    p.add_argument("--compression", type=float, default=1.0,
                   help="Time compression factor (default: 1.0 = real-time)")
    p.add_argument("--duration", type=str, default="60",
                   help="Total simulated duration to run (e.g., '30s', '90d')")
    p.add_argument("--sim-start", type=str, default="2026-01-01T00:00:00Z",
                   help="Sim clock start datetime ISO (default: 2026-01-01T00:00:00Z)")
    p.add_argument("--kafka-bootstrap", type=str,
                   default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9192"),
                   help="Kafka bootstrap servers (default: localhost:9192)")
    p.add_argument("--transactions-per-hour", type=float, default=10.0,
                   help="Simulated EDI transactions per simulated hour (default: 10)")
    p.add_argument("--order-skip-prob", type=float, default=0.10,
                   help="Probability of skipping the order event (→ shipment_without_order)")
    p.add_argument("--shipment-skip-prob", type=float, default=0.10,
                   help="Probability of skipping the shipment event (→ order_unfulfilled)")
    p.add_argument("--invoice-skip-prob", type=float, default=0.05,
                   help="Probability of skipping the invoice event")
    p.add_argument("--sim-seed", type=int, default=42,
                   help="RNG seed for reproducibility (default: 42)")
    p.add_argument("--verbose", action="store_true",
                   help="Log each produced event to stdout")
    return p


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    sim_start_dt = datetime.fromisoformat(
        args.sim_start.replace("Z", "+00:00")
    ).replace(tzinfo=timezone.utc)

    duration_td = parse_duration(args.duration)
    end_sim_dt = sim_start_dt + duration_td

    clock = SimulationClock(start=sim_start_dt, compression=args.compression)

    kafka_bootstrap = args.kafka_bootstrap
    sasl_username = os.environ.get("KAFKA_SASL_USERNAME", "harmonicmesh")
    sasl_password = os.environ.get("KAFKA_SASL_PASSWORD", "HmSvc2026R4mL8jTv")

    rng = random.Random(args.sim_seed)

    log.info("Connecting to Kafka at %s …", kafka_bootstrap)
    producer = KafkaProducerWrapper(
        bootstrap_servers=kafka_bootstrap,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
        verbose=args.verbose,
    )
    producer.ensure_topics([EDI_EVENTS_TOPIC], num_partitions=3)

    print("=" * 70)
    print("  HarmonicMesh EDI Simulator — Phase 6")
    print("=" * 70)
    print(f"  Compression  : {args.compression}x")
    print(f"  Sim start    : {sim_start_dt.isoformat()}")
    print(f"  Sim end      : {end_sim_dt.isoformat()}")
    print(f"  Txns/hr      : {args.transactions_per_hour:.1f}")
    print(f"  Order skip   : {args.order_skip_prob:.0%}")
    print(f"  Shipment skip: {args.shipment_skip_prob:.0%}")
    print(f"  Invoice skip : {args.invoice_skip_prob:.0%}")
    print(f"  Kafka        : {kafka_bootstrap}")
    print("=" * 70)

    signal.signal(signal.SIGINT,  lambda s, f: _shutdown_event.set())
    signal.signal(signal.SIGTERM, lambda s, f: _shutdown_event.set())

    # Priority queue: (emit_sim_time, counter, event_dict)
    # Counter breaks ties so dict comparison is never needed.
    pending: list = []
    counter = 0

    # Schedule first transaction at sim_start
    txn_interval = timedelta(seconds=3600.0 / args.transactions_per_hour)
    next_txn_time = sim_start_dt

    events_emitted = 0

    while not _shutdown_event.is_set() and clock.now() < end_sim_dt:
        sim_now = clock.now()

        # Schedule all transactions whose start time has been reached
        while next_txn_time <= sim_now and next_txn_time < end_sim_dt:
            order_id = f"order-{uuid.uuid4()}"
            planned = plan_transaction(
                order_id, next_txn_time, rng,
                args.order_skip_prob, args.shipment_skip_prob, args.invoice_skip_prob,
            )
            for emit_dt, ev_type, oid in planned:
                evt = make_edi_event(ev_type, oid, emit_dt, rng)
                heapq.heappush(pending, (emit_dt, counter, evt))
                counter += 1
            next_txn_time += txn_interval

        # Emit all events whose time has arrived
        while pending and pending[0][0] <= sim_now:
            _, _, evt = heapq.heappop(pending)
            producer.send(EDI_EVENTS_TOPIC, key=evt["order_id"], value=evt)
            events_emitted += 1

        time.sleep(0.05)  # 50ms wall-clock polling interval

    log.info("Simulation complete — %d events emitted. Flushing …", events_emitted)
    producer.flush(timeout=30.0)
    log.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
