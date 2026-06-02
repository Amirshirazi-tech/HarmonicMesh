"""CLI entry point for the HarmonicMesh machine simulator.

Run as:
    python -m simulators.machine_simulator [options]

Or from the simulators directory:
    python -m machine_simulator [options]
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# .env loading — try repo root first, then cwd
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv

    _here = Path(__file__).resolve().parent          # machine_simulator/
    _repo_root = _here.parent.parent                  # HarmonicMesh/
    _env_candidates = [_repo_root / ".env", Path.cwd() / ".env"]
    for _env_path in _env_candidates:
        if _env_path.exists():
            load_dotenv(dotenv_path=_env_path, override=False)
            break
except ImportError:
    pass  # python-dotenv not installed; rely on environment variables

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("machine_simulator")

# ---------------------------------------------------------------------------
# Local imports (after env loading so .env is available to sub-modules)
# ---------------------------------------------------------------------------
from .clock import SimulationClock
from .failure_modes import HeartbeatLoss, build_failure_mode
from .machines import build_all_machines
from .models import Machine
from .producer import KafkaProducerWrapper
from .scheduler import FailureScheduler, parse_at_offset, parse_duration


# ---------------------------------------------------------------------------
# Topic naming helpers
# ---------------------------------------------------------------------------

def telemetry_topic(machine_id: str) -> str:
    """e.g. 'Machine-03' → 'harmonicmesh.sensors.machine-03'"""
    return f"harmonicmesh.sensors.{machine_id.lower()}"


HEARTBEAT_TOPIC_M04 = "harmonicmesh.heartbeats.machine-04"


# ---------------------------------------------------------------------------
# Shutdown coordination
# ---------------------------------------------------------------------------

_shutdown_event = threading.Event()


def _handle_signal(signum, frame):  # noqa: ANN001
    log.info("Signal %s received — initiating graceful shutdown …", signum)
    _shutdown_event.set()


# ---------------------------------------------------------------------------
# Machine tick loop (runs in its own thread)
# ---------------------------------------------------------------------------

def _run_machine(
    machine: Machine,
    clock: SimulationClock,
    scheduler: FailureScheduler,
    producer: KafkaProducerWrapper,
    end_sim_dt: datetime,
    verbose: bool,
) -> None:
    """Tick loop for a single machine, running in a dedicated thread.

    event_time comes from clock.now(), which advances at compression × wall-clock
    rate, so --compression 1000 produces event_times 1000× faster than real time.
    Pacing sleeps 1/compression wall-seconds per tick so each machine emits
    exactly one event per simulated second.
    """
    compression = clock.compression
    topic = telemetry_topic(machine.machine_id)
    hb_topic = HEARTBEAT_TOPIC_M04 if machine.emits_heartbeat else None
    target_interval = 1.0 / compression   # real seconds per simulated second
    thread_name = threading.current_thread().name

    log.info("[%s] thread started — topic: %s", thread_name, topic)

    tick_count = 0
    while not _shutdown_event.is_set() and clock.now() < end_sim_dt:
        sim_time = clock.now()
        tick_count += 1

        tick_wall_start = time.monotonic()

        # Update active_faults for this machine only.
        scheduler.update({machine.machine_id: machine}, sim_time)
        fault_elapsed = scheduler.get_fault_elapsed(machine.machine_id, sim_time)

        # Produce telemetry
        event = machine.tick(sim_time, fault_elapsed)
        producer.send(topic, key=machine.machine_id, value=event)

        # Heartbeat (Machine-04 only)
        if hb_topic is not None:
            suppress_hb = HeartbeatLoss.is_active_on(machine.active_faults)
            hb_event = machine.maybe_heartbeat(sim_time, suppress=suppress_hb)
            if hb_event is not None:
                producer.send(hb_topic, key=machine.machine_id, value=hb_event)
                if verbose:
                    log.info("[%s] heartbeat seq=%d", machine.machine_id, hb_event["sequence"])

        # Pace to one event per simulated second at all compression levels.
        elapsed = time.monotonic() - tick_wall_start
        sleep_remaining = target_interval - elapsed
        while sleep_remaining > 0.0 and not _shutdown_event.is_set():
            chunk = min(sleep_remaining, 0.05)
            time.sleep(chunk)
            sleep_remaining -= chunk

    log.info("[%s] thread finished after %d ticks", thread_name, tick_count)



# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m simulators.machine_simulator",
        description="HarmonicMesh Phase 2 — Industrial machine telemetry simulator",
    )
    p.add_argument("--compression", type=float, default=1.0,
                   help="Time compression factor (default: 1.0 = real-time)")
    p.add_argument("--duration", type=str, default="60",
                   help="Total simulated duration to run (e.g., '30s', '90d'). "
                        "Wall-clock duration depends on the compression factor.")
    p.add_argument("--sim-start", type=str, default="2026-01-01T00:00:00Z",
                   help="Sim clock start datetime ISO (default: 2026-01-01T00:00:00Z)")
    p.add_argument("--inject", type=str, default=None,
                   help="Failure type name for scheduled injection")
    p.add_argument("--on", type=str, default=None, dest="on_machine",
                   help="Machine ID for scheduled injection (e.g. Machine-03)")
    p.add_argument("--at", type=str, default=None,
                   help="Sim time offset for injection: +5m, +2h, or ISO datetime")
    p.add_argument("--random-failures", action="store_true",
                   help="Enable profile-driven random failure injection")
    p.add_argument("--profile", type=str,
                   default=str(Path(__file__).parent / "profiles" / "warmup.yml"),
                   help="Path to YAML failure profile")
    p.add_argument("--sim-seed", type=int, default=42,
                   help="RNG seed for reproducibility (default: 42)")
    p.add_argument("--kafka-bootstrap", type=str,
                   default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9192"),
                   help="Kafka bootstrap servers (default: localhost:9192)")
    p.add_argument("--verbose", action="store_true",
                   help="Log each produced event to stdout")
    return p


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # ------------------------------------------------------------------ Clock
    sim_start_dt = datetime.fromisoformat(
        args.sim_start.replace("Z", "+00:00")
    ).replace(tzinfo=timezone.utc)

    duration_td = parse_duration(args.duration)
    end_sim_dt = sim_start_dt + duration_td

    clock = SimulationClock(start=sim_start_dt, compression=args.compression)

    # ----------------------------------------------------------------- Kafka
    kafka_bootstrap = args.kafka_bootstrap
    sasl_username = os.environ.get("KAFKA_SASL_USERNAME", "harmonicmesh")
    sasl_password = os.environ.get("KAFKA_SASL_PASSWORD")
    if not sasl_password:
        parser.error(
            "KAFKA_SASL_PASSWORD environment variable is required "
            "(copy .env.example to .env and set it)"
        )

    # ---------------------------------------------------------------- Machines
    all_machines = build_all_machines(args.sim_seed)
    machines_by_id: Dict[str, Machine] = {m.machine_id: m for m in all_machines}

    # --------------------------------------------------------------- Scheduler
    global_rng = random.Random(args.sim_seed + 9999)
    scheduler = FailureScheduler()

    # Scheduled injection via --inject / --on / --at
    if args.inject is not None:
        if args.on_machine is None or args.at is None:
            parser.error("--inject requires --on and --at")
        target_machine = machines_by_id.get(args.on_machine)
        if target_machine is None:
            parser.error(f"Unknown machine '{args.on_machine}'")
        fault = build_failure_mode(args.inject)
        fault_start = parse_at_offset(args.at, sim_start_dt)
        # Default duration: read from failure type sensible defaults
        _INJECT_DURATIONS = {
            "thermal_vibration_cascade": 15 * 60,
            "heartbeat_loss": 10 * 60,
            "refractory_degradation": 480 * 60,
        }
        duration_s = _INJECT_DURATIONS.get(args.inject, 15 * 60)
        scheduler.add_scheduled(target_machine, fault, fault_start, duration_s)

    # Random failures from profile
    if args.random_failures:
        profile_path = args.profile
        if not Path(profile_path).exists():
            log.error("Profile not found: %s", profile_path)
            return 1
        scheduler.add_random_from_profile(
            machines_by_id=machines_by_id,
            profile_path=profile_path,
            clock_sim_start=sim_start_dt,
            clock_sim_end=end_sim_dt,
            rng=global_rng,
        )

    # --------------------------------------------------------- Topic creation
    all_topics = [telemetry_topic(m.machine_id) for m in all_machines]
    all_topics.append(HEARTBEAT_TOPIC_M04)

    log.info("Connecting to Kafka at %s …", kafka_bootstrap)
    producer = KafkaProducerWrapper(
        bootstrap_servers=kafka_bootstrap,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
        verbose=args.verbose,
    )
    producer.ensure_topics(all_topics, num_partitions=3)

    # ----------------------------------------------------------- Startup banner
    print("=" * 70)
    print("  HarmonicMesh Machine Simulator — Phase 2")
    print("=" * 70)
    print(f"  Machines     : {', '.join(m.machine_id for m in all_machines)}")
    print(f"  Compression  : {args.compression}x")
    print(f"  Sim start    : {sim_start_dt.isoformat()}")
    print(f"  Sim end      : {end_sim_dt.isoformat()}")
    print(f"  Sim seed     : {args.sim_seed}")
    print(f"  Kafka        : {kafka_bootstrap}")
    wall_secs = duration_td.total_seconds() / args.compression
    print(f"  Est. wall    : ~{wall_secs:.0f} seconds ({wall_secs / 60:.1f} min)")
    scheduled = scheduler.summary()
    if scheduled:
        print("  Scheduled failures:")
        for line in scheduled:
            print(line)
    else:
        print("  Scheduled failures: none")
    print("=" * 70)

    # --------------------------------------------------------- Signal handlers
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # -------------------------------------------------- Per-machine tick threads
    threads: List[threading.Thread] = []
    for machine in all_machines:
        t = threading.Thread(
            target=_run_machine,
            args=(machine, clock, scheduler, producer, end_sim_dt, args.verbose),
            name=machine.machine_id,
            daemon=True,
        )
        threads.append(t)
        t.start()

    # Wait for all threads to finish (or shutdown signal)
    try:
        for t in threads:
            while t.is_alive():
                t.join(timeout=0.5)
                if _shutdown_event.is_set():
                    break
    except KeyboardInterrupt:
        _shutdown_event.set()

    log.info("Simulation complete — flushing producer …")
    producer.flush(timeout=30.0)
    log.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
