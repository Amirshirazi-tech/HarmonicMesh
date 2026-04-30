"""FailureScheduler: manages scheduled and random (Poisson) fault injection.

A FailureEvent binds a FailureMode to a machine, start time, and duration.
The scheduler holds the full timeline and exposes helpers used by each
machine's tick loop.
"""
from __future__ import annotations

import logging
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import yaml

from .failure_modes import FailureMode, build_failure_mode
from .models import Machine

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class FailureEvent:
    """A single scheduled occurrence of a failure mode on a machine."""

    machine_id: str
    fault: FailureMode
    sim_start: datetime       # When the fault becomes active (sim time)
    sim_end: datetime         # When the fault is deactivated (sim time)

    def is_active_at(self, sim_time: datetime) -> bool:
        return self.sim_start <= sim_time < self.sim_end

    def elapsed_seconds(self, sim_time: datetime) -> float:
        """Seconds since fault start; clamped to 0 at the boundary."""
        return max(0.0, (sim_time - self.sim_start).total_seconds())


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

class FailureScheduler:
    """Holds all failure events and mutates machine.active_faults each tick.

    Usage::

        scheduler = FailureScheduler()
        scheduler.add_scheduled(machine, fault, sim_start, duration_s)
        scheduler.add_random_from_profile(machines_by_id, profile_path, ...)
        ...
        # In tick loop:
        scheduler.update(machines_by_id, current_sim_time)
    """

    def __init__(self) -> None:
        self._events: List[FailureEvent] = []

    # ------------------------------------------------------------------
    # Building the schedule
    # ------------------------------------------------------------------

    def add_scheduled(
        self,
        machine: Machine,
        fault: FailureMode,
        sim_start: datetime,
        duration_seconds: float,
    ) -> FailureEvent:
        """Register a single scheduled failure event."""
        sim_end = sim_start + timedelta(seconds=duration_seconds)
        event = FailureEvent(
            machine_id=machine.machine_id,
            fault=fault,
            sim_start=sim_start,
            sim_end=sim_end,
        )
        self._events.append(event)
        log.info(
            "Scheduled fault '%s' on %s: %s → %s",
            fault.name,
            machine.machine_id,
            sim_start.isoformat(),
            sim_end.isoformat(),
        )
        return event

    def add_random_from_profile(
        self,
        machines_by_id: Dict[str, Machine],
        profile_path: str,
        clock_sim_start: datetime,
        clock_sim_end: datetime,
        rng: random.Random,
    ) -> None:
        """Parse a YAML profile and pre-generate Poisson-distributed events.

        Inter-arrival times are drawn from an exponential distribution:
            mean_inter_arrival = 7*24*3600 / mean_occurrences_per_simulated_week

        Jitter shifts each start time by ±jitter_pct% of the mean inter-arrival.
        """
        with open(profile_path, "r") as fh:
            profile = yaml.safe_load(fh)

        for entry in profile.get("failures", []):
            fault_type: str = entry["type"]
            machine_id: str = entry["machine"]
            mean_per_week: float = float(entry["mean_occurrences_per_simulated_week"])
            jitter_pct: float = float(entry.get("jitter_pct", 0))
            duration_secs: float = float(entry["duration_simulated_minutes"]) * 60.0

            if machine_id not in machines_by_id:
                log.warning("Profile references unknown machine '%s' — skipping", machine_id)
                continue

            machine = machines_by_id[machine_id]
            mean_inter_arrival = (7 * 24 * 3600) / mean_per_week  # seconds between events

            sim_time = clock_sim_start
            while True:
                # Exponential inter-arrival
                raw_interval = rng.expovariate(1.0 / mean_inter_arrival)
                # Apply jitter
                jitter_range = raw_interval * (jitter_pct / 100.0)
                jitter = rng.uniform(-jitter_range, jitter_range)
                interval = max(1.0, raw_interval + jitter)

                sim_time = sim_time + timedelta(seconds=interval)
                if sim_time >= clock_sim_end:
                    break

                # Clip duration so it doesn't exceed sim end
                actual_duration = min(duration_secs, (clock_sim_end - sim_time).total_seconds())
                if actual_duration <= 0:
                    break

                self.add_scheduled(machine, build_failure_mode(fault_type), sim_time, actual_duration)

    # ------------------------------------------------------------------
    # Runtime update
    # ------------------------------------------------------------------

    def update(self, machines_by_id: Dict[str, Machine], sim_time: datetime) -> None:
        """Recompute active_faults for every machine based on current sim time.

        Called once per tick, before machine.tick().
        """
        # Reset all machines' active fault list
        for machine in machines_by_id.values():
            machine.active_faults = []

        for event in self._events:
            if not event.is_active_at(sim_time):
                continue
            machine = machines_by_id.get(event.machine_id)
            if machine is None:
                continue
            machine.active_faults.append(event.fault)

    def get_fault_elapsed(self, machine_id: str, sim_time: datetime) -> float:
        """Return elapsed seconds for the first active fault on this machine."""
        for event in self._events:
            if event.machine_id == machine_id and event.is_active_at(sim_time):
                return event.elapsed_seconds(sim_time)
        return 0.0

    def summary(self) -> List[str]:
        """Return human-readable lines describing all scheduled events."""
        lines = []
        for ev in sorted(self._events, key=lambda e: (e.machine_id, e.sim_start)):
            lines.append(
                f"  {ev.machine_id}: [{ev.fault.name}] "
                f"{ev.sim_start.strftime('%Y-%m-%dT%H:%M:%SZ')} → "
                f"{ev.sim_end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            )
        return lines


# ---------------------------------------------------------------------------
# Helper: parse --at offset strings
# ---------------------------------------------------------------------------

def parse_at_offset(at_str: str, sim_start: datetime) -> datetime:
    """Convert an --at argument to an absolute sim datetime.

    Supported formats:
        +5m    — 5 simulated minutes after sim_start
        +2h    — 2 simulated hours after sim_start
        +90d   — 90 simulated days after sim_start
        ISO    — absolute datetime string (UTC assumed if no tz)
    """
    at_str = at_str.strip()
    if at_str.startswith("+"):
        suffix = at_str[1:]
        if suffix.endswith("m"):
            delta = timedelta(minutes=float(suffix[:-1]))
        elif suffix.endswith("h"):
            delta = timedelta(hours=float(suffix[:-1]))
        elif suffix.endswith("d"):
            delta = timedelta(days=float(suffix[:-1]))
        elif suffix.endswith("s"):
            delta = timedelta(seconds=float(suffix[:-1]))
        else:
            raise ValueError(f"Unrecognised offset unit in '{at_str}'. Use s/m/h/d.")
        return sim_start + delta
    else:
        # Try ISO parse
        dt = datetime.fromisoformat(at_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt


def parse_duration(duration_str: str) -> timedelta:
    """Parse CLI duration strings like '60', '30m', '2h', '90d'.

    Bare integers are treated as seconds.
    """
    s = duration_str.strip()
    if s.endswith("d"):
        return timedelta(days=float(s[:-1]))
    elif s.endswith("h"):
        return timedelta(hours=float(s[:-1]))
    elif s.endswith("m"):
        return timedelta(minutes=float(s[:-1]))
    elif s.endswith("s"):
        return timedelta(seconds=float(s[:-1]))
    else:
        return timedelta(seconds=float(s))
