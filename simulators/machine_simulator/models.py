"""Data models for the machine simulator."""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class SensorProfile:
    """Defines the normal operating range and noise level for a single sensor.

    Normal value = uniform(low, high) + Gaussian(0, noise_std)
    """

    name: str
    low: float
    high: float
    noise_std: float

    def sample(self, rng: random.Random) -> float:
        """Sample a normal sensor reading."""
        base = rng.uniform(self.low, self.high)
        noise = rng.gauss(0.0, self.noise_std)
        return round(base + noise, 4)


@dataclass
class Machine:
    """Represents a single industrial machine in the simulation.

    Attributes:
        machine_id:    Human-readable ID, e.g. "Machine-03"
        machine_type:  Lowercase snake_case type, e.g. "rolling_mill"
        sensors:       Ordered list of SensorProfile objects
        rng:           Per-machine RNG for reproducibility
        sim_seed:      Original seed (stored for meta)
        active_faults: List of currently active FailureMode objects
        emits_heartbeat: True for Machine-04
    """

    machine_id: str
    machine_type: str
    sensors: List[SensorProfile]
    rng: random.Random
    sim_seed: int
    active_faults: List[Any] = field(default_factory=list)
    emits_heartbeat: bool = False
    _heartbeat_sequence: int = field(default=0, init=False, repr=False)
    _last_heartbeat_sim_time: Optional[datetime] = field(default=None, init=False, repr=False)

    def tick(self, sim_time: datetime, fault_elapsed_secs: float = 0.0) -> Dict[str, Any]:
        """Generate one telemetry event for the current simulation tick.

        Args:
            sim_time:           Current simulation timestamp (from SimulationClock).
            fault_elapsed_secs: Seconds elapsed since the active fault started
                                (used by failure modes to compute phase-based offsets).

        Returns:
            A dict ready for JSON serialisation.
        """
        # Sample baseline sensor readings
        sensor_values: Dict[str, float] = {
            profile.name: profile.sample(self.rng)
            for profile in self.sensors
        }

        # Determine the active injected fault name (first active fault wins for meta)
        injected_fault: Optional[str] = None
        for fault in self.active_faults:
            fault_name = fault.apply(sensor_values, fault_elapsed_secs, self.rng)
            if injected_fault is None:
                injected_fault = fault_name

        return {
            "machine_id": self.machine_id,
            "machine_type": self.machine_type,
            "event_time": sim_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "event_type": "telemetry",
            "sensors": sensor_values,
            "meta": {
                "sim_seed": self.sim_seed,
                "injected_fault": injected_fault,
            },
        }

    def maybe_heartbeat(
        self, sim_time: datetime, suppress: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Return a heartbeat event if 60 simulated seconds have passed.

        Only applicable to machines with ``emits_heartbeat = True``.
        When ``suppress`` is True the timer still advances but no event is emitted.

        Returns:
            A heartbeat dict, or None.
        """
        if not self.emits_heartbeat:
            return None

        if self._last_heartbeat_sim_time is None:
            self._last_heartbeat_sim_time = sim_time
            return None

        elapsed = (sim_time - self._last_heartbeat_sim_time).total_seconds()
        if elapsed < 60.0:
            return None

        # Advance the marker regardless of suppression
        self._last_heartbeat_sim_time = sim_time
        self._heartbeat_sequence += 1

        if suppress:
            return None

        return {
            "machine_id": self.machine_id,
            "event_time": sim_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "event_type": "heartbeat",
            "sequence": self._heartbeat_sequence,
        }
