"""Failure mode implementations for the machine simulator.

Each failure mode:
  - Subclasses FailureMode
  - Implements apply(sensor_values, elapsed_secs, rng) -> str
    which mutates sensor_values in-place and returns its fault name.
"""
from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import Dict


class FailureMode(ABC):
    """Abstract base class for all failure modes."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Snake_case fault identifier used in event meta."""

    @abstractmethod
    def apply(
        self,
        sensors: Dict[str, float],
        elapsed_secs: float,
        rng: random.Random,
    ) -> str:
        """Mutate *sensors* in-place to reflect this fault at *elapsed_secs*.

        Args:
            sensors:      Baseline sensor readings (already sampled).
            elapsed_secs: Seconds since this fault began.
            rng:          Per-machine RNG for reproducible noise.

        Returns:
            The fault name string (same as self.name).
        """


# ---------------------------------------------------------------------------
# ThermalVibrationCascade — Machine-03 (Rolling Mill)
# ---------------------------------------------------------------------------

class ThermalVibrationCascade(FailureMode):
    """Three-phase thermal/vibration cascade failure.

    Phase 1 (0–180 s): temperature rises linearly from 320°C baseline to 420°C.
    Phase 2 (120–300 s): vibration rises linearly from 3.0 to 5.2 mm/s.
    Phase 3 (300–600 s): current_a spikes +17% (~415A → ~485A).

    Offsets are ADDED to whatever the baseline sampler produced.
    """

    _TEMP_BASELINE = 320.0
    _TEMP_PEAK = 420.0
    _TEMP_PHASE_START = 0.0
    _TEMP_PHASE_END = 180.0

    _VIB_BASELINE = 3.0
    _VIB_PEAK = 5.2
    _VIB_PHASE_START = 120.0
    _VIB_PHASE_END = 300.0

    _CURRENT_BASELINE = 415.0
    _CURRENT_SPIKE_FACTOR = 0.17
    _CURRENT_PHASE_START = 300.0
    _CURRENT_PHASE_END = 600.0

    @property
    def name(self) -> str:
        return "thermal_vibration_cascade"

    def apply(
        self,
        sensors: Dict[str, float],
        elapsed_secs: float,
        rng: random.Random,
    ) -> str:
        # Phase 1: temperature
        if self._TEMP_PHASE_START <= elapsed_secs <= self._TEMP_PHASE_END:
            t = (elapsed_secs - self._TEMP_PHASE_START) / (
                self._TEMP_PHASE_END - self._TEMP_PHASE_START
            )
            temp_offset = (self._TEMP_PEAK - self._TEMP_BASELINE) * t
            sensors["temperature_c"] = sensors.get("temperature_c", self._TEMP_BASELINE) + temp_offset
        elif elapsed_secs > self._TEMP_PHASE_END:
            # Hold at peak with noise
            sensors["temperature_c"] = (
                self._TEMP_PEAK + rng.gauss(0.0, 3.0)
            )

        # Phase 2: vibration
        if self._VIB_PHASE_START <= elapsed_secs <= self._VIB_PHASE_END:
            t = (elapsed_secs - self._VIB_PHASE_START) / (
                self._VIB_PHASE_END - self._VIB_PHASE_START
            )
            vib_target = self._VIB_BASELINE + (self._VIB_PEAK - self._VIB_BASELINE) * t
            sensors["vibration_rms_mm_s"] = vib_target + rng.gauss(0.0, 0.1)
        elif elapsed_secs > self._VIB_PHASE_END:
            sensors["vibration_rms_mm_s"] = self._VIB_PEAK + rng.gauss(0.0, 0.2)

        # Phase 3: current spike
        if self._CURRENT_PHASE_START <= elapsed_secs <= self._CURRENT_PHASE_END:
            spike = self._CURRENT_BASELINE * self._CURRENT_SPIKE_FACTOR
            sensors["current_a"] = (
                sensors.get("current_a", self._CURRENT_BASELINE) + spike + rng.gauss(0.0, 5.0)
            )
        elif elapsed_secs > self._CURRENT_PHASE_END:
            # Hold at spike level
            sensors["current_a"] = (
                self._CURRENT_BASELINE * (1.0 + self._CURRENT_SPIKE_FACTOR)
                + rng.gauss(0.0, 5.0)
            )

        # Round all touched sensors
        for key in ("temperature_c", "vibration_rms_mm_s", "current_a"):
            if key in sensors:
                sensors[key] = round(sensors[key], 4)

        return self.name


# ---------------------------------------------------------------------------
# HeartbeatLoss — Machine-04 (Annealing Oven)
# ---------------------------------------------------------------------------

class HeartbeatLoss(FailureMode):
    """Suppresses heartbeat emission while active.

    Telemetry continues normally; heartbeat suppression is handled by the
    tick loop which checks ``machine.active_faults`` for this type.
    The apply() method just sets the meta flag — no sensor changes.
    """

    @property
    def name(self) -> str:
        return "heartbeat_loss"

    def apply(
        self,
        sensors: Dict[str, float],
        elapsed_secs: float,
        rng: random.Random,
    ) -> str:
        # No sensor modification; the tick loop reads active faults to decide
        # whether to suppress heartbeat emission.
        return self.name

    @staticmethod
    def is_active_on(machine_active_faults: list) -> bool:
        """Helper: returns True if any HeartbeatLoss is active for a machine."""
        return any(isinstance(f, HeartbeatLoss) for f in machine_active_faults)


# ---------------------------------------------------------------------------
# RefractoryDegradation — Machine-01 (Melting Furnace)
# ---------------------------------------------------------------------------

class RefractoryDegradation(FailureMode):
    """Gradual refractory wear: temperature drifts up by 0.5°C per simulated minute.

    The offset accumulates over time and is applied additively to the
    baseline temperature reading.
    """

    _DRIFT_RATE_PER_MIN = 0.5  # °C per simulated minute

    @property
    def name(self) -> str:
        return "refractory_degradation"

    def apply(
        self,
        sensors: Dict[str, float],
        elapsed_secs: float,
        rng: random.Random,
    ) -> str:
        drift = (elapsed_secs / 60.0) * self._DRIFT_RATE_PER_MIN
        sensors["temperature_c"] = round(
            sensors.get("temperature_c", 730.0) + drift, 4
        )
        return self.name


# ---------------------------------------------------------------------------
# Registry — maps YAML/CLI names to classes
# ---------------------------------------------------------------------------

FAILURE_MODE_REGISTRY: Dict[str, type] = {
    "thermal_vibration_cascade": ThermalVibrationCascade,
    "heartbeat_loss": HeartbeatLoss,
    "refractory_degradation": RefractoryDegradation,
}


def build_failure_mode(name: str) -> FailureMode:
    """Instantiate a FailureMode by its snake_case name.

    Raises:
        KeyError: if the name is not in the registry.
    """
    try:
        cls = FAILURE_MODE_REGISTRY[name]
    except KeyError:
        available = ", ".join(sorted(FAILURE_MODE_REGISTRY))
        raise KeyError(
            f"Unknown failure mode '{name}'. Available: {available}"
        ) from None
    return cls()
