"""Factory functions that construct the five industrial machines."""
from __future__ import annotations

import random

from .models import Machine, SensorProfile


def _make_rng(sim_seed: int, machine_index: int) -> random.Random:
    """Create a per-machine RNG seeded deterministically from sim_seed + index."""
    return random.Random(sim_seed + machine_index)


def make_machine_01(sim_seed: int) -> Machine:
    """Machine-01 — Melting Furnace."""
    return Machine(
        machine_id="Machine-01",
        machine_type="melting_furnace",
        sensors=[
            SensorProfile("temperature_c", low=700.0, high=760.0, noise_std=3.0),
            SensorProfile("current_a", low=180.0, high=220.0, noise_std=5.0),
            SensorProfile("vibration_rms_mm_s", low=0.5, high=1.5, noise_std=0.1),
            SensorProfile("duty_cycle_pct", low=95.0, high=100.0, noise_std=0.5),
        ],
        rng=_make_rng(sim_seed, 1),
        sim_seed=sim_seed,
    )


def make_machine_02(sim_seed: int) -> Machine:
    """Machine-02 — Continuous Caster."""
    return Machine(
        machine_id="Machine-02",
        machine_type="continuous_caster",
        sensors=[
            SensorProfile("temperature_c", low=680.0, high=720.0, noise_std=2.0),
            SensorProfile("coolant_flow_l_min", low=40.0, high=60.0, noise_std=1.5),
            SensorProfile("vibration_rms_mm_s", low=1.0, high=2.0, noise_std=0.2),
            SensorProfile("duty_cycle_pct", low=85.0, high=95.0, noise_std=1.0),
        ],
        rng=_make_rng(sim_seed, 2),
        sim_seed=sim_seed,
    )


def make_machine_03(sim_seed: int) -> Machine:
    """Machine-03 — Rolling Mill (primary demo machine)."""
    return Machine(
        machine_id="Machine-03",
        machine_type="rolling_mill",
        sensors=[
            SensorProfile("temperature_c", low=250.0, high=350.0, noise_std=5.0),
            SensorProfile("vibration_rms_mm_s", low=2.0, high=3.5, noise_std=0.3),
            SensorProfile("current_a", low=380.0, high=450.0, noise_std=8.0),
            SensorProfile("duty_cycle_pct", low=80.0, high=92.0, noise_std=1.5),
        ],
        rng=_make_rng(sim_seed, 3),
        sim_seed=sim_seed,
    )


def make_machine_04(sim_seed: int) -> Machine:
    """Machine-04 — Annealing Oven (also emits heartbeats)."""
    return Machine(
        machine_id="Machine-04",
        machine_type="annealing_oven",
        sensors=[
            SensorProfile("temperature_c", low=340.0, high=380.0, noise_std=2.0),
            SensorProfile("conveyor_speed_m_min", low=1.8, high=2.2, noise_std=0.05),
            SensorProfile("duty_cycle_pct", low=85.0, high=95.0, noise_std=1.0),
            SensorProfile("vibration_rms_mm_s", low=0.3, high=0.8, noise_std=0.05),
        ],
        rng=_make_rng(sim_seed, 4),
        sim_seed=sim_seed,
        emits_heartbeat=True,
    )


def make_machine_05(sim_seed: int) -> Machine:
    """Machine-05 — Finishing Line."""
    return Machine(
        machine_id="Machine-05",
        machine_type="finishing_line",
        sensors=[
            SensorProfile("temperature_c", low=25.0, high=45.0, noise_std=1.0),
            SensorProfile("vibration_rms_mm_s", low=1.5, high=2.5, noise_std=0.2),
            SensorProfile("cutting_force_kn", low=2.0, high=4.0, noise_std=0.15),
            SensorProfile("duty_cycle_pct", low=70.0, high=85.0, noise_std=2.0),
        ],
        rng=_make_rng(sim_seed, 5),
        sim_seed=sim_seed,
    )


def build_all_machines(sim_seed: int) -> list[Machine]:
    """Return all five machines in ID order."""
    return [
        make_machine_01(sim_seed),
        make_machine_02(sim_seed),
        make_machine_03(sim_seed),
        make_machine_04(sim_seed),
        make_machine_05(sim_seed),
    ]
