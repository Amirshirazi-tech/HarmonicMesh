"""SimulationClock: maps wall time to compressed simulation time."""
from __future__ import annotations

import time
from datetime import datetime, timezone


class SimulationClock:
    """Single shared clock that advances simulation time at a configurable rate.

    sim_time = sim_start + elapsed_wall * compression

    Attributes:
        start: The datetime from which simulation time begins (UTC).
        compression: Ratio of simulated seconds per real second.
            1.0  => real-time
            60.0 => one simulated minute per real second
    """

    def __init__(self, start: datetime, compression: float) -> None:
        if compression <= 0:
            raise ValueError(f"compression must be positive, got {compression}")
        self._sim_start: datetime = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start
        self._compression: float = compression
        self._wall_start: float = time.monotonic()

    @property
    def sim_start(self) -> datetime:
        return self._sim_start

    @property
    def compression(self) -> float:
        return self._compression

    def now(self) -> datetime:
        """Return current simulation time (UTC)."""
        elapsed_wall = time.monotonic() - self._wall_start
        elapsed_sim_secs = elapsed_wall * self._compression
        from datetime import timedelta
        return self._sim_start + timedelta(seconds=elapsed_sim_secs)

    def sim_elapsed_seconds(self) -> float:
        """Simulated seconds since clock start."""
        elapsed_wall = time.monotonic() - self._wall_start
        return elapsed_wall * self._compression
