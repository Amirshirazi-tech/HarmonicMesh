"""Tests for the Thermal-Vibration Cascade Phase 3 CEP job.

Each test feeds a hand-built sequence of telemetry rows into a real PyFlink
mini-cluster via ``from_collection`` (a bounded source). The same
``attach_cascade_pipeline`` used in production drives the test, so the
pattern definition, watermark strategy, and selector are exercised
end-to-end — no re-implementation in Python.

The bounded source matters: when ``from_collection`` finishes, its
watermark advances to MAX, which forces CEP to flush any pending matches
or timeouts. That's how the within-boundary tests (cases 3 and 4) get
deterministic results.

Run:
    pytest tests/flink/test_thermal_vibration_cascade.py

PyFlink starts a JVM per test; expect ~10–20 s wall time per case.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pytest

# ---------------------------------------------------------------------------
# Make the job module importable without packaging the flink_jobs tree.
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parents[2]
_JOB_DIR = _REPO_ROOT / "flink_jobs" / "python"
if str(_JOB_DIR) not in sys.path:
    sys.path.insert(0, str(_JOB_DIR))

from pyflink.common import Row, Types  # noqa: E402
from pyflink.datastream import StreamExecutionEnvironment  # noqa: E402

from thermal_vibration_cascade import (  # noqa: E402
    EVENT_ROW_TYPE,
    attach_cascade_pipeline,
    load_baselines,
)

# ---------------------------------------------------------------------------
# Machine-03 baselines, loaded from the real YAML so tests and production
# stay in lockstep with the threshold mapping.
# ---------------------------------------------------------------------------
M3 = load_baselines()["Machine-03"]
BASE_TEMP = M3["baseline_temperature_c"]               # 320.0
BASE_CUR = M3["baseline_current_a"]                    # 415.0
TEMP_THRESHOLD = BASE_TEMP + M3["cascade_temperature_offset_c"]   # 380.0
VIB_THRESHOLD = M3["cascade_vibration_threshold_mm_s"]            # 4.5
CUR_DEV = M3["cascade_current_deviation_pct"]                     # 0.15
CUR_HIGH = BASE_CUR * (1.0 + CUR_DEV) + 5.0            # comfortably above
VIB_BASELINE = 3.0


def _ms_to_iso(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _event(
    machine_id: str,
    ts_ms: int,
    *,
    temp: float = BASE_TEMP,
    vib: float = VIB_BASELINE,
    cur: float = BASE_CUR,
) -> Row:
    """Build one row whose field order matches EVENT_ROW_TYPE exactly."""
    iso = _ms_to_iso(ts_ms)
    raw = json.dumps({
        "machine_id": machine_id,
        "event_time": iso,
        "event_type": "telemetry",
        "sensors": {
            "temperature_c": float(temp),
            "vibration_rms_mm_s": float(vib),
            "current_a": float(cur),
        },
    })
    return Row(
        machine_id,
        int(ts_ms),
        iso,
        float(temp),
        float(vib),
        float(cur),
        raw,
    )


def _run_pipeline(events: List[Row]) -> List[dict]:
    """Run the cascade pipeline on a fixed event list, return parsed matches."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    parsed = env.from_collection(events, type_info=EVENT_ROW_TYPE)
    matches = attach_cascade_pipeline(parsed, M3)
    return [json.loads(m) for m in matches.execute_and_collect()]


# ---------------------------------------------------------------------------
# Time helpers (event-time, in ms-since-epoch, anchored to a fixed start so
# all tests use the same readable timeline).
# ---------------------------------------------------------------------------
T0_MS = 1_767_225_600_000  # 2026-01-01T00:00:00.000Z


def at(seconds: float) -> int:
    return T0_MS + int(seconds * 1000)


# ===========================================================================
# Test 1 — Happy path
# ===========================================================================
def test_happy_path_emits_one_match():
    events = [
        _event("Machine-03", at(0),   temp=TEMP_THRESHOLD + 20),  # temp_anomaly
        _event("Machine-03", at(60),  vib=VIB_THRESHOLD + 0.5),   # vib_anomaly
        _event("Machine-03", at(120), cur=CUR_HIGH),              # current_anomaly
    ]
    matches = _run_pipeline(events)

    assert len(matches) == 1, f"expected 1 match, got {len(matches)}"
    m = matches[0]
    assert m["pattern_name"] == "ThermalVibrationCascade"
    assert m["machine_id"] == "Machine-03"
    assert m["severity"] == "CRITICAL"
    assert m["schema_version"] == "1.0"
    assert len(m["source_events"]) == 3
    # detected_at must equal the third (current) event's event_time
    assert m["detected_at"] == _ms_to_iso(at(120))
    # All three source events captured in order
    assert m["source_events"][0]["sensors"]["temperature_c"] > TEMP_THRESHOLD
    assert m["source_events"][1]["sensors"]["vibration_rms_mm_s"] > VIB_THRESHOLD
    assert m["source_events"][2]["sensors"]["current_a"] > BASE_CUR * (1 + CUR_DEV)


# ===========================================================================
# Test 2 — Out of order in event time (vibration earlier than temperature)
# ===========================================================================
def test_out_of_order_in_event_time_does_not_match():
    # Vibration's event_time is BEFORE temperature's event_time. Even though
    # the records can arrive in any order on the wire, Flink CEP processes
    # in event-time order, and the pattern requires temp BEFORE vib.
    events = [
        _event("Machine-03", at(30),  temp=TEMP_THRESHOLD + 20),  # temp at +30s
        _event("Machine-03", at(0),   vib=VIB_THRESHOLD + 0.5),   # vib at +0s
        _event("Machine-03", at(60),  cur=CUR_HIGH),              # cur at +60s
    ]
    matches = _run_pipeline(events)
    assert matches == [], f"expected 0 matches, got {matches}"


# ===========================================================================
# Test 3 — Just inside the within boundary
# ===========================================================================
def test_third_event_just_inside_within_boundary_matches():
    # Third event at 00:09:59 (599 s) relative to the first event at 00:00:00.
    # Gap is 599 s < 600 s window → match.
    events = [
        _event("Machine-03", at(0),   temp=TEMP_THRESHOLD + 20),
        _event("Machine-03", at(100), vib=VIB_THRESHOLD + 0.5),
        _event("Machine-03", at(599), cur=CUR_HIGH),
    ]
    matches = _run_pipeline(events)
    assert len(matches) == 1
    assert matches[0]["detected_at"] == _ms_to_iso(at(599))


# ===========================================================================
# Test 4 — Just outside the within boundary
# ===========================================================================
def test_third_event_just_outside_within_boundary_does_not_match():
    # Third event at 00:10:01 (601 s). Gap 601 s > 600 s window → no match.
    # The deadline is determined by within(Time.minutes(10)), not by the
    # watermark — which is why a 5 s out-of-orderness budget here cannot
    # "rescue" the match. The pending partial match times out on
    # end-of-stream watermark and is discarded.
    events = [
        _event("Machine-03", at(0),   temp=TEMP_THRESHOLD + 20),
        _event("Machine-03", at(100), vib=VIB_THRESHOLD + 0.5),
        _event("Machine-03", at(601), cur=CUR_HIGH),
    ]
    matches = _run_pipeline(events)
    assert matches == [], f"expected 0 matches, got {matches}"


# ===========================================================================
# Test 5 — Different machines (per-machine keying isolates state)
# ===========================================================================
def test_events_split_across_machines_does_not_match():
    # Temperature on Machine-03, vibration on Machine-04. After key_by(
    # machine_id), each machine has its own NFA; neither sees the full
    # cascade.
    events = [
        _event("Machine-03", at(0),   temp=TEMP_THRESHOLD + 20),
        _event("Machine-04", at(60),  vib=VIB_THRESHOLD + 0.5),
        _event("Machine-03", at(120), cur=CUR_HIGH),
    ]
    matches = _run_pipeline(events)
    assert matches == [], f"expected 0 matches, got {matches}"


# ===========================================================================
# Test 6 — Threshold edge (strict ``>`` is documented)
# ===========================================================================
def test_temperature_exactly_at_threshold_does_not_match():
    # The TemperatureAnomaly condition uses strict ``>`` (not ``>=``). At
    # temp_c exactly equal to baseline + offset (380.0 here), the predicate
    # is False and the pattern never enters its first state.
    # See docs/patterns.md for the operator-mapping table.
    events = [
        _event("Machine-03", at(0),   temp=TEMP_THRESHOLD),       # exactly 380.0
        _event("Machine-03", at(60),  vib=VIB_THRESHOLD + 0.5),
        _event("Machine-03", at(120), cur=CUR_HIGH),
    ]
    matches = _run_pipeline(events)
    assert matches == [], (
        "TemperatureAnomaly uses strict `>`; equality at threshold must not "
        "trigger the cascade. If this fails, the operator was changed to `>=` "
        "without updating docs/patterns.md."
    )
