"""Thermal-Vibration Cascade detector — Phase 3.

PyFlink CEP job that consumes Machine-03 telemetry from
``harmonicmesh.sensors.machine-03``, detects a three-step cascade in event
time, and publishes pattern matches to ``harmonicmesh.patterns.machine-03``.

Pattern (relaxed contiguity, all transitions are ``followed_by``):

    temp_anomaly        temperature_c   > baseline + 60
    --> vib_anomaly     vibration_rms   > 4.5 absolute
    --> current_anomaly |current - baseline| / baseline > 0.15
    within(Time.minutes(10))

Event-time semantics throughout. Watermark strategy:
    bounded out-of-orderness 5 s, with_idleness 30 s.

Stream is keyed by ``machine_id`` so per-machine state is isolated.

The job is deliberately split into small importable functions so the test
suite (`tests/flink/test_thermal_vibration_cascade.py`) can drive the same
pattern definition against a `from_collection` source.

Run inside the Flink container, or locally with PyFlink installed:

    python flink_jobs/python/thermal_vibration_cascade.py \\
        --bootstrap kafka:29092 \\
        --baselines flink_jobs/python/config/machine_baselines.yaml
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pyflink.cep import (
    CEP,
    AfterMatchSkipStrategy,
    IterativeCondition,
    Pattern,
    PatternSelectFunction,
)
from pyflink.common import Duration, Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import MapFunction

log = logging.getLogger(__name__)

PATTERN_NAME = "ThermalVibrationCascade"
SCHEMA_VERSION = "1.0"
DEFAULT_GROUP_ID = "harmonicmesh-cep-thermal-vibration"
DEFAULT_SOURCE_TOPIC = "harmonicmesh.sensors.machine-03"
DEFAULT_SINK_TOPIC = "harmonicmesh.patterns.machine-03"

# ---------------------------------------------------------------------------
# Row type used by the CEP pipeline.
#
# A few derived fields ride alongside the raw JSON so conditions and the
# select function don't need to re-parse on every event:
#   event_time_ms — millisecond epoch for the watermark/timestamp assigner
#   raw_json      — the original event payload, re-emitted in the match output
# ---------------------------------------------------------------------------
EVENT_ROW_TYPE = Types.ROW_NAMED(
    [
        "machine_id",
        "event_time_ms",
        "event_time_iso",
        "temperature_c",
        "vibration_rms_mm_s",
        "current_a",
        "raw_json",
    ],
    [
        Types.STRING(),
        Types.LONG(),
        Types.STRING(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.STRING(),
    ],
)


# ---------------------------------------------------------------------------
# Baselines
# ---------------------------------------------------------------------------

DEFAULT_BASELINES_PATH = Path(__file__).parent / "config" / "machine_baselines.yaml"


def load_baselines(path: Optional[Path] = None) -> Dict[str, Dict[str, float]]:
    """Load static per-machine baselines from YAML.

    Baselines are static-by-design; runtime learning is v2 scope.
    """
    path = Path(path) if path is not None else DEFAULT_BASELINES_PATH
    with open(path) as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping at top level of {path}")
    return data


# ---------------------------------------------------------------------------
# JSON parse → Row
# ---------------------------------------------------------------------------

def _iso_to_millis(iso_string: str) -> int:
    """Parse an ISO-8601 'Z' timestamp into milliseconds since epoch."""
    dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


class ParseTelemetry(MapFunction):
    """Map a raw JSON string into the EVENT_ROW_TYPE Row.

    Malformed events return a sentinel row with event_time_ms = 0 and all
    sensor values at NaN-equivalent (-1.0) so downstream conditions reject
    them. We don't drop them silently here because that would mask a real
    upstream bug — the sentinel will surface if anything ever flows through.
    """

    def map(self, value: str) -> Row:
        try:
            evt = json.loads(value)
            sensors = evt.get("sensors", {}) or {}
            return Row(
                machine_id=str(evt.get("machine_id", "")),
                event_time_ms=_iso_to_millis(evt["event_time"]),
                event_time_iso=str(evt["event_time"]),
                temperature_c=float(sensors.get("temperature_c", -1.0)),
                vibration_rms_mm_s=float(sensors.get("vibration_rms_mm_s", -1.0)),
                current_a=float(sensors.get("current_a", -1.0)),
                raw_json=value,
            )
        except Exception as exc:  # noqa: BLE001
            log.error("Failed to parse telemetry event: %s | payload=%s", exc, value)
            return Row(
                machine_id="",
                event_time_ms=0,
                event_time_iso="",
                temperature_c=-1.0,
                vibration_rms_mm_s=-1.0,
                current_a=-1.0,
                raw_json=value,
            )


# ---------------------------------------------------------------------------
# Watermark / timestamp assignment
# ---------------------------------------------------------------------------

class EventTimeAssigner(TimestampAssigner):
    """Pull the timestamp from the parsed event_time_ms field."""

    def extract_timestamp(self, value: Row, record_timestamp: int) -> int:
        return value["event_time_ms"]


def make_watermark_strategy() -> WatermarkStrategy:
    """5 s bounded out-of-orderness, 30 s idleness, event-time assigner.

    The 5 s OOO budget is generous for a synthetic stream where producer
    threads can briefly desync under high compression; it is small enough
    that real cascades (which span minutes) are never delayed perceptibly.

    with_idleness(30 s) lets the partitions of an idle Machine-03 stream
    not block CEP timeout firing — important for the .within(10m) deadline.
    """
    return (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_idleness(Duration.of_seconds(30))
        .with_timestamp_assigner(EventTimeAssigner())
    )


# ---------------------------------------------------------------------------
# Conditions
#
# All three are top-level classes (not closures) so they pickle cleanly when
# Flink ships them to the TaskManager. Constructor params are simple floats
# resolved from the YAML baselines at job startup.
# ---------------------------------------------------------------------------

class TemperatureAnomaly(IterativeCondition):
    """Strict greater-than: temperature_c > baseline + offset.

    Strict ``>`` (not ``>=``) — equality at the threshold is treated as
    not-yet-anomalous. Documented in docs/patterns.md and asserted by the
    threshold-edge test case.
    """

    def __init__(self, baseline_c: float, offset_c: float):
        self._threshold = float(baseline_c) + float(offset_c)

    def filter(self, value: Row, ctx) -> bool:
        return value["temperature_c"] > self._threshold


class VibrationAnomaly(IterativeCondition):
    """Strict greater-than: vibration_rms_mm_s > absolute threshold."""

    def __init__(self, threshold_mm_s: float):
        self._threshold = float(threshold_mm_s)

    def filter(self, value: Row, ctx) -> bool:
        return value["vibration_rms_mm_s"] > self._threshold


class CurrentAnomaly(IterativeCondition):
    """Symmetric deviation: |current - baseline| / baseline > deviation_pct.

    Captures both spikes and dips; the cascade only produces spikes today
    (per failure_modes.ThermalVibrationCascade) but symmetric form keeps
    the predicate honest to the spec.
    """

    def __init__(self, baseline_a: float, deviation_pct: float):
        self._baseline = float(baseline_a)
        self._deviation = float(deviation_pct)

    def filter(self, value: Row, ctx) -> bool:
        if self._baseline == 0.0:
            return False
        return abs(value["current_a"] - self._baseline) / self._baseline > self._deviation


# ---------------------------------------------------------------------------
# Pattern definition
# ---------------------------------------------------------------------------

def build_pattern(machine_baselines: Dict[str, float]) -> Pattern:
    """Construct the cascade pattern using Machine-03 baselines.

    Both transitions are ``.followed_by(...)`` (relaxed contiguity) — other
    events between cascade steps are tolerated, which matches the simulator
    where every tick emits all three sensors and we only react to the ones
    crossing the threshold.

    ``AfterMatchSkipStrategy.skip_past_last_event()`` ensures one cascade
    fires one match: once the cur_anomaly closes a match, all candidate
    partial matches whose events are at or before that point are discarded.
    """
    return (
        Pattern
        .begin("temp_anomaly", AfterMatchSkipStrategy.skip_past_last_event())
        .where(TemperatureAnomaly(
            baseline_c=machine_baselines["baseline_temperature_c"],
            offset_c=machine_baselines["cascade_temperature_offset_c"],
        ))
        .followed_by("vib_anomaly")
        .where(VibrationAnomaly(
            threshold_mm_s=machine_baselines["cascade_vibration_threshold_mm_s"],
        ))
        .followed_by("current_anomaly")
        .where(CurrentAnomaly(
            baseline_a=machine_baselines["baseline_current_a"],
            deviation_pct=machine_baselines["cascade_current_deviation_pct"],
        ))
        .within(Time.minutes(10))
    )


# ---------------------------------------------------------------------------
# Match → output JSON
# ---------------------------------------------------------------------------

def _extract_first(events: Any) -> Row:
    """PyFlink hands the select function a dict[str, list[Row]]; each list
    has one element when the pattern step has no quantifier."""
    if isinstance(events, list):
        return events[0]
    return events


def _row_to_event_dict(row: Row) -> Dict[str, Any]:
    """Re-hydrate the original telemetry JSON if available, else build one."""
    if row["raw_json"]:
        try:
            return json.loads(row["raw_json"])
        except (TypeError, ValueError):
            pass
    return {
        "machine_id": row["machine_id"],
        "event_time": row["event_time_iso"],
        "sensors": {
            "temperature_c": row["temperature_c"],
            "vibration_rms_mm_s": row["vibration_rms_mm_s"],
            "current_a": row["current_a"],
        },
    }


class CascadeSelector(PatternSelectFunction):
    """Build the pattern-match JSON payload.

    Output schema (v1.0):
        {
          "schema_version": "1.0",
          "pattern_name": "ThermalVibrationCascade",
          "machine_id": <str>,
          "detected_at": <ISO event_time of the third event>,
          "severity": "CRITICAL",
          "source_events": [<temp event>, <vib event>, <cur event>]
        }

    severity is hard-coded CRITICAL in v1 because the cascade itself is the
    bearing-failure precursor; tiered severity (CRITICAL/HIGH/etc.) requires
    historical context and belongs in the agent layer (Phase 5), not in CEP.
    """

    def select(self, pattern_match: Dict[str, Any]) -> str:
        temp_row = _extract_first(pattern_match["temp_anomaly"])
        vib_row = _extract_first(pattern_match["vib_anomaly"])
        cur_row = _extract_first(pattern_match["current_anomaly"])

        payload = {
            "schema_version": SCHEMA_VERSION,
            "pattern_name": PATTERN_NAME,
            "machine_id": cur_row["machine_id"],
            "detected_at": cur_row["event_time_iso"],
            "severity": "CRITICAL",
            "source_events": [
                _row_to_event_dict(temp_row),
                _row_to_event_dict(vib_row),
                _row_to_event_dict(cur_row),
            ],
        }
        return json.dumps(payload, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Kafka source / sink
# ---------------------------------------------------------------------------

def _sasl_jaas_config(user: str, password: str) -> str:
    return (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{user}" password="{password}";'
    )


def build_kafka_source(
    bootstrap: str,
    topic: str,
    group_id: str,
    sasl_user: str,
    sasl_password: str,
    starting_offsets: str = "latest",
) -> KafkaSource:
    """Kafka source using SimpleStringSchema (parse JSON downstream)."""
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_value_only_deserializer(SimpleStringSchema())
        .set_property("security.protocol", "SASL_PLAINTEXT")
        .set_property("sasl.mechanism", "PLAIN")
        .set_property("sasl.jaas.config", _sasl_jaas_config(sasl_user, sasl_password))
    )
    if starting_offsets == "earliest":
        builder = builder.set_starting_offsets(KafkaOffsetsInitializer.earliest())
    else:
        builder = builder.set_starting_offsets(KafkaOffsetsInitializer.latest())
    return builder.build()


def build_kafka_sink(
    bootstrap: str,
    topic: str,
    sasl_user: str,
    sasl_password: str,
) -> KafkaSink:
    """Kafka sink emitting JSON strings via SimpleStringSchema."""
    record_serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap)
        .set_record_serializer(record_serializer)
        .set_property("security.protocol", "SASL_PLAINTEXT")
        .set_property("sasl.mechanism", "PLAIN")
        .set_property("sasl.jaas.config", _sasl_jaas_config(sasl_user, sasl_password))
        .build()
    )


# ---------------------------------------------------------------------------
# Pipeline assembly (importable for tests)
# ---------------------------------------------------------------------------

def attach_cascade_pipeline(
    parsed_stream,
    machine_baselines: Dict[str, float],
):
    """Attach the cascade pipeline to an already-parsed Row stream.

    Steps:
        1. assign event-time watermarks
        2. key by machine_id
        3. apply CEP pattern
        4. emit JSON match strings

    Used by main() and by the test suite. The caller decides where the Row
    stream comes from (Kafka in main, from_collection in tests).
    """
    timestamped = parsed_stream.assign_timestamps_and_watermarks(make_watermark_strategy())
    keyed = timestamped.key_by(lambda row: row["machine_id"], key_type=Types.STRING())
    pattern = build_pattern(machine_baselines)
    return CEP.pattern(keyed, pattern).select(CascadeSelector(), output_type=Types.STRING())


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="thermal_vibration_cascade",
        description="HarmonicMesh Phase 3 — Thermal-Vibration Cascade CEP job",
    )
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
    p.add_argument("--source-topic", default=DEFAULT_SOURCE_TOPIC)
    p.add_argument("--sink-topic", default=DEFAULT_SINK_TOPIC)
    p.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    p.add_argument("--baselines", type=Path, default=DEFAULT_BASELINES_PATH)
    p.add_argument("--machine-id", default="Machine-03",
                   help="Key into the baselines YAML (default: Machine-03)")
    p.add_argument(
        "--starting-offsets", choices=["earliest", "latest"], default="latest",
    )
    p.add_argument("--parallelism", type=int, default=1)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    args = _build_arg_parser().parse_args(argv)

    sasl_user = os.environ["KAFKA_SASL_USERNAME"]
    sasl_password = os.environ["KAFKA_SASL_PASSWORD"]

    baselines = load_baselines(args.baselines)
    if args.machine_id not in baselines:
        raise SystemExit(
            f"machine_id={args.machine_id} not found in {args.baselines}; "
            f"available: {sorted(baselines)}"
        )
    machine_baselines = baselines[args.machine_id]

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    source = build_kafka_source(
        bootstrap=args.bootstrap,
        topic=args.source_topic,
        group_id=args.group_id,
        sasl_user=sasl_user,
        sasl_password=sasl_password,
        starting_offsets=args.starting_offsets,
    )
    sink = build_kafka_sink(
        bootstrap=args.bootstrap,
        topic=args.sink_topic,
        sasl_user=sasl_user,
        sasl_password=sasl_password,
    )

    raw_stream = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name=f"kafka:{args.source_topic}",
    )
    parsed_stream = raw_stream.map(ParseTelemetry(), output_type=EVENT_ROW_TYPE)
    matches = attach_cascade_pipeline(parsed_stream, machine_baselines)
    matches.sink_to(sink).name(f"kafka:{args.sink_topic}")

    log.info(
        "Submitting Thermal-Vibration Cascade job: %s -> %s (group=%s)",
        args.source_topic, args.sink_topic, args.group_id,
    )
    env.execute("HarmonicMesh-ThermalVibrationCascade")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
