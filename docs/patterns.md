# CEP Patterns

Three v1 patterns, one per CEP surface:

| Pattern | Method | Phase |
|---|---|---|
| Thermal-Vibration Cascade | Pattern API (Java) | 3 |
| Missing Heartbeat | Flink SQL LEFT JOIN | 6 |
| EDI Sequence Violation | MATCH_RECOGNIZE | 6 |

> **Note on language.** The Pattern API is implemented in **Java**, not
> Python — PyFlink does not expose CEP bindings in any released version.
> The rest of the project (machine simulator, agent) remains
> Python; only Flink CEP code lives under `flink_jobs/java/`.

---

## Pattern 1 — Thermal-Vibration Cascade

Detects the bearing-failure precursor on Machine-03. Three sequential sensor anomalies, each a separate event, all within a ten-minute event-time window.

### Operator structure

```java
Pattern.<SensorEvent>begin("temp_anomaly", AfterMatchSkipStrategy.skipPastLastEvent())
       .where(temperature_c > baseline_temp + offset)
       .followedBy("vib_anomaly")
       .where(vibration_rms_mm_s > threshold)
       .followedBy("current_anomaly")
       .where(|current_a - baseline_current| / baseline_current > deviation_pct)
       .within(Duration.ofMinutes(10))
```

Both transitions use `.followedBy(...)` (relaxed contiguity): events between cascade steps are tolerated, which matches the simulator where every tick emits all three sensors and we only react to the ones crossing their threshold. `AfterMatchSkipStrategy.skipPastLastEvent()` ensures one cascade emits one match.

### Threshold mapping (Machine-03)

The spec's original phrasing — *"temperature > 85 °C"* — was missing its qualifier; the intent is *85 °C above baseline*, or for Machine-03 specifically, **temperature 60 °C above its 320 °C baseline**. Thresholds are loaded from `flink_jobs/config/machine_baselines.yaml` at job startup (bundled into the JAR as a classpath resource via the Maven build); runtime baseline learning is v2 scope.

| Sensor | Predicate form | Machine-03 values | Effective threshold |
|---|---|---|---|
| `temperature_c` | additive deviation | baseline 320 °C, offset 60 °C | strict `>` 380 °C |
| `vibration_rms_mm_s` | absolute | threshold 4.5 mm/s | strict `>` 4.5 mm/s |
| `current_a` | symmetric multiplicative deviation | baseline 415 A, ±15 % | strict `>` (\|x − 415\|/415) > 0.15 |

All three predicates use **strict `>`** (not `>=`). Equality at the threshold is treated as not-yet-anomalous and is asserted by the threshold-edge test case in `tests/flink/test_thermal_vibration_cascade.py`.

### Time semantics

End-to-end event time. The watermark strategy is:

- `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))`
- `.withIdleness(Duration.ofSeconds(30))`
- timestamp pulled from each event's `event_time` JSON field, parsed to milliseconds

The 5 s out-of-orderness budget is generous for a synthetic stream where producer threads briefly desync under high time-compression; it is small enough that real cascades, which span minutes, are never delayed perceptibly. The 30 s idleness flag lets the partition stop holding back watermark progression during quiet periods so `.within(10m)` deadlines fire on schedule.

The `.within(...)` deadline is enforced *by the pattern operator on event-time*, independent of the watermark. A late-arriving third event whose event_time exceeds `first_event_time + 10 min` is discarded even if the watermark hasn't yet crossed the deadline.

### Output schema (v1.0)

Published to `harmonicmesh.patterns.machine-03` as JSON:

```json
{
  "schema_version": "1.0",
  "pattern_name": "ThermalVibrationCascade",
  "machine_id": "Machine-03",
  "detected_at": "2026-04-18T14:23:00.000Z",
  "severity": "CRITICAL",
  "source_events": [
    { "machine_id": "Machine-03", "event_time": "...", "sensors": {...} },
    { "machine_id": "Machine-03", "event_time": "...", "sensors": {...} },
    { "machine_id": "Machine-03", "event_time": "...", "sensors": {...} }
  ]
}
```

`detected_at` is the event_time of the third (current_anomaly) event — the moment the cascade closes. `severity` is hard-coded `CRITICAL` in v1; tiered severity requires historical context and belongs in the Phase 5 agent layer, not in CEP.

### Source code

- Job: `flink_jobs/java/src/main/java/com/harmonicmesh/cep/ThermalVibrationCascadeJob.java`
- Conditions / selector: same file (top-level static classes)
- POJO: `flink_jobs/java/src/main/java/com/harmonicmesh/cep/SensorEvent.java`
- Baselines loader: `flink_jobs/java/src/main/java/com/harmonicmesh/cep/MachineBaselines.java`
- Baselines values: `flink_jobs/config/machine_baselines.yaml`
- Tests: `flink_jobs/java/src/test/java/com/harmonicmesh/cep/ThermalVibrationCascadeJobTest.java`
- Build / submit instructions: `flink_jobs/java/README.md`

---

## Pattern 2 — Missing Heartbeat

Phase 6. Flink SQL LEFT JOIN approach (per Waehner's recommendation over a negative pattern). Detects absence of a Machine-04 heartbeat on `harmonicmesh.heartbeats.machine-04` for >90 simulated seconds.

## Pattern 3 — EDI Sequence Violation

Phase 6. Flink SQL `MATCH_RECOGNIZE`. Validates the expected `PO → ORDRSP → DESADV → RECADV → INVOIC` sequence per trading partner.

---

## Follow-up

The project spec (`harmonicmesh_project_spec.md` §8) still shows the old absolute-threshold phrasing for Pattern 1 (`temperature > 85°C`). Update it to reflect the per-machine baseline + deviation form documented above before any external publication.
