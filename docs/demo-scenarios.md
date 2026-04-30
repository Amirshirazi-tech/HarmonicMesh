# Demo Scenarios

## Scenario 1 — Recurring Bearing Failure (Machine-03)

Trigger: `--inject thermal-vibration-cascade --machine machine-03 --at T+120s`

Expected output in Kafka UI:
1. `harmonicmesh.sensors.machine-03` shows temperature rise, then vibration spike.
2. `harmonicmesh.patterns.machine-03` receives a `ThermalVibrationCascade` event within 10 simulated minutes.
3. `harmonicmesh.alerts.machine-03` receives a contextual alert referencing prior occurrences from Graphiti.

## Scenario 2 — Missing ORDRSP (Hillebrand GmbH)

Trigger: `--inject missing-ordrsp --partner HGB`

Expected output:
1. `harmonicmesh.edi.HGB` shows `ORDERS` with no following `ORDRSP`.
2. `harmonicmesh.patterns.edi` receives a `MissingORDRSP` event after 24 simulated hours.
3. Alert includes Hillebrand's SLA breach history from Graphiti.
