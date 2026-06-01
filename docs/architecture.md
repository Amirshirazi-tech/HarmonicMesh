# Architecture

See `harmonicmesh_project_spec.md` Section 5 for the full architecture diagram and data flow.

Key principle: detection (Flink CEP) is decoupled from response (LangGraph agent) via Kafka.
No CEP job writes directly to Graphiti — all writes go through the agent.

## Kafka topic retention

**Rule: any Kafka topic whose records are stamped with *simulated* event-time
(i.e. from `SimulationClock.now()`) needs `retention.ms=-1` (infinite), regardless
of whether it is a source or a sink topic.**

Kafka's retention thread compares message timestamps against wall-clock `now`.  Under
time-compression (and especially the 90-day warm-up history), simulated event-times are
far in the past.  With the default 7-day retention, the broker treats freshly written
records as already expired and deletes them within minutes of arrival.

Topics affected:

| Topic | Role | Reason |
|---|---|---|
| `harmonicmesh.patterns.*` | CEP output | Flink stamps matches with the match's event-time |
| `harmonicmesh.edi.events` | EDI simulator output | `SimulationClock`-stamped event-time |

Topics **not** affected:
- `harmonicmesh.sensors.*` — machine simulator writes wall-clock-aligned event times
- `harmonicmesh.heartbeats.*` — same

Apply with the idempotent `scripts/configure_kafka_topics.sh` after `docker compose up`.
The script's `PATTERN_TOPICS` array lists all topics that need this setting; add to it
whenever a new simulated-time topic is introduced.

## Episode-type discrimination in Graphiti

Graphiti stores every episode as an `EpisodicNode` with a `source_description` field.
HarmonicMesh uses that field as the **stable public identifier** for the logical
episode type. The Phase 5 agent's `search_history(episode_types=[...])` filter
translates logical names to source-description strings via
`graphiti_layer.EPISODE_TYPE_TO_SOURCE_DESCRIPTION`.

The mapping today:

| Logical name          | `source_description` written to Neo4j      |
|-----------------------|--------------------------------------------|
| `pattern_occurrence`  | `HarmonicMesh CEP pattern match`           |
| `intervention`        | `HarmonicMesh maintenance intervention`    |
| `agent_alert`         | `HarmonicMesh agent alert`                 |
| `outcome`             | `HarmonicMesh intervention outcome`        |

**These strings must not be changed** without migrating existing episode data
in Neo4j (or starting from a clean volume). Renaming a value silently makes
every previously written episode invisible to type-filtered retrieval. Adding
a new logical type is safe; renaming or removing one is not.

## Phase 5 known limitation — alert duplication on downstream retry

The agent's LangGraph runs `emit_alert -> reify_memory -> emit_training_record`
in that order. Kafka offset commit happens only after the full graph completes.
If `reify_memory` (Graphiti write) or `emit_training_record` (filesystem write)
fails, the consumer seeks back and re-invokes the graph, which re-runs
`emit_alert` and publishes a second alert for the same input pattern match.

Observed live during Phase 5 verification: 3 duplicate alerts were emitted to
`harmonicmesh.alerts.machine-03` for a single pattern match while reify_memory
was failing on a Graphiti reserved-attribute clash. Functionally correct
under the at-least-once contract; not ideal for downstream consumers.

Two v2 options to address this:

1. **Deterministic `alert_id`** derived from `(machine_id, detected_at,
   pattern_name)` rather than a UUID, so downstream consumers can dedupe
   alerts by key. The alert topic could also use log compaction on
   `alert_id` to drop duplicates at the broker.
2. **Reorder the graph** so `reify_memory` (and possibly
   `emit_training_record`) commit before `emit_alert`. A retry then re-runs
   the idempotent Graphiti write — Graphiti already deduplicates by episode
   reference time + group_id — and only emits one alert.

Neither is required for Phase 5 correctness; both are reserved for v2.
