# Architecture

See `harmonicmesh_project_spec.md` Section 5 for the full architecture diagram and data flow.

Key principle: detection (Flink CEP) is decoupled from response (LangGraph agent) via Kafka.
No CEP job writes directly to Graphiti — all writes go through the agent.

## Kafka topic retention

Pattern topics (`harmonicmesh.patterns.*`) use **infinite retention** (`retention.ms=-1`).

Flink's `KafkaSink` stamps each CEP match with the *event-time of the match* — which is
*simulated* time, often far in the past under time-compression (and especially the 90-day
warm-up history). Kafka's retention thread compares message timestamps against wall-clock
`now`, so with the default 7-day retention it treats freshly written matches as already
expired and deletes them within minutes.

Apply the setting after `docker compose up` with the idempotent
`scripts/configure_kafka_topics.sh`.

This override is scoped to `harmonicmesh.patterns.*` only. Ingest topics
(`harmonicmesh.sensors.*`, `harmonicmesh.heartbeats.*`, `harmonicmesh.edi.*`) keep the
default retention — they carry a steady stream of fresh, wall-clock-aligned records.

This is a Kafka topic-configuration concern only; the Flink CEP job and the simulator are
deliberately left unchanged.
