# Architecture

See `harmonicmesh_project_spec.md` Section 5 for the full architecture diagram and data flow.

Key principle: detection (Flink CEP) is decoupled from response (LangGraph agent) via Kafka.
No CEP job writes directly to Graphiti — all writes go through the agent.
