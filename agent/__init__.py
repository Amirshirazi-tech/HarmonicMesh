"""HarmonicMesh Phase 5 LangGraph reasoning agent.

The package is split in two layers:

  * ``agent/`` — the inner workflow: state, nodes, graph assembly. Pure
    reasoning code. Has no knowledge of Kafka.
  * ``consumers/agent_consumer.py`` — the outer loop: Kafka consumption,
    offset management, retry policy. Compiles the LangGraph once at startup
    via :func:`agent.graph.build_agent` and invokes it per message.

The agent's only persistence interface is the ``graphiti_layer`` package; no
graph access goes through any other code path.
"""
