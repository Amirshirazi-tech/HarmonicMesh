# HarmonicMesh

Real-time failure pattern detection with temporal memory for industrial operations.
Flink CEP detects recurring signatures; Graphiti remembers them.

---

## Quick Start

### Prerequisites

- Docker Engine 24+ and Docker Compose v2
- 8 GB RAM available to Docker (Flink + Neo4j are the heavy hitters)

### 1. Clone and configure credentials

```bash
git clone https://github.com/Amirshirazi-tech/harmonicmesh.git
cd harmonicmesh
```

Copy the environment template and set your passwords:

```bash
cp .env.example .env
# Edit .env and fill in all <placeholder> values before continuing.
```

Copy the Kafka JAAS template and set the **same** passwords as `.env`:

```bash
cp secrets/kafka_server_jaas.conf.example secrets/kafka_server_jaas.conf
# Edit secrets/kafka_server_jaas.conf:
#   user_admin="<KAFKA_ADMIN_PASSWORD>"         ← must match .env KAFKA_ADMIN_PASSWORD
#   user_harmonicmesh="<KAFKA_SASL_PASSWORD>"   ← must match .env KAFKA_SASL_PASSWORD
#   The top-level username/password block uses KAFKA_ADMIN_PASSWORD as well.
```

### 2. Start the stack

```bash
docker compose up -d
```

The first run pulls images and builds the `api` container — allow 5–10 minutes.
Subsequent starts are fast.

### 3. Verify all UIs are reachable

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8180 | admin / `KAFKA_UI_PASSWORD` from `.env` |
| Neo4j Browser | http://localhost:7474 | neo4j / `NEO4J_PASSWORD` from `.env` |
| Flink JobManager | http://localhost:8181 | — |
| FastAPI health | http://localhost:8001/health | — |

Wait ~60 seconds after `docker compose up` for Neo4j and Flink to finish initializing.
The Kafka broker needs ~45 seconds before Kafka UI can connect.

### 4. Run the machine simulator (Phase 2+)

```bash
cd simulators
pip install -r requirements.txt

# Real-time, 60 simulated seconds (~60 wall-clock seconds)
python3 -m machine_simulator --compression 1 --duration 60s

# Fast dev run: 1 h simulated in ~36 wall-clock seconds
python3 -m machine_simulator --compression 100 --duration 1h

# Warm-up burst: 30 000 simulated seconds in ~30 wall-clock seconds
python3 -m machine_simulator --compression 1000 --duration 30000s  # ~30 wall-clock seconds

# --duration is always simulated time; wall-clock = duration / compression
```

Observed throughput at compression=1000: ~5 000 events/sec across all topics
(5 machines × 1 event/simulated-second × 1000).

### 5. Stop the stack

```bash
docker compose down          # stop containers, keep volumes
docker compose down -v       # stop and delete all data volumes (full reset)
```

---

## Stack Overview

| Service | Image | Port (host) |
|---|---|---|
| Kafka (KRaft, SASL) | confluentinc/cp-kafka:latest | 9192 |
| Kafka UI | provectuslabs/kafka-ui:latest | 8180 |
| Neo4j Community 5 | neo4j:5-community | 7474, 7687 |
| Flink JobManager | flink:1.19-scala_2.12-java11 | 8181 |
| Flink TaskManager | flink:1.19-scala_2.12-java11 | — |
| FastAPI | python:3.11-slim + fastapi | 8001 |

All services share the `harmonicmeshnet` Docker network.
Ports are offset +100 from a sibling stack so both can run simultaneously on the same host.

### Kafka topics (created in Phase 2+)

```
harmonicmesh.sensors.<machine-id>    # raw sensor telemetry
harmonicmesh.edi.<partner-code>      # EDI process messages
harmonicmesh.patterns.<machine-id>   # Flink CEP pattern matches
harmonicmesh.alerts.<machine-id>     # agent-generated alerts
harmonicmesh.training_records        # downstream training data (v2)
```

---

## Repository Structure

```
harmonicmesh/
├── docker-compose.yml
├── .env.example
├── secrets/
│   └── kafka_server_jaas.conf.example   # copy to kafka_server_jaas.conf
├── docs/
│   ├── architecture.md
│   ├── patterns.md
│   ├── edi-json-mapping.md
│   └── demo-scenarios.md
├── simulators/
│   ├── machine_simulator/   # Phase 2
│   └── edi_simulator/       # Phase 6
├── flink_jobs/
│   ├── java/                # Phase 3 — CEP Pattern API (Java)
│   └── sql/                 # Phase 6 — MATCH_RECOGNIZE + LEFT JOIN
├── agent/                   # Phase 5 — LangGraph reactive consumer
├── graphiti_layer/          # Phase 4 — Graphiti + Neo4j
├── api/                     # Phase 1 — FastAPI (health + alerts)
├── training_data/           # downstream training-pipeline JSONL exports (gitignored)
├── scripts/
│   ├── run_warmup.sh        # 90-day compressed warm-up (Phase 5+)
│   ├── run_demo.sh          # live demo from snapshot (Phase 5+)
│   ├── run_dev.sh           # fast iteration (Phase 5+)
│   ├── snapshot_neo4j.sh    # snapshot Neo4j volume after warm-up
│   └── export_training_records.sh
└── tests/
```

---

## Operational Modes (Phase 5+)

| Mode | Script | Compression | Purpose |
|---|---|---|---|
| Warm-up | `scripts/run_warmup.sh` | 1000x | Generate 90 simulated days of history + training records |
| Demo | `scripts/run_demo.sh` | 1x or 10x | Live agent against pre-warmed history |
| Dev | `scripts/run_dev.sh` | 100x | Fast iteration on agent logic |

---

## Neo4j Snapshot (Phase 6+)

After a full warm-up run, snapshot Neo4j data for instant demo restores:

```bash
# Create snapshot
./scripts/snapshot_neo4j.sh

# Restore from snapshot (wipes current Neo4j volume)
docker compose stop neo4j
docker run --rm \
  -v harmonicmesh_neo4j_data:/data \
  -v $(pwd):/backup \
  alpine tar xzf /backup/<snapshot-file>.tar.gz -C /data
docker compose start neo4j
```

Distribute snapshots via GitHub Releases — too large for the git repo.

---

## Security Note

`secrets/kafka_server_jaas.conf` is gitignored. Never commit it.
Kafka runs SASL/PLAIN — credentials are in plaintext on the wire.
This is a local development stack; do not expose ports to the public internet.

---

## Built with Claude Code

This project was implemented with Claude Code, Anthropic's coding agent, working in a
delegated mode. I owned the architecture — making the design decisions, directing the work,
and verifying every component end-to-end — while Claude Code wrote the code from directive
prompts. The experience underlined a shift worth naming: as coding agents grow more capable,
the binding constraint moves from typing to judgment.

---

## Attribution

The CEP architecture follows principles from Kai Waehner's April 2026 article
*"Complex Event Processing (CEP) with Apache Flink: What It Is and When (Not) to Use It"*:
- Every pattern uses an explicit `WITHIN` clause to bound state memory.
- Detection (Flink CEP → Kafka) is decoupled from response (LangGraph agent).
- Separate Flink jobs per pattern family — no monolithic job.
