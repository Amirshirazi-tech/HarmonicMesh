#!/usr/bin/env bash
# Snapshot the Neo4j data volume after a full warm-up run.
# Distribute the archive via GitHub Releases (too large for git).
set -euo pipefail

SNAPSHOT_FILE="neo4j_warmup_snapshot_$(date +%Y%m%d_%H%M%S).tar.gz"

echo "Snapshotting harmonicmesh_neo4j_data -> ${SNAPSHOT_FILE}"
docker run --rm \
  -v harmonicmesh_neo4j_data:/data \
  -v "$(pwd)":/backup \
  alpine tar czf "/backup/${SNAPSHOT_FILE}" -C /data .

echo "Snapshot written to ${SNAPSHOT_FILE}"
