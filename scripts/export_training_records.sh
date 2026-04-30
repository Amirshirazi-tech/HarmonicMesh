#!/usr/bin/env bash
# Bundle training_data/harmonicmesh/ JSONL files for SovereignMesh handoff.
set -euo pipefail

EXPORT_FILE="harmonicmesh_training_records_$(date +%Y%m%d).tar.gz"

echo "Bundling training records -> ${EXPORT_FILE}"
tar czf "${EXPORT_FILE}" training_data/harmonicmesh/
echo "Export written to ${EXPORT_FILE} — hand off to SovereignMesh."
