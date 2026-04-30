#!/usr/bin/env bash
# Mode 1: 90-day compressed warm-up (agent ON, 1000x time compression).
# Runs after Phase 5 is complete.
set -euo pipefail

COMPRESSION="${TIME_COMPRESSION:-1000}"
SEED="${SIM_SEED:-42}"

echo "Starting HarmonicMesh warm-up: ${COMPRESSION}x compression, seed=${SEED}"
echo "Phase 2+ required: simulators not yet implemented."
