#!/usr/bin/env bash
# Mode 3: Fast development iteration (100x compression, short bursts).
set -euo pipefail

COMPRESSION="${TIME_COMPRESSION:-100}"
SEED="${SIM_SEED:-42}"

echo "Starting HarmonicMesh dev run: ${COMPRESSION}x compression, seed=${SEED}"
echo "Phase 2+ required: simulators not yet implemented."
