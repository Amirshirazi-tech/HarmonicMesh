# Phase 4 + 5 — shared consumer image (pattern_ingester, agent_consumer).
#
# Build context is the repo root so graphiti_layer/, consumers/ and agent/ are
# importable. Model weights (bge-m3, bge-reranker-v2-m3) are NOT baked in —
# they download on first run into the mounted hf_cache volume (HF_HOME). The
# image carries both consumers; docker-compose picks which CMD to run per service.
FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    HF_HOME=/hf-cache \
    SENTENCE_TRANSFORMERS_HOME=/hf-cache \
    MODEL_DEVICE=cpu

# Requirements first for layer caching.
COPY graphiti_layer/requirements.txt /app/graphiti_layer/requirements.txt
COPY consumers/requirements.txt /app/consumers/requirements.txt

# CPU-only torch wheels — no CUDA payload needed for bge models at this scale.
RUN pip install --no-cache-dir \
        --extra-index-url https://download.pytorch.org/whl/cpu \
        -r /app/consumers/requirements.txt

COPY graphiti_layer/ /app/graphiti_layer/
COPY consumers/ /app/consumers/
COPY agent/ /app/agent/

CMD ["python", "-m", "consumers.pattern_ingester"]
