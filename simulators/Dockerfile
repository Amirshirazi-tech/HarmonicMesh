# Phase 6 — EDI simulator image.
#
# Minimal image: only the simulators/ package and its requirements.
# Build context is the repo root.
FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY simulators/requirements.txt /app/simulators/requirements.txt
RUN pip install --no-cache-dir -r /app/simulators/requirements.txt

COPY simulators/ /app/simulators/

CMD ["python", "-m", "simulators.edi_simulator"]
