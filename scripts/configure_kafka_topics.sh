#!/usr/bin/env bash
#
# configure_kafka_topics.sh — apply retention policy to HarmonicMesh CEP output topics.
#
# Why: pattern topics (harmonicmesh.patterns.*) carry CEP match records whose
# Kafka message timestamp is the *simulated* event-time of the match — often
# far in the past relative to wall-clock. With the broker default (7-day)
# retention, Kafka's retention thread treats a freshly written match as
# already expired and deletes the segment within minutes. retention.ms=-1
# (infinite) keeps them.
#
# Sensor topics (harmonicmesh.sensors.*) are deliberately left at default
# retention — they receive a steady stream of fresh, wall-clock-aligned records.
#
# Idempotent: safe to run repeatedly. Run once after `docker compose up`.
#
# Credentials: KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD, sourced from .env
# (the same file docker compose uses) or already present in the environment.

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-harmonicmesh-kafka}"
# Internal SASL_PLAINTEXT listener. Do NOT use localhost:9092 from inside the
# container — that listener advertises localhost:9192 (the host port), which
# is unreachable from within the container network.
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

# Pattern topics that must have infinite retention. Created if absent so the
# setting is in place before the CEP job writes its first match.
PATTERN_TOPICS=(
  harmonicmesh.patterns.machine-03
)

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="$repo_root/.env"
if [[ -f "$env_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
fi

: "${KAFKA_SASL_USERNAME:?set KAFKA_SASL_USERNAME (expected in $env_file)}"
: "${KAFKA_SASL_PASSWORD:?set KAFKA_SASL_PASSWORD (expected in $env_file)}"

if ! docker ps --format '{{.Names}}' | grep -qx "$KAFKA_CONTAINER"; then
  echo "error: Kafka container '$KAFKA_CONTAINER' is not running." >&2
  echo "       Start the stack first: docker compose up -d" >&2
  exit 1
fi

# Throwaway SASL client config, written inside the container and removed on exit.
client_config=/tmp/harmonicmesh-admin.properties
docker exec -i "$KAFKA_CONTAINER" bash -c "cat > $client_config" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${KAFKA_SASL_USERNAME}" password="${KAFKA_SASL_PASSWORD}";
EOF

cleanup() { docker exec "$KAFKA_CONTAINER" rm -f "$client_config" >/dev/null 2>&1 || true; }
trap cleanup EXIT

ktopics() {
  docker exec -i "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$BOOTSTRAP" --command-config "$client_config" "$@"
}
kcfg() {
  docker exec -i "$KAFKA_CONTAINER" kafka-configs \
    --bootstrap-server "$BOOTSTRAP" --command-config "$client_config" "$@"
}

# Union the known list with any other harmonicmesh.patterns.* topics that
# already exist, so future pattern topics are picked up automatically.
existing="$(ktopics --list 2>/dev/null | tr -d '\r' | grep '^harmonicmesh\.patterns\.' || true)"
declare -A want=()
for t in "${PATTERN_TOPICS[@]}" $existing; do
  want["$t"]=1
done

for topic in "${!want[@]}"; do
  echo "==> $topic"
  ktopics --create --if-not-exists --topic "$topic" \
    --partitions 1 --replication-factor 1 --config retention.ms=-1
  kcfg --alter --entity-type topics --entity-name "$topic" \
    --add-config retention.ms=-1
  kcfg --describe --entity-type topics --entity-name "$topic"
done

echo
echo "Done — harmonicmesh.patterns.* topics set to retention.ms=-1 (infinite)."
