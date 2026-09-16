#!/bin/sh
# Create the topics the plugins declare, plus a DLQ per source. Idempotent.
# Run from the repo root or anywhere — it finds its own way. Requires the
# kafka container from infra/kafka/compose.yml.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
CONTAINER=${RAT_KAFKA_CONTAINER:-rat-kafka}
BOOTSTRAP=${RAT_BOOTSTRAP:-localhost:9092}
PARTITIONS=${RAT_PARTITIONS:-6}
EVENTS_RETENTION_MS=${RAT_EVENTS_RETENTION_MS:-604800000}   # 7d
DLQ_RETENTION_MS=${RAT_DLQ_RETENTION_MS:-2592000000}        # 30d

# Detect a runtime whose kafka container is RUNNING (not merely created):
# exec into a stopped container fails with an obscure error later.
if command -v podman >/dev/null 2>&1 && \
   podman ps --filter "name=^${CONTAINER}$" --format "{{.Names}}" | grep -qx "$CONTAINER"; then
  RUNTIME=podman
elif command -v docker >/dev/null 2>&1 && \
     docker ps --filter "name=^${CONTAINER}$" --format "{{.Names}}" | grep -qx "$CONTAINER"; then
  RUNTIME=docker
else
  echo "kafka container '$CONTAINER' is not running — start it first:" >&2
  echo "  podman-compose -f infra/kafka/compose.yml up -d" >&2
  exit 1
fi

TOPICS=""
for manifest in "$ROOT"/plugins/*/plugin.toml; do
  [ -f "$manifest" ] || continue
  # tolerate leading whitespace and single or double quoted values
  t=$(sed -n "s/^[[:space:]]*topic[[:space:]]*=[[:space:]]*['\"]\\([^'\"]*\\)['\"].*/\\1/p" "$manifest" | head -n 1)
  if [ -n "$t" ]; then
    TOPICS="$TOPICS $t ${t}.dlq"
  else
    echo "warning: no topic in $manifest" >&2
  fi
done

if [ -z "$TOPICS" ]; then
  echo "no plugin topics found under $ROOT/plugins" >&2
  exit 1
fi

for t in $TOPICS; do
  case $t in
    *.dlq) conf="retention.ms=$DLQ_RETENTION_MS" ;;
    *)     conf="retention.ms=$EVENTS_RETENTION_MS" ;;
  esac
  echo "creating $t ($conf)"
  "$RUNTIME" exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$t" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    --config "$conf" >/dev/null
done

echo
"$RUNTIME" exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --list
