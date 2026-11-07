#!/bin/sh
# Launch the pinned Iggy broker (infra/iggy/compose.yml), wait for /ping, then
# create the rat stream + plugin topics and DLQs. Idempotent; safe to rerun.
#   ./scripts/iggy-up.sh            # up + wait + provision
#   ./scripts/iggy-up.sh --down     # stop the container (data volume kept)
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
COMPOSE_FILE="$ROOT/infra/iggy/compose.yml"

if command -v podman-compose >/dev/null 2>&1; then
  COMPOSE="podman-compose"
elif command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
else
  echo "need podman-compose or docker compose" >&2
  exit 1
fi

if [ "${1:-}" = "--down" ]; then
  exec $COMPOSE -f "$COMPOSE_FILE" down
fi

echo "== iggy broker ($COMPOSE) =="
$COMPOSE -f "$COMPOSE_FILE" up -d

echo "== wait =="
if ! "$ROOT/scripts/iggy-wait.sh"; then
  podman logs --tail 80 rat-iggy >&2 2>/dev/null || docker logs --tail 80 rat-iggy >&2 || true
  exit 1
fi

echo "== topics =="
RAT_BROKER=iggy uv run rat topics
