#!/bin/sh
# End-to-end smoke: broker -> topics -> fixture pair -> Correlate -> Query.
# Proves the repo's done check: a source lands queryable and the $AAPL pair
# becomes exactly one correlated row. Safe to rerun; uses its own sink dir
# under /tmp — a custom RAT_SINK_DIR pointing at durable data is refused.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

SINK_DIR=${RAT_SINK_DIR:-/tmp/rat-smoke}
BOOTSTRAP=${RAT_BOOTSTRAP:-localhost:9092}

# nix shell when the machine has it; bare commands in CI.
if command -v nix >/dev/null 2>&1 && [ -f "$ROOT/flake.nix" ]; then
  PREFIX="nix develop -c"
else
  PREFIX=""
fi

# One container engine for every exec call: podman when present (this box,
# possibly via the flake), docker otherwise (CI).
if command -v podman >/dev/null 2>&1; then
  ENGINE=podman
elif command -v nix >/dev/null 2>&1 && [ -f "$ROOT/flake.nix" ] \
     && $PREFIX sh -c 'command -v podman >/dev/null 2>&1'; then
  ENGINE="nix-podman"
elif command -v docker >/dev/null 2>&1; then
  ENGINE=docker
else
  echo "no container engine (podman/docker) found" >&2
  exit 1
fi

eng() {  # run a container-engine command through the same prefix
  if [ "$ENGINE" = "nix-podman" ]; then
    $PREFIX podman "$@"
  else
    "$ENGINE" "$@"
  fi
}

echo "== broker =="
if ! eng container inspect rat-kafka >/dev/null 2>&1; then
  if command -v podman-compose >/dev/null 2>&1 || [ "$ENGINE" = "nix-podman" ]; then
    $PREFIX podman-compose -f infra/kafka/compose.yml up -d
  else
    docker compose -f infra/kafka/compose.yml up -d
  fi
fi
i=0
until eng exec rat-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -gt 30 ] && echo "broker never came up" >&2 && exit 1
  sleep 2
done

echo "== topics =="
./scripts/make-topics.sh

echo "== fixture =="
case "$SINK_DIR" in
  /tmp/*) rm -rf "$SINK_DIR" ;;
  *) echo "refusing to wipe RAT_SINK_DIR=$SINK_DIR — point it under /tmp for smoke runs" >&2; exit 1 ;;
esac
TS=$($PREFIX uv run python scripts/fixture.py | tail -1)
case "$TS" in
  ''|*[!0-9]*) echo "fixture failed: bad timestamp '$TS'" >&2; exit 1 ;;
esac
echo "fixture timestamp: $TS"

echo "== correlate (bounded run) =="
rc=0
(cd spark && RAT_SINK_DIR="$SINK_DIR" RAT_BOOTSTRAP="$BOOTSTRAP" \
  RAT_STARTING_OFFSETS=earliest RAT_TRIGGER="2 seconds" \
  timeout --signal=INT --kill-after=15 240 $PREFIX sbt -batch "runMain rat.Correlate") || rc=$?
case "$rc" in
  0|124|130|143) ;;  # clean stop and the expected timeout/INT exits
  *) echo "Correlate failed rc=$rc" >&2; exit "$rc" ;;
esac

echo "== query + assert =="
OUT=$(cd spark && RAT_SINK_DIR="$SINK_DIR" $PREFIX sbt -batch "runMain rat.Query") || {
  echo "Query failed" >&2; exit 1; }
echo "$OUT" | tail -30

RAW_ROWS=$(echo "$OUT" | grep -c "smoke:raw:$TS" || true)
PAIR_ROWS=$(echo "$OUT" | grep -cE "smoke:hn:$TS.*smoke:rss:$TS" || true)

echo "raw rows for this run: $RAW_ROWS (need 1)"
echo "correlated pair rows: $PAIR_ROWS (need exactly 1)"
[ "$RAW_ROWS" -ge 1 ] || { echo "FAIL: raw envelope missing from rat_events" >&2; exit 1; }
[ "$PAIR_ROWS" -eq 1 ] || { echo "FAIL: expected exactly one correlated row" >&2; exit 1; }

echo "SMOKE OK — raw row queryable, AAPL pair = one row"
