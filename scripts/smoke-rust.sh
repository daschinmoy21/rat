#!/usr/bin/env bash
# Actual Iggy -> Rust -> Parquet -> DataFusion smoke, isolated by stream name.
set -euo pipefail
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
BIN=${RAT_ENGINE_BIN:-$ROOT/target/release/rat-rs}
[[ -x "$BIN" ]] || { echo "build rat-rs first (cargo build --locked --release -p rat-rs)" >&2; exit 1; }
WORK=$(mktemp -d "${TMPDIR:-/tmp}/rat-smoke.XXXXXX")
export RAT_BROKER=iggy RAT_IGGY_STREAM="rat-smoke-$(date +%s)-$$"
export RAT_SINK_DIR="$WORK/sink" RAT_CHECKPOINT_DIR="$WORK/checkpoint"
export RAT_STARTING_OFFSETS=earliest RAT_TRIGGER='100 milliseconds'
PID=
STARTED_BROKER=false
cleanup() {
  if [[ -n "$PID" ]]; then kill -TERM "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; fi
  if [[ "$STARTED_BROKER" == true ]]; then ./scripts/iggy-up.sh --down >/dev/null 2>&1 || true; fi
  # Only scratch data created by this invocation is removed.
  rm -rf -- "$WORK"
}
trap cleanup EXIT
if ! python3 -c 'import os,urllib.request as u; u.urlopen(os.environ.get("RAT_IGGY_URL","http://127.0.0.1:3000")+"/ping",timeout=2)' >/dev/null 2>&1; then
  STARTED_BROKER=true
  ./scripts/iggy-up.sh
else
  uv run rat topics
fi
"$BIN" consume >"$WORK/engine.log" 2>&1 &
PID=$!
uv run python scripts/fixture.py
await_rows() {
  for _ in $(seq 1 120); do
    if ! kill -0 "$PID" 2>/dev/null; then cat "$WORK/engine.log" >&2; return 1; fi
    if "$BIN" digest >"$WORK/digest.json" 2>/dev/null && python3 -c \
      'import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if d["raw_rows"]==1 and d["correlated_rows"]==3 else 1)' \
      "$WORK/digest.json"; then return 0; fi
    sleep 0.5
  done
  cat "$WORK/engine.log" >&2
  echo "timed out waiting for three pairs and one raw row" >&2
  return 1
}
await_rows
if [[ "$STARTED_BROKER" == true ]]; then
  # Fault-inject only into a broker this invocation started. Verifying sink
  # rows alone would miss a broker that lost already-consumed messages.
  if command -v podman >/dev/null 2>&1 && podman container exists rat-iggy; then
    RUNTIME=podman
  else
    RUNTIME=docker
  fi
  "$RUNTIME" kill --signal KILL rat-iggy
  "$RUNTIME" start rat-iggy
  ./scripts/iggy-wait.sh
  uv run python - <<'PY'
from rat_producers.iggy import IggyClient
c=IggyClient.from_env()
s=c.get_stream()
assert s['messages_count']==4, f"broker lost messages across crash: {s['messages_count']}"
print('broker retained all four messages across forced kill')
PY
fi
cp "$WORK/digest.json" "$WORK/before.json"
kill -TERM "$PID"
wait "$PID"
PID=
"$BIN" consume >>"$WORK/engine.log" 2>&1 &
PID=$!
sleep 2
await_rows
python3 - "$WORK/before.json" "$WORK/digest.json" <<'PY'
import json,sys
a,b=(json.load(open(p)) for p in sys.argv[1:])
assert a['digest']==b['digest'], (a,b)
print('restart did not duplicate output:', b)
PY
"$BIN" sql 'SELECT count(*) AS pairs FROM correlated' --format json
"$BIN" sql 'SELECT count(*) AS raw FROM rat_events' --format json
kill -TERM "$PID"
wait "$PID"
PID=
# The stream is unique to this smoke; leave any shared broker and its data alone.
uv run python - <<'PY'
import os
from rat_producers.iggy import IggyClient
c=IggyClient.from_env()
c.request('DELETE', '/streams/' + os.environ['RAT_IGGY_STREAM'])
PY
echo 'Rust + real Iggy smoke passed'
