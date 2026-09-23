#!/bin/sh
# Real HDFS + HiveServer2 + beeline end-to-end smoke test.
# Spins up Kafka + Hadoop (NameNode :8020, DataNode :9866) + Hive (Metastore :9083, HS2 :10000).
# Emits fixture envelopes, runs Spark Correlate over HDFS, repairs partitions,
# and verifies records via HiveServer2 beeline and rat.Query.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

BOOTSTRAP=${RAT_BOOTSTRAP:-localhost:9092}
HDFS_BASE=${RAT_SINK_DIR:-hdfs://localhost:8020/rat}
CHECKPOINT_DIR=${RAT_CHECKPOINT_DIR:-hdfs://localhost:8020/rat/checkpoints}
METASTORE_URI=${RAT_HIVE_METASTORE_URI:-thrift://localhost:9083}

if command -v nix >/dev/null 2>&1 && [ -f "$ROOT/flake.nix" ]; then
  PREFIX="nix develop -c"
else
  PREFIX=""
fi

if command -v podman >/dev/null 2>&1 && command -v podman-compose >/dev/null 2>&1; then
  ENGINE=podman
elif command -v nix >/dev/null 2>&1 && [ -f "$ROOT/flake.nix" ] \
     && $PREFIX sh -c 'command -v podman >/dev/null 2>&1 && command -v podman-compose >/dev/null 2>&1'; then
  ENGINE="nix-podman"
elif command -v docker >/dev/null 2>&1; then
  ENGINE=docker
else
  echo "no container engine (podman-compose or docker) found" >&2
  exit 1
fi

eng() {
  if [ "$ENGINE" = "nix-podman" ]; then
    $PREFIX podman "$@"
  else
    "$ENGINE" "$@"
  fi
}

compose_up() {
  file="$1"
  if [ "$ENGINE" = "docker" ]; then
    docker compose -f "$file" up -d
  elif [ "$ENGINE" = "nix-podman" ]; then
    $PREFIX podman-compose -f "$file" up -d
  else
    podman-compose -f "$file" up -d
  fi
}

compose_down() {
  file="$1"
  if [ "$ENGINE" = "docker" ]; then
    docker compose -f "$file" down
  elif [ "$ENGINE" = "nix-podman" ]; then
    $PREFIX podman-compose -f "$file" down
  else
    podman-compose -f "$file" down
  fi
}

cleanup() {
  if [ -n "${CORR_PID:-}" ]; then
    kill -TERM -"$CORR_PID" 2>/dev/null || kill -TERM "$CORR_PID" 2>/dev/null || true
  fi
  if [ "${RAT_KEEP_CONTAINERS:-false}" != "true" ]; then
    echo "== cleaning up containers =="
    compose_down "infra/hive/compose.yml" || true
    compose_down "infra/hadoop/compose.yml" || true
    compose_down "infra/kafka/compose.yml" || true
  fi
}
trap cleanup EXIT

echo "== starting broker, hadoop, hive ($ENGINE) =="
compose_up "infra/kafka/compose.yml"
compose_up "infra/hadoop/compose.yml"
compose_up "infra/hive/compose.yml"

echo "== waiting for kafka =="
i=0
until eng exec rat-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -gt 90 ] && { echo "broker timed out" >&2; exit 1; }
  sleep 2
done

echo "== waiting for hdfs =="
i=0
until eng exec rat-namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is OFF"; do
  # If NameNode is stuck in safe mode, command it to leave
  eng exec rat-namenode hdfs dfsadmin -safemode leave >/dev/null 2>&1 || true
  i=$((i + 1))
  [ "$i" -gt 90 ] && { echo "HDFS NameNode timed out" >&2; exit 1; }
  sleep 2
done

echo "== waiting for hive metastore :9083 =="
i=0
until python3 -c "import socket; s = socket.create_connection(('127.0.0.1', 9083), timeout=1); s.close()" >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -gt 90 ] && { echo "metastore timed out" >&2; exit 1; }
  sleep 2
done

echo "== waiting for hiveserver2 :10000 =="
i=0
until eng exec rat-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n hive -e "SHOW DATABASES;" >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -gt 90 ] && { echo "hiveserver2 timed out" >&2; exit 1; }
  sleep 2
done

echo "== topics =="
$PREFIX ./scripts/make-topics.sh

echo "== clean hdfs sink =="
eng exec rat-namenode hdfs dfs -rm -r -f /rat >/dev/null 2>&1 || true

echo "== fixture =="
TS=$($PREFIX uv run python scripts/fixture.py | tail -1)
echo "fixture timestamp: $TS"

echo "== correlate (bounded HDFS + Hive run) =="
set -m
(cd spark && RAT_HIVE_ENABLED=true RAT_HIVE_METASTORE_URI="$METASTORE_URI" \
  RAT_SINK_DIR="$HDFS_BASE" RAT_CHECKPOINT_DIR="$CHECKPOINT_DIR" \
  RAT_BOOTSTRAP="$BOOTSTRAP" RAT_STARTING_OFFSETS=earliest \
  RAT_TRIGGER="2 seconds" RAT_HIVE_MSCK_MIN_SECONDS=1 \
  exec $PREFIX sbt -batch "runMain rat.Correlate") &
CORR_PID=$!
set +m

echo "waiting for partitions to land on HDFS..."
i=0
until eng exec rat-namenode hdfs dfs -test -d /rat/correlated 2>/dev/null && \
      eng exec rat-namenode hdfs dfs -test -d /rat/events_raw 2>/dev/null && \
      [ "$(eng exec rat-namenode hdfs dfs -ls -R /rat/correlated 2>/dev/null | grep -c '\.parquet' || true)" -ge 1 ] && \
      [ "$(eng exec rat-namenode hdfs dfs -ls -R /rat/events_raw 2>/dev/null | grep -c '\.parquet' || true)" -ge 1 ]; do
  i=$((i + 1))
  if [ "$i" -gt 150 ]; then
    echo "Correlate timed out waiting for HDFS partitions" >&2
    kill -TERM -"$CORR_PID" 2>/dev/null || kill -TERM "$CORR_PID" 2>/dev/null || true
    wait "$CORR_PID" 2>/dev/null || true
    exit 1
  fi
  if ! kill -0 "$CORR_PID" 2>/dev/null; then
    echo "Correlate process exited prematurely" >&2
    wait "$CORR_PID" 2>/dev/null || true
    exit 1
  fi
  sleep 2
done

# Wait for Hive MSCK listener to register the new batch
sleep 5

# Stop Correlate cleanly
kill -TERM -"$CORR_PID" 2>/dev/null || kill -TERM "$CORR_PID" 2>/dev/null || true
wait "$CORR_PID" 2>/dev/null || true
unset CORR_PID

# Explicitly ensure partition repair in Hive so beeline reads are fresh
eng exec rat-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n hive \
  -e "MSCK REPAIR TABLE rat_events; MSCK REPAIR TABLE correlated;" >/dev/null 2>&1 || true

echo "== verify partitions on hdfs =="
eng exec rat-namenode hdfs dfs -ls -R /rat

echo "== query through beeline =="
CORR_OUT=$(eng exec rat-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n hive \
  -e "SELECT entity, a_id, b_id, ts_ms FROM correlated;")
echo "$CORR_OUT"

RAW_OUT=$(eng exec rat-hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n hive \
  -e "SELECT event_id, source, ts_ms FROM rat_events WHERE event_id LIKE '%$TS%';")
echo "$RAW_OUT"

echo "== query through rat.Query =="
QUERY_OUT=$(cd spark && RAT_HIVE_ENABLED=true RAT_HIVE_METASTORE_URI="$METASTORE_URI" \
  RAT_SINK_DIR="$HDFS_BASE" $PREFIX sbt -batch "runMain rat.Query")
echo "$QUERY_OUT" | tail -30

PAIR_MATCH=$(echo "$CORR_OUT" | grep -cE "smoke:hn:$TS.*smoke:rss:$TS" || true)
RAW_MATCH=$(echo "$RAW_OUT" | grep -c "smoke:raw:$TS" || true)

echo "beeline correlated pairs: $PAIR_MATCH (need >= 1)"
echo "beeline raw rows: $RAW_MATCH (need >= 1)"

[ "$PAIR_MATCH" -ge 1 ] || { echo "FAIL: correlated pair missing in beeline" >&2; exit 1; }
[ "$RAW_MATCH" -ge 1 ] || { echo "FAIL: raw row missing in beeline" >&2; exit 1; }

echo "HDFS + HIVESERVER2 + BEELINE SMOKE OK"
