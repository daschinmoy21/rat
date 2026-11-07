#!/usr/bin/env bash
# Reproducible plugin benchmark. Offline: no live feed, no Kafka, no services.
#
#   tools/plugin-bench/run.sh [OUT_DIR]    (default docs/benchmarks/plugin-<date>)
#
# Tools come from the nixpkgs revision pinned in flake.lock, so Python is the
# same 3.12.x the .venv was built with; Lua 5.4 and LuaJIT come from the same
# revision. Python deps (jsonschema, kafka-python) come from .venv, so run
# `uv sync --frozen` first. Override with PY= LUA54= LUAJIT= to use other builds.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BENCH="$ROOT/tools/plugin-bench"
OUT="${1:-$ROOT/docs/benchmarks/plugin-$(date +%Y-%m-%d)}"
REV="$(sed -n '/"nixpkgs": {/,/"rev"/s/.*"rev": "\([0-9a-f]*\)".*/\1/p' "$ROOT/flake.lock" | head -1)"
NP="github:NixOS/nixpkgs/$REV"
build() { nix build --no-link --print-out-paths "$NP#$1" | tail -1; }
PY="${PY:-$(build python312)/bin/python3.12}"
LUA54="${LUA54:-$(build lua5_4)/bin/lua}"
LUAJIT="${LUAJIT:-$(build luajit)/bin/luajit}"
SITE="$(echo "$ROOT"/.venv/lib/python3.12/site-packages)"
[ -d "$SITE" ] || { echo "missing $SITE: run uv sync --frozen" >&2; exit 1; }

mkdir -p "$OUT"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT
mkdir -p "$SCRATCH/home" "$SCRATCH/src"
# Measure a committed tree, not whatever is half-edited in the worktree.
REF="${BENCH_REF:-HEAD}"
git -C "$ROOT" archive "$REF" producers plugins | tar -x -C "$SCRATCH/src"
SRC="$SCRATCH/src"
export PY LUA54 LUAJIT BENCH
export PYTHONPATH="$SITE:$SRC/producers:$BENCH"
export PYTHONPYCACHEPREFIX="$SCRATCH/pycache"   # never write __pycache__ into the repo
export RAT_BENCH_HOME="$SCRATCH/home"           # Python harness isolates Path.home()
export RAT_PLUGINS_DIR="$SRC/plugins"
export LANG=C LC_ALL=C
export FIXTURES_LUA="$SCRATCH/fixtures.lua"
"$PY" "$BENCH/fixtures.py" "$SCRATCH" | tee "$OUT/fixtures.txt"

{
  echo "date_utc: $(date -u +%FT%TZ)"
  echo "nixpkgs_rev: $REV"
  echo "python: $("$PY" -VV)"
  echo "lua54: $("$LUA54" -v 2>&1)"
  echo "luajit: $("$LUAJIT" -v 2>&1)"
  echo "kernel: $(uname -srm)"
  echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
  echo "nproc: $(nproc)"
  echo "mem: $(free -m | awk '/Mem:/{print $2" MiB total, "$7" MiB available"}')"
  echo "loadavg_at_start: $(cut -d' ' -f1-3 /proc/loadavg)"
  echo "bench_ref: $REF = $(git -C "$ROOT" rev-parse "$REF") (producers/ and plugins/ via git archive)"
  echo "worktree_dirty_paths: $(git -C "$ROOT" status --short | wc -l) (not measured)"
  echo "venv: jsonschema $("$PY" -c 'import importlib.metadata as m; print(m.version("jsonschema"))'), kafka-python $("$PY" -c 'import importlib.metadata as m; print(m.version("kafka-python"))')"
} > "$OUT/env.txt"
cat "$OUT/env.txt"

if [ "${SKIP_MLUA:-0}" != 1 ]; then
  echo "== embedded mlua probe (Lua 5.4 and LuaJIT, built from crates.io)"
  PY="$PY" "$BENCH/run-mlua.sh" "$OUT"
fi
export MLUA_DIR="$OUT"

echo "== correctness: Lua port vs Python"
"$PY" "$BENCH/extract_diff.py" "$OUT/extract-diff.json" | tee "$OUT/extract-diff.txt"

echo "== microbenchmark: in-process transform (Python)"
"$PY" "$BENCH/py_transform.py" "$OUT/py-transform.json" | tee "$OUT/py-transform.txt"

echo "== microbenchmark: in-process transform (Lua)"
for L in "$LUA54" "$LUAJIT"; do
  "$L" "$BENCH/transform.lua" "$FIXTURES_LUA" bench "${REPS:-300}"
done | tee "$OUT/lua-transform.jsonl"

echo "== Python import profile (-X importtime, load_app; 3 warm runs, last kept)"
for _ in 1 2 3; do
  "$PY" -X importtime -c 'from pathlib import Path; import os; Path.home=classmethod(lambda cls: Path(os.environ["RAT_BENCH_HOME"])); from rat_producers.cli import load_app; load_app()' \
    2> "$OUT/py-importtime.txt"
done
sort -t'|' -k2 -n -r "$OUT/py-importtime.txt" | awk 'NR<=15' 

echo "== process spawn: wall / cpu / peak RSS"
"$PY" "$BENCH/spawn.py" "$OUT/spawn.json" | tee "$OUT/spawn.txt"

echo "== loopback end-to-end (Python only; Lua has no HTTP client in its stdlib)"
"$PY" "$BENCH/loopback.py" "$OUT/loopback.json" | tee "$OUT/loopback.txt"

echo "loadavg_at_end: $(cut -d' ' -f1-3 /proc/loadavg)" >> "$OUT/env.txt"
echo "wrote $OUT"
