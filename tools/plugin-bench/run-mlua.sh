#!/usr/bin/env bash
# Build the standalone mlua probe for Lua 5.4 and LuaJIT, run it on the same
# decoded fixtures as transform.lua. Needs a Rust toolchain (>= 1.88, mlua's
# MSRV) and crates.io access for the first build. Target dir is outside the
# repo; the repo's own Cargo workspace is not touched.
#
#   tools/plugin-bench/run-mlua.sh OUT_DIR
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BENCH="$ROOT/tools/plugin-bench"
OUT="$1"; mkdir -p "$OUT"
CARGO="${CARGO:-$(command -v cargo || echo "$HOME/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/cargo")}"
export PATH="$(dirname "$CARGO"):$PATH"
TGT="${CARGO_TARGET_DIR:-${TMPDIR:-/tmp}/plugin-bench-mlua-target}"
PY="${PY:-python3}"
FX="$(mktemp -d)"; trap 'rm -rf "$FX"' EXIT
"$PY" "$BENCH/fixtures.py" "$FX" >/dev/null
"$CARGO" --version > "$OUT/mlua-env.txt"
for f in lua54 luajit; do
  "$CARGO" build --release --quiet --manifest-path "$BENCH/mlua-host/Cargo.toml" \
    --target-dir "$TGT/$f" --no-default-features --features "$f"
  "$TGT/$f/release/plugin-bench-mlua-host" "$BENCH/transform.lua" "$FX/fixtures.json" \
    "${REPS:-300}" "$OUT/mlua-$f-envelopes.jsonl" | tee "$OUT/mlua-$f.json"
done
cp "$BENCH/mlua-host/Cargo.lock" "$OUT/mlua-Cargo.lock" 2>/dev/null || true
