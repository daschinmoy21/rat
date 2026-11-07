#!/usr/bin/env bash
# Verify Rust stack prerequisites. --legacy selects the original JVM stack.
set -euo pipefail

fail=0
legacy=false
case "${1:-}" in
  --legacy) legacy=true ;;
  '') ;;
  *) echo 'usage: check-deps.sh [--legacy]' >&2; exit 1 ;;
esac

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing: $1"
    fail=1
  fi
}

need uv
need python3
if "$legacy"; then
  need java
  need sbt
else
  need cargo
  need rustc
fi

if "$legacy" && command -v java >/dev/null 2>&1; then
  if ! java -version 2>&1 | grep -Eq 'version "17(\.|$)'; then
    echo "java must be 17, got:"
    java -version 2>&1 | head -n1
    fail=1
  fi
fi

if ! "$legacy" && command -v rustc >/dev/null 2>&1 && command -v python3 >/dev/null 2>&1; then
  if ! rustc --version | python3 -c 'import sys; v=sys.stdin.read().split()[1].split("-")[0]; sys.exit(0 if tuple(map(int,v.split(".")[:2])) >= (1,97) else 1)'; then
    echo 'Rust 1.97+ is required'
    fail=1
  fi
fi

if command -v python3 >/dev/null 2>&1; then
  if ! python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)'; then
    echo "python must be 3.12+, got: $(python3 --version)"
    fail=1
  fi
fi

if [ "$fail" -ne 0 ]; then
  echo "see docs/rust-migration.md (legacy: docs/setup.md)"
  exit 1
fi

if "$legacy"; then
  echo "java    $(java -version 2>&1 | head -n1)"
  echo "sbt     $(command -v sbt)"
else
  echo "rust    $(rustc --version)"
  echo "cargo   $(cargo --version)"
fi
echo "uv      $(uv --version)"
echo "python  $(python3 --version)"
