#!/usr/bin/env bash
set -euo pipefail

fail=0

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing: $1"
    fail=1
    return 1
  fi
}

need java
need sbt
need uv
need python3

if command -v java >/dev/null 2>&1; then
  if ! java -version 2>&1 | grep -Eq 'version "17(\.|$)'; then
    echo "java must be 17, got:"
    java -version 2>&1 | head -n1
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
  echo "see docs/setup.md"
  exit 1
fi

echo "java    $(java -version 2>&1 | head -n1)"
echo "sbt     $(command -v sbt)"
echo "uv      $(uv --version)"
echo "python  $(python3 --version)"
