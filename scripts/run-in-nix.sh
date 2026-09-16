#!/bin/sh
# Run a command inside the repo's nix develop shell; bare when nix is absent.
# Used by the systemd user units so services see the same tools as the shell.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

if command -v nix >/dev/null 2>&1 && [ -f "$ROOT/flake.nix" ]; then
  if [ "$#" -eq 0 ]; then
    exec nix develop "$ROOT"
  fi
  exec nix develop "$ROOT" -c "$@"
fi
exec "$@"
