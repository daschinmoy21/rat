#!/bin/sh
# Run a command inside the repo's nix develop shell; bare when nix is absent.
# Used by the systemd user units so services see the same tools as the shell.
# Falls back directly to exec "$@" when Nix or flake.nix is not detected.
set -eu

ROOT=${RAT_ROOT:-$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)}

if command -v nix >/dev/null 2>&1 && [ -f "$ROOT/flake.nix" ]; then
  # JVM service units select this explicitly; the default shell is Rust-only.
  FLAKE="$ROOT${RAT_NIX_SHELL:+#$RAT_NIX_SHELL}"
  if [ "$#" -eq 0 ]; then
    exec nix develop "$FLAKE"
  fi
  exec nix develop "$FLAKE" -c "$@"
fi
exec "$@"
