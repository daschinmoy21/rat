#!/usr/bin/env bash
# Install rat systemd user units and configure environment
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TARGET_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
RAT_CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/rat"
ENV_FILE="$RAT_CONFIG_DIR/env"

ENABLE_START=false
for arg in "$@"; do
  case "$arg" in
    --start|--enable) ENABLE_START=true ;;
    *) echo "Unknown option: $arg" >&2; exit 1 ;;
  esac
done

mkdir -p "$TARGET_DIR" "$RAT_CONFIG_DIR"

if [ ! -f "$ENV_FILE" ]; then
  cat <<EOF > "$ENV_FILE"
# Environment configuration for rat services
RAT_ROOT=$ROOT
EOF
  echo "Created $ENV_FILE (RAT_ROOT=$ROOT)"
else
  if ! grep -q "^RAT_ROOT=" "$ENV_FILE"; then
    echo "RAT_ROOT=$ROOT" >> "$ENV_FILE"
    echo "Appended RAT_ROOT=$ROOT to $ENV_FILE"
  fi
fi

echo "Installing user units to $TARGET_DIR..."
for u in "$ROOT"/deploy/systemd/user/*.service; do
  [ -f "$u" ] || continue
  cp -f "$u" "$TARGET_DIR/"
  echo "  Installed $(basename "$u")"
done

if command -v systemctl >/dev/null 2>&1; then
  systemctl --user daemon-reload
  echo "systemd user daemon reloaded."
  if [ "$ENABLE_START" = true ]; then
    echo "Enabling and starting storage & broker services..."
    systemctl --user enable --now rat-kafka.service rat-hdfs.service rat-hive.service
    echo "Enabling and starting producers and Spark job..."
    systemctl --user enable --now rat-producers.service rat-spark.service
    echo "All rat services started. Check status with: systemctl --user status 'rat-*'"
  else
    echo "Units installed. To enable and start:"
    echo "  systemctl --user enable --now rat-kafka.service rat-hdfs.service rat-hive.service"
    echo "  systemctl --user enable --now rat-producers.service rat-spark.service"
  fi
fi
