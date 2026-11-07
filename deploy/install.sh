#!/usr/bin/env bash
# Install rat systemd user units and configure environment
#   ./deploy/install.sh [--broker kafka|iggy] [--start]
# kafka:          kafka + hdfs + hive + producers + spark (legacy JVM stack)
# iggy:            iggy + producers + rust engine (rat-rs consume)
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TARGET_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
RAT_CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/rat"
ENV_FILE="$RAT_CONFIG_DIR/env"

ENABLE_START=false
BROKER=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --start|--enable) ENABLE_START=true ;;
    --broker)
      if [ "$#" -lt 2 ]; then echo '--broker needs kafka or iggy' >&2; exit 1; fi
      shift; BROKER="$1" ;;
    --broker=*) BROKER="${1#--broker=}" ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
  shift
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

# Broker: --broker wins, then RAT_BROKER already in the env file, then iggy.
if [ -z "$BROKER" ]; then
  BROKER=$(sed -n 's/^RAT_BROKER=//p' "$ENV_FILE" | tail -n 1)
  BROKER=${BROKER:-iggy}
fi
case "$BROKER" in
  kafka|iggy) ;;
  *) echo "--broker must be kafka or iggy, got '$BROKER'" >&2; exit 1 ;;
esac
if grep -q "^RAT_BROKER=" "$ENV_FILE"; then
  sed -i "s/^RAT_BROKER=.*/RAT_BROKER=$BROKER/" "$ENV_FILE"
else
  echo "RAT_BROKER=$BROKER" >> "$ENV_FILE"
fi
echo "Broker: $BROKER (RAT_BROKER in $ENV_FILE)"

echo "Installing user units to $TARGET_DIR..."
for u in "$ROOT"/deploy/systemd/user/*.service; do
  [ -f "$u" ] || continue
  cp -f "$u" "$TARGET_DIR/"
  echo "  Installed $(basename "$u")"
done

if [ "$BROKER" = "iggy" ]; then
  INFRA_UNITS="rat-iggy.service"
  APP_UNITS="rat-producers.service rat-engine.service"
else
  INFRA_UNITS="rat-kafka.service rat-hdfs.service rat-hive.service"
  APP_UNITS="rat-producers.service rat-spark.service"
fi

if command -v systemctl >/dev/null 2>&1; then
  systemctl --user daemon-reload
  echo "systemd user daemon reloaded."
  if [ "$ENABLE_START" = true ]; then
    echo "Enabling and starting storage & broker services..."
    # shellcheck disable=SC2086
    systemctl --user enable --now $INFRA_UNITS
    echo "Enabling and starting producers and the stream job..."
    # shellcheck disable=SC2086
    systemctl --user enable --now $APP_UNITS
    echo "All rat services started. Check status with: systemctl --user status 'rat-*'"
  else
    echo "Units installed. To enable and start:"
    echo "  systemctl --user enable --now $INFRA_UNITS"
    echo "  systemctl --user enable --now $APP_UNITS"
  fi
fi
