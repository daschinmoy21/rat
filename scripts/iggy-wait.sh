#!/bin/sh
# Wait until the Iggy HTTP API answers GET /ping with "pong".
#   RAT_IGGY_URL (default http://127.0.0.1:3000), RAT_IGGY_WAIT_S (default 120)
set -eu

URL=${RAT_IGGY_URL:-http://127.0.0.1:3000}
WAIT_S=${RAT_IGGY_WAIT_S:-120}

i=0
until python3 -c 'import sys, urllib.request as u
sys.exit(0 if u.urlopen(sys.argv[1] + "/ping", timeout=2).read().strip() == b"pong" else 1)' \
    "$URL" 2>/dev/null; do
  i=$((i + 1))
  if [ "$i" -ge "$WAIT_S" ]; then
    echo "iggy at $URL never answered /ping within ${WAIT_S}s" >&2
    exit 1
  fi
  sleep 1
done
echo "iggy at $URL is up"
