"""Synthetic envelopes for the end-to-end smoke.

A $AAPL trio (one per source — hn, rss, stocks — 30s steps, inside the
watermark window) plus one entity-less row for the raw path. The third
source pins the any-source correlate: a hard-coded hn/rss join would
still emit the hn-rss pair but drop the two stocks pairs. Event ids
carry the run timestamp, so reruns add fresh pairs instead of
duplicating. Prints the timestamp for the smoke script's assertion.
"""

import sys
import time

from rat_producers.producer import connect, emit


def envelope(event_id, source, ts_ms, entities, payload):
    return {
        "event_id": event_id,
        "source": source,
        "entities": entities,
        "ts_ms": ts_ms,
        "payload": payload,
    }


def main():
    ts = int(time.time() * 1000)
    producer = connect()
    try:
        emit(envelope(
            f"smoke:hn:{ts}", "hn", ts, ["AAPL"],
            {"title": "Smoke $AAPL headline", "url": "https://example.com/smoke-hn",
             "score": 1},
        ), producer)
        emit(envelope(
            f"smoke:rss:{ts}", "rss", ts + 30_000, ["AAPL"],
            {"title": "Smoke $AAPL follow-up", "link": "https://example.com/smoke-rss"},
        ), producer)
        emit(envelope(
            f"smoke:stocks:{ts}", "stocks", ts + 60_000, ["AAPL"],
            {"symbol": "AAPL", "price": 250.0},
        ), producer)
        emit(envelope(
            f"smoke:raw:{ts}", "rss", ts, [],
            {"title": "Smoke raw-only row", "link": "https://example.com/smoke-raw"},
        ), producer)
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)
    print(ts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
