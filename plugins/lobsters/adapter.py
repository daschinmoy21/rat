import http.client
import json
import logging
import urllib.request
from datetime import datetime

from rat_producers.extract import extract_entities

log = logging.getLogger("rat.lobsters")


def register(app):
    app.add_source("lobsters", poll=poll)
    app.add_extractor("lobsters", extract_entities)


def poll(since_ms=None):
    req = urllib.request.Request(
        "https://lobste.rs/hottest.json",
        headers={"User-Agent": "Mozilla/5.0 (compatible; rat-agent/1.0)"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            stories = json.load(r)
    except (OSError, ValueError, http.client.HTTPException) as err:
        log.warning("failed to fetch lobsters hottest: %s", err)
        return []
    if not isinstance(stories, list):
        log.warning("invalid lobsters response: expected a list")
        return []

    envelopes = []
    for s in stories:
        try:
            ts_ms = int(datetime.fromisoformat(s["created_at"]).timestamp() * 1000)
            if since_ms is not None and ts_ms <= since_ms:
                continue
            envelopes.append({
                "event_id": f"lobsters:{s['short_id']}",
                "source": "lobsters",
                "entities": [],
                "ts_ms": ts_ms,
                "payload": {
                    "title": s["title"],
                    "url": s.get("url") or None,
                    "score": s.get("score"),
                    "tags": s.get("tags") or [],
                },
            })
        except (KeyError, ValueError, TypeError) as err:
            log.warning("skipping malformed lobsters story %r: %s", s, err)
    envelopes.sort(key=lambda e: e["ts_ms"])
    return envelopes
