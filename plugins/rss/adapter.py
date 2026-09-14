import email.utils
import tomllib
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

from rat_producers.extract import extract_entities


def register(app):
    app.add_source("rss", poll=poll)


def _feed_url():
    manifest = tomllib.loads((Path(__file__).parent / "plugin.toml").read_text())
    return manifest["config"]["url"]


def _ts_ms(pub_date):
    return int(email.utils.parsedate_to_datetime(pub_date).timestamp() * 1000)


def poll(since_ms=None):
    with urllib.request.urlopen(_feed_url()) as r:
        root = ET.fromstring(r.read())
    envelopes = []
    for item in root.findall(".//item"):
        ts_ms = _ts_ms(item.findtext("pubDate"))
        if since_ms is not None and ts_ms <= since_ms:
            continue
        title, link = item.findtext("title"), item.findtext("link")
        guid = item.findtext("guid") or link
        envelopes.append({
            "event_id": f"rss:{guid}",
            "source": "rss",
            "entities": extract_entities(f"{title or ''} {link or ''}"),
            "ts_ms": ts_ms,
            "payload": {"title": title, "link": link},
        })
    return envelopes
