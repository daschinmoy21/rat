import email.utils
import logging
import time
import tomllib
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

from rat_producers.extract import extract_entities

log = logging.getLogger("rat.rss")


def register(app):
    app.add_source("rss", poll=poll)
    app.add_extractor("rss", extract_entities)


def _feed_urls() -> list[str]:
    # Check user config override in ~/.config/rat/rss.toml first
    user_conf = Path.home() / ".config" / "rat" / "rss.toml"
    if user_conf.exists():
        try:
            data = tomllib.loads(user_conf.read_text())
            cfg = data.get("config", {})
            if "urls" in cfg and isinstance(cfg["urls"], list):
                return [str(u) for u in cfg["urls"]]
            if "url" in cfg and isinstance(cfg["url"], str):
                return [cfg["url"]]
        except (OSError, tomllib.TOMLDecodeError) as e:
            log.warning("failed to parse %s: %s", user_conf, e)

    manifest_path = Path(__file__).parent / "plugin.toml"
    if manifest_path.exists():
        try:
            manifest = tomllib.loads(manifest_path.read_text())
            cfg = manifest.get("config", {})
            if "urls" in cfg and isinstance(cfg["urls"], list):
                return [str(u) for u in cfg["urls"]]
            if "url" in cfg and isinstance(cfg["url"], str):
                return [cfg["url"]]
        except (OSError, tomllib.TOMLDecodeError) as e:
            log.warning("failed to parse %s: %s", manifest_path, e)

    return ["https://hnrss.org/frontpage"]


def _ts_ms(pub_date):
    if not pub_date:
        return int(time.time() * 1000)
    try:
        return int(email.utils.parsedate_to_datetime(pub_date).timestamp() * 1000)
    except (ValueError, TypeError):
        return int(time.time() * 1000)


def poll(since_ms=None):
    urls = _feed_urls()
    envelopes = []
    for url in urls:
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0 (compatible; rat-agent/1.0)"}
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                root = ET.fromstring(r.read())
            for item in root.findall(".//item"):
                ts_ms = _ts_ms(item.findtext("pubDate"))
                if since_ms is not None and ts_ms <= since_ms:
                    continue
                title, link = item.findtext("title"), item.findtext("link")
                guid = item.findtext("guid") or link
                envelopes.append({
                    "event_id": f"rss:{guid}",
                    "source": "rss",
                    "entities": [],
                    "ts_ms": ts_ms,
                    "payload": {"title": title, "link": link},
                })
        except (OSError, urllib.error.URLError, ET.ParseError) as err:
            log.warning("failed to fetch RSS feed %s: %s", url, err)
    return envelopes
