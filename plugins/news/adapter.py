import email.utils
import logging
import re
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

log = logging.getLogger("rat.news")

_FEED = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={}&region=US&lang=en-US"
_TICKER = re.compile(r"^[A-Z][A-Z.\-]{0,9}$")


def extract_symbol(text: str) -> list[str]:
    """The feed's own symbol is the entity: a headline fetched for AAPL is
    about AAPL, which is what lets it pair with a stocks quote for AAPL."""
    out = []
    for token in text.split():
        token = token.upper()
        if _TICKER.match(token) and token not in out:
            out.append(token)
    return out


def register(app):
    app.add_source("news", poll=poll)
    app.add_extractor("news", extract_symbol)


def _load_symbols() -> list[str]:
    for path in (Path.home() / ".config" / "rat" / "news.toml",
                 Path(__file__).parent / "plugin.toml"):
        if not path.exists():
            continue
        try:
            cfg = tomllib.loads(path.read_text()).get("config", {})
            if isinstance(cfg.get("symbols"), list):
                return [str(s).upper() for s in cfg["symbols"]]
        except (OSError, tomllib.TOMLDecodeError) as e:
            log.warning("failed to parse %s: %s", path, e)
    return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]


def _ts_ms(pub_date):
    try:
        return int(email.utils.parsedate_to_datetime(pub_date).timestamp() * 1000)
    except (ValueError, TypeError):
        return int(time.time() * 1000)


def _fetch(symbol: str) -> bytes:
    req = urllib.request.Request(
        _FEED.format(urllib.parse.quote(symbol)),
        headers={"User-Agent": "Mozilla/5.0 (compatible; rat-agent/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.read()


def poll(since_ms=None):
    envelopes = []
    for sym in _load_symbols():
        try:
            root = ET.fromstring(_fetch(sym))
        except (OSError, urllib.error.URLError, ET.ParseError) as err:
            log.warning("failed to fetch headlines for %s: %s", sym, err)
            continue
        for item in root.findall(".//item"):
            title = item.findtext("title")
            if not title:
                continue
            ts_ms = _ts_ms(item.findtext("pubDate"))
            if since_ms is not None and ts_ms <= since_ms:
                continue
            link = item.findtext("link")
            guid = item.findtext("guid") or link or title
            # One article can be listed under several symbols; keep one event
            # per (symbol, article) so each symbol gets its own pairing.
            envelopes.append({
                "event_id": f"news:{sym}:{guid}",
                "source": "news",
                "entities": [],
                "ts_ms": ts_ms,
                "payload": {"symbol": sym, "title": title, "link": link},
            })
    # oldest first: the runner advances the cursor per emitted event
    envelopes.sort(key=lambda e: e["ts_ms"])
    return envelopes
