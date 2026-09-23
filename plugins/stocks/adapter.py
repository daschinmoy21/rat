import json
import logging
import re
import time
import tomllib
import urllib.request
from pathlib import Path

log = logging.getLogger("rat.stocks")

_CASHTAG = re.compile(r"\$([A-Za-z]{1,5})\b")


_STOP_WORDS = {
    "A", "AN", "THE", "AND", "OR", "BUT", "IF", "OF", "AT", "BY", "FOR",
    "IN", "ON", "TO", "FROM", "WITH", "IS", "ARE", "WAS", "WERE", "BE",
    "BEEN", "BEING", "HAVE", "HAS", "HAD", "DO", "DOES", "DID", "CAN",
    "COULD", "SHOULD", "WOULD", "WILL", "SHALL", "MAY", "MIGHT", "MUST",
    "NOT", "NO", "SO", "ALL", "ANY", "SOME", "MORE", "MOST", "OTHER",
    "INTO", "OVER", "AFTER", "THEN", "NOW", "JUST", "ALSO", "HOW", "OUT",
    "UP", "DOWN", "BUY", "SELL", "HOLD", "NEWS", "TODAY", "WEEK", "YEAR",
}


def extract_stocks(text: str) -> list[str]:
    """Extract stock symbols from text.

    Handles cashtags ($AAPL) and standalone tickers (AAPL), filtering common words.
    """
    seen: set[str] = set()
    out: list[str] = []
    cashtags = _CASHTAG.findall(text)
    if cashtags:
        for tag in cashtags:
            u = tag.upper()
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out

    for token in re.split(r"[\s,;]+", text):
        clean = token.strip("$.,;()[]\"'").upper()
        if (
            re.match(r"^[A-Z]{1,5}$", clean)
            and clean not in _STOP_WORDS
            and clean not in seen
        ):
            seen.add(clean)
            out.append(clean)
    return out


def register(app):
    app.add_source("stocks", poll=poll)
    app.add_extractor("stocks", extract_stocks)


def _load_symbols() -> list[str]:
    user_conf = Path.home() / ".config" / "rat" / "stocks.toml"
    if user_conf.exists():
        try:
            data = tomllib.loads(user_conf.read_text())
            cfg = data.get("config", {})
            if "symbols" in cfg and isinstance(cfg["symbols"], list):
                return [str(s).upper() for s in cfg["symbols"]]
        except (OSError, tomllib.TOMLDecodeError) as e:
            log.warning("failed to parse %s: %s", user_conf, e)

    manifest_path = Path(__file__).parent / "plugin.toml"
    if manifest_path.exists():
        try:
            data = tomllib.loads(manifest_path.read_text())
            cfg = data.get("config", {})
            if "symbols" in cfg and isinstance(cfg["symbols"], list):
                return [str(s).upper() for s in cfg["symbols"]]
        except (OSError, tomllib.TOMLDecodeError) as e:
            log.warning("failed to parse %s: %s", manifest_path, e)

    return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]


def _fetch_quote(symbol: str) -> dict | None:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (compatible; rat-agent/1.0)"}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        data = json.load(r)
    result = data.get("chart", {}).get("result")
    if not result:
        return None
    meta = result[0].get("meta", {})
    price = meta.get("regularMarketPrice")
    if price is None:
        return None
    ts = meta.get("regularMarketTime", int(time.time()))
    return {
        "symbol": meta.get("symbol", symbol),
        "price": float(price),
        "currency": meta.get("currency", "USD"),
        "ts_ms": int(ts * 1000),
    }


def poll(since_ms=None):
    symbols = _load_symbols()
    envelopes = []
    for sym in symbols:
        try:
            quote = _fetch_quote(sym)
            if not quote:
                continue
            ts_ms = quote["ts_ms"]
            if since_ms is not None and ts_ms <= since_ms:
                continue
            envelopes.append({
                "event_id": f"stocks:{sym}:{ts_ms}",
                "source": "stocks",
                "entities": [],
                "ts_ms": ts_ms,
                "payload": {
                    "symbol": quote["symbol"],
                    "price": quote["price"],
                    "currency": quote["currency"],
                },
            })
        except (OSError, urllib.error.URLError, json.JSONDecodeError, KeyError, IndexError, ValueError) as err:
            log.warning("failed to fetch quote for %s: %s", sym, err)
    return envelopes
