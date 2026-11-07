"""Deterministic, synthetic upstream responses for the five in-tree plugins.

Nothing here is fetched from a live feed. Sizes follow the adapters' own code
where the code fixes them (hn takes topstories[:30]; news/stocks poll the five
symbols in their plugin.toml); elsewhere they are round guesses (RSS_ITEMS,
NEWS_ITEMS, LOBSTERS_STORIES) and are reported as such, not as real feed sizes.

`responses()` maps URL path (host stripped) -> bytes, and is shared by the
in-process microbenchmark and the loopback HTTP server. `lua_tables()` writes
the same data, already decoded, as a Lua table literal: the shape a Rust host
(serde_json / quick-xml -> mlua tables) would hand an embedded Lua plugin.
"""

import json
from email.utils import formatdate
from xml.sax.saxutils import escape

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # plugins/{news,stocks}/plugin.toml
HN_TOP = 30            # plugins/hn/adapter.py: ids[:30]
RSS_ITEMS = 30         # guess
NEWS_ITEMS = 20        # guess, per symbol
LOBSTERS_STORIES = 25  # guess
BASE_TS = 1_725_192_000  # 2024-09-01T12:00:00Z


def _title(i):
    # mix of cashtags, bare tickers and stop words so every extractor does work
    words = ["$AAPL beats estimates", "MSFT and the cloud", "Why $TSLA fell",
             "Rust 2024 edition", "GOOGL antitrust ruling", "A new $AMZN warehouse",
             "Show HN: a tiny Lua VM", "NVDA, AMD; INTC (chips)"]
    return f"{words[i % len(words)]} #{i}"


def rss_xml(n, prefix):
    items = []
    for i in range(n):
        items.append(
            "<item>"
            f"<title>{escape(_title(i))}</title>"
            f"<link>https://example.com/{prefix}/{i}</link>"
            f"<guid>https://example.com/{prefix}/{i}#g</guid>"
            f"<pubDate>{formatdate(BASE_TS + i * 60, usegmt=True)}</pubDate>"
            "</item>")
    return ('<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>'
            + "".join(items) + "</channel></rss>").encode()


def hn_items():
    out = {}
    for i in range(HN_TOP):
        iid = 40_000_000 + i
        kind = "job" if i % 10 == 9 else "story"
        out[iid] = {"id": iid, "type": kind, "time": BASE_TS + i * 60, "by": "u",
                    "title": _title(i), "url": f"https://example.com/hn/{i}",
                    "score": 100 + i, "descendants": i}
    return out


def chart(sym, i):
    return {"chart": {"result": [{"meta": {
        "currency": "USD", "symbol": sym, "regularMarketPrice": 100.0 + i * 1.25,
        "regularMarketTime": BASE_TS + i, "exchangeName": "NMS"}}], "error": None}}


def lobsters():
    return [{"short_id": f"ab{i:03d}", "title": _title(i),
             "url": f"https://example.com/l/{i}" if i % 4 else "",
             "score": i, "tags": ["rust", "lua"][: i % 3],
             "created_at": f"2024-09-01T12:{i % 60:02d}:00.000-05:00"}
            for i in range(LOBSTERS_STORIES)]


def responses():
    r = {"/frontpage": rss_xml(RSS_ITEMS, "rss"),
         "/v0/topstories.json": json.dumps(sorted(hn_items())).encode(),
         "/hottest.json": json.dumps(lobsters()).encode()}
    for iid, item in hn_items().items():
        r[f"/v0/item/{iid}.json"] = json.dumps(item).encode()
    for i, sym in enumerate(SYMBOLS):
        r[f"/v8/finance/chart/{sym}"] = json.dumps(chart(sym, i)).encode()
        r[f"/rss/2.0/headline?s={sym}&region=US&lang=en-US"] = rss_xml(NEWS_ITEMS, f"news/{sym}")
    return r


def path_of(url):
    """URL -> fixture key: drop scheme+host, keep path and query."""
    rest = url.split("://", 1)[-1]
    return "/" + rest.split("/", 1)[1] if "/" in rest else "/"


def _lua(v):
    if v is None:
        return "nil"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return json.dumps(v)  # JSON string escapes are valid Lua for this ASCII data
    if isinstance(v, list):
        return "{" + ",".join(_lua(x) for x in v) + "}"
    return "{" + ",".join(f"[{json.dumps(k)}]={_lua(x)}" for k, x in v.items()) + "}"


def _rss_items(n, prefix):
    return [{"title": _title(i), "link": f"https://example.com/{prefix}/{i}",
             "guid": f"https://example.com/{prefix}/{i}#g",
             "pubDate": formatdate(BASE_TS + i * 60, usegmt=True)} for i in range(n)]


def decoded_data():
    return {
        "rss": _rss_items(RSS_ITEMS, "rss"),
        "hn": [hn_items()[k] for k in sorted(hn_items())],
        "stocks": [chart(s, i) for i, s in enumerate(SYMBOLS)],
        "news": {s: _rss_items(NEWS_ITEMS, f"news/{s}") for s in SYMBOLS},
        "lobsters": lobsters(),
        "symbols": SYMBOLS,
    }


def lua_tables():
    """Pre-decoded inputs, as a Lua chunk returning one table."""
    return "return " + _lua(decoded_data()) + "\n"


if __name__ == "__main__":
    import sys
    from pathlib import Path
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    (out / "fixtures.lua").write_text(lua_tables())
    (out / "fixtures.json").write_text(json.dumps(decoded_data()))
    total = sum(len(v) for v in responses().values())
    print(f"{len(responses())} responses, {total} bytes")
