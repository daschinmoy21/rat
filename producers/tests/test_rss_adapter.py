import importlib.util
import io
from pathlib import Path
from unittest.mock import patch

ADAPTER = Path(__file__).resolve().parents[2] / "plugins" / "rss" / "adapter.py"
spec = importlib.util.spec_from_file_location("rss_adapter", ADAPTER)
rss = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rss)

FEED = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>$TSLA rebounds</title>
    <link>https://example.com/tesla</link>
    <guid>https://example.com/tesla#id</guid>
    <pubDate>Sun, 01 Sep 2024 12:00:00 GMT</pubDate>
  </item>
  <item>
    <title>no guid here</title>
    <link>https://example.com/noguid</link>
    <pubDate>Sun, 01 Sep 2024 13:00:00 GMT</pubDate>
  </item>
</channel></rss>
"""


def fake_urlopen(req, timeout=None):
    return io.BytesIO(FEED.encode())


def test_poll_maps_items_to_envelopes():
    with patch.object(rss.urllib.request, "urlopen", fake_urlopen):
        envs = rss.poll()
    assert [e["event_id"] for e in envs] == [
        "rss:https://example.com/tesla#id",
        "rss:https://example.com/noguid",  # guid missing -> link fallback
    ]
    env = envs[0]
    assert env["source"] == "rss"
    assert env["entities"] == ["TSLA"]
    assert env["ts_ms"] == 1725192000000
    assert env["payload"] == {"title": "$TSLA rebounds",
                              "link": "https://example.com/tesla"}
    assert set(env) == {"event_id", "source", "entities", "ts_ms", "payload"}


def test_poll_filters_since_ms():
    with patch.object(rss.urllib.request, "urlopen", fake_urlopen):
        envs = rss.poll(since_ms=1725192000000)  # first item's ts
    assert [e["event_id"] for e in envs] == ["rss:https://example.com/noguid"]
