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

FEED2 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>$AAPL record highs</title>
    <link>https://example.com/apple</link>
    <guid>https://example.com/apple#id</guid>
    <pubDate>Sun, 01 Sep 2024 14:00:00 GMT</pubDate>
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
    assert env["entities"] == []  # core fills entities from from_fields
    assert env["ts_ms"] == 1725192000000
    assert env["payload"] == {"title": "$TSLA rebounds",
                              "link": "https://example.com/tesla"}
    assert set(env) == {"event_id", "source", "entities", "ts_ms", "payload"}


def test_poll_filters_since_ms():
    with patch.object(rss.urllib.request, "urlopen", fake_urlopen):
        envs = rss.poll(since_ms=1725192000000)  # first item's ts
    assert [e["event_id"] for e in envs] == ["rss:https://example.com/noguid"]


def test_poll_multiple_feeds():
    def multi_urlopen(req, timeout=None):
        if "feed2" in req.full_url:
            return io.BytesIO(FEED2.encode())
        return io.BytesIO(FEED.encode())

    with patch.object(rss.urllib.request, "urlopen", multi_urlopen), \
         patch.object(rss, "_feed_urls", return_value=["https://example.com/feed1", "https://example.com/feed2"]):
        envs = rss.poll()

    assert len(envs) == 3
    assert [e["event_id"] for e in envs] == [
        "rss:https://example.com/tesla#id",
        "rss:https://example.com/noguid",
        "rss:https://example.com/apple#id",
    ]


def test_poll_isolates_feed_failure():
    def partial_fail_urlopen(req, timeout=None):
        if "failing" in req.full_url:
            raise OSError("Feed down")
        return io.BytesIO(FEED.encode())

    with patch.object(rss.urllib.request, "urlopen", partial_fail_urlopen), \
         patch.object(rss, "_feed_urls", return_value=["https://example.com/failing", "https://example.com/ok"]):
        envs = rss.poll()

    assert len(envs) == 2
    assert envs[0]["event_id"] == "rss:https://example.com/tesla#id"


def test_user_config_override_rss(tmp_path):
    with patch("pathlib.Path.home", return_value=tmp_path):
        config_dir = tmp_path / ".config" / "rat"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / "rss.toml").write_text('[config]\nurls = ["https://custom.feed/rss"]\n')
        urls = rss._feed_urls()
        assert urls == ["https://custom.feed/rss"]
