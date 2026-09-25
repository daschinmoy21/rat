import importlib.util
import io
from pathlib import Path
from unittest.mock import patch

from rat_producers.app import App

ADAPTER = Path(__file__).resolve().parents[2] / "plugins" / "news" / "adapter.py"
spec = importlib.util.spec_from_file_location("news_adapter", ADAPTER)
news = importlib.util.module_from_spec(spec)
spec.loader.exec_module(news)

FEED = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <title>Apple unveils new watch</title>
    <link>https://example.com/apple-watch</link>
    <guid isPermaLink="false">abc-123</guid>
    <pubDate>Sun, 01 Sep 2024 14:00:00 +0000</pubDate>
  </item>
  <item>
    <title>Older Apple story</title>
    <link>https://example.com/apple-old</link>
    <pubDate>Sun, 01 Sep 2024 12:00:00 +0000</pubDate>
  </item>
  <item>
    <link>https://example.com/untitled</link>
  </item>
</channel></rss>
"""


def fake_urlopen(req, timeout=None):
    return io.BytesIO(FEED.encode())


def test_register_hooks():
    app = App()
    news.register(app)
    assert "news" in app.sources
    assert app.extractors["news"] is news.extract_symbol


def test_extract_symbol():
    assert news.extract_symbol("AAPL") == ["AAPL"]
    assert news.extract_symbol("brk.b") == ["BRK.B"]
    assert news.extract_symbol("") == []
    assert news.extract_symbol("123") == []


def test_poll_maps_items_oldest_first():
    with patch.object(news.urllib.request, "urlopen", fake_urlopen), \
         patch.object(news, "_load_symbols", return_value=["AAPL"]):
        envs = news.poll()

    # untitled item is dropped; sorted ascending by event time
    assert [e["event_id"] for e in envs] == [
        "news:AAPL:https://example.com/apple-old",
        "news:AAPL:abc-123",
    ]
    env = envs[1]
    assert env["source"] == "news"
    assert env["entities"] == []
    assert env["ts_ms"] == 1725199200000
    assert env["payload"] == {
        "symbol": "AAPL",
        "title": "Apple unveils new watch",
        "link": "https://example.com/apple-watch",
    }


def test_poll_same_article_per_symbol():
    with patch.object(news.urllib.request, "urlopen", fake_urlopen), \
         patch.object(news, "_load_symbols", return_value=["AAPL", "MSFT"]):
        envs = news.poll(since_ms=1725195600000)
    assert sorted(e["event_id"] for e in envs) == ["news:AAPL:abc-123", "news:MSFT:abc-123"]


def test_poll_filters_since_ms():
    with patch.object(news.urllib.request, "urlopen", fake_urlopen), \
         patch.object(news, "_load_symbols", return_value=["AAPL"]):
        assert news.poll(since_ms=1725199200000) == []


def test_poll_skips_failing_symbol():
    def flaky(req, timeout=None):
        if "s=MSFT" in req.full_url:
            raise OSError("boom")
        return io.BytesIO(FEED.encode())

    with patch.object(news.urllib.request, "urlopen", flaky), \
         patch.object(news, "_load_symbols", return_value=["MSFT", "AAPL"]):
        envs = news.poll()
    assert {e["payload"]["symbol"] for e in envs} == {"AAPL"}
