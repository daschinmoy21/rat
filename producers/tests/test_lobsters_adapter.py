import importlib.util
import io
import json
from pathlib import Path
from unittest.mock import patch

from rat_producers.app import App

ADAPTER = Path(__file__).resolve().parents[2] / "plugins" / "lobsters" / "adapter.py"
spec = importlib.util.spec_from_file_location("lobsters_adapter", ADAPTER)
lobsters = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lobsters)

STORIES = [
    {"short_id": "b2", "title": "Newer $NVDA story", "url": "https://example.com/b",
     "created_at": "2024-09-01T09:00:00.000-05:00", "score": 12, "tags": ["hardware"]},
    {"short_id": "a1", "title": "Ask: text post", "url": "",
     "created_at": "2024-09-01T08:00:00.000-05:00", "score": 3, "tags": ["ask"]},
]


def fake_urlopen(req, timeout=None):
    return io.BytesIO(json.dumps(STORIES).encode())


def test_register_hooks():
    app = App()
    lobsters.register(app)
    assert "lobsters" in app.sources
    assert "lobsters" in app.extractors


def test_poll_maps_stories_oldest_first():
    with patch.object(lobsters.urllib.request, "urlopen", fake_urlopen):
        envs = lobsters.poll()
    assert [e["event_id"] for e in envs] == ["lobsters:a1", "lobsters:b2"]
    assert envs[0]["payload"]["url"] is None  # text posts have an empty url
    assert envs[1]["ts_ms"] == 1725199200000
    assert envs[1]["payload"] == {
        "title": "Newer $NVDA story",
        "url": "https://example.com/b",
        "score": 12,
        "tags": ["hardware"],
    }


def test_poll_filters_since_ms():
    with patch.object(lobsters.urllib.request, "urlopen", fake_urlopen):
        envs = lobsters.poll(since_ms=1725195600000)
    assert [e["event_id"] for e in envs] == ["lobsters:b2"]
