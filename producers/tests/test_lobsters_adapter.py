import http.client
import importlib.util
import io
import json
from pathlib import Path
from unittest.mock import patch

from rat_producers.app import App
from rat_producers.schema import load_validator

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


def test_poll_handles_fetch_failure():
    def failing_urlopen(req, timeout=None):
        raise OSError("connection refused")

    with patch.object(lobsters.urllib.request, "urlopen", failing_urlopen):
        assert lobsters.poll() == []


def test_poll_handles_incomplete_response():
    def incomplete_urlopen(req, timeout=None):
        raise http.client.IncompleteRead(b"partial", 10)

    with patch.object(lobsters.urllib.request, "urlopen", incomplete_urlopen):
        assert lobsters.poll() == []


def test_poll_handles_invalid_json():
    def invalid_json_urlopen(req, timeout=None):
        return io.BytesIO(b"not json")

    with patch.object(lobsters.urllib.request, "urlopen", invalid_json_urlopen):
        assert lobsters.poll() == []


def test_poll_handles_non_list_response():
    def non_list_urlopen(req, timeout=None):
        return io.BytesIO(json.dumps({"error": "rate limited"}).encode())

    with patch.object(lobsters.urllib.request, "urlopen", non_list_urlopen):
        assert lobsters.poll() == []


def test_poll_skips_malformed_stories():
    stories = [
        {"short_id": "bad", "title": "Bad timestamp", "created_at": "not-a-timestamp"},
        {"short_id": "missing-title", "created_at": "2024-09-01T09:00:00.000-05:00"},
        {"short_id": "missing-created", "title": "Missing timestamp"},
        {"title": "Missing id", "created_at": "2024-09-01T09:00:00.000-05:00"},
        {"short_id": "bad-type", "title": "Bad type", "created_at": None},
        None,
        "not a story",
        *STORIES,
    ]

    def fake_urlopen_with_bad_stories(req, timeout=None):
        return io.BytesIO(json.dumps(stories).encode())

    with patch.object(lobsters.urllib.request, "urlopen", fake_urlopen_with_bad_stories):
        envs = lobsters.poll()

    assert [e["event_id"] for e in envs] == ["lobsters:a1", "lobsters:b2"]


def test_poll_normalizes_nullable_fields():
    stories = [{**STORIES[0], "score": None, "tags": None}]

    def fake_urlopen_with_nulls(req, timeout=None):
        return io.BytesIO(json.dumps(stories).encode())

    with patch.object(lobsters.urllib.request, "urlopen", fake_urlopen_with_nulls):
        envs = lobsters.poll()

    assert envs[0]["payload"]["score"] is None
    assert envs[0]["payload"]["tags"] == []
    assert list(load_validator(ADAPTER.parent).iter_errors(envs[0]["payload"])) == []


def test_poll_defaults_optional_fields():
    stories = [{
        "short_id": "defaults",
        "title": "Defaults",
        "created_at": "2024-09-01T10:00:00.000-05:00",
    }]

    def fake_urlopen_with_defaults(req, timeout=None):
        return io.BytesIO(json.dumps(stories).encode())

    with patch.object(lobsters.urllib.request, "urlopen", fake_urlopen_with_defaults):
        envs = lobsters.poll()

    assert envs[0]["payload"]["score"] is None
    assert envs[0]["payload"]["tags"] == []
    assert list(load_validator(ADAPTER.parent).iter_errors(envs[0]["payload"])) == []
