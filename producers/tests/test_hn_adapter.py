import importlib.util
import io
import json
from pathlib import Path
from unittest.mock import patch

ADAPTER = Path(__file__).resolve().parents[2] / "plugins" / "hn" / "adapter.py"
spec = importlib.util.spec_from_file_location("hn_adapter", ADAPTER)
hn = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hn)

story = {"id": 1, "type": "story", "time": 1725200000,
         "title": "$AAPL does a thing", "url": "https://example.com", "score": 42}
job = {"id": 2, "type": "job", "time": 1725200001, "title": "hire me"}


def fake_urlopen(req, timeout=None):
    if "topstories" in str(req):
        return io.BytesIO(json.dumps([1, 2]).encode())
    item = story if "1.json" in str(req) else job
    return io.BytesIO(json.dumps(item).encode())


def test_poll_maps_items_to_envelopes():
    with patch.object(hn.urllib.request, "urlopen", fake_urlopen):
        envs = hn.poll()
    assert len(envs) == 1  # the job item is skipped
    env = envs[0]
    assert env["event_id"] == "hn:1"
    assert env["source"] == "hn"
    assert env["entities"] == ["AAPL"]
    assert env["ts_ms"] == 1725200000000
    assert env["payload"] == {"title": "$AAPL does a thing",
                              "url": "https://example.com", "score": 42}
    assert set(env) == {"event_id", "source", "entities", "ts_ms", "payload"}


def test_poll_filters_since_ms():
    with patch.object(hn.urllib.request, "urlopen", fake_urlopen):
        assert hn.poll(since_ms=1725200000000) == []
        assert hn.poll(since_ms=1725100000000)  # older cursor: still new
