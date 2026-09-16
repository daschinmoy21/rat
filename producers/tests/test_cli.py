import threading
import time

from rat_producers.app import App
from rat_producers.cli import run
from rat_producers.cursor import Cursor
from rat_producers.loader import load_all


class FakeFuture:
    def get(self, timeout=None):
        return None


class FakeProducer:
    def __init__(self):
        self.sent: list[tuple[str, bytes, bytes, list | None]] = []

    def send(self, topic, key, value, headers=None):
        self.sent.append((topic, key, value, headers))
        return FakeFuture()


def envelope(event_id, ts_ms, **overrides):
    env = {
        "event_id": event_id,
        "source": "hn",
        "entities": ["AAPL"],
        "ts_ms": ts_ms,
        "payload": {"title": "t"},
    }
    env.update(overrides)
    return env


def make_app(poll):
    app = App()
    app.add_source("hn", poll=poll)
    app.schemas["hn"] = None
    app.manifests["hn"] = {"interval": "60s"}
    return app


def test_valid_envelopes_emit_and_advance_cursor(tmp_path):
    rows = [envelope("hn:1", 1000), envelope("hn:2", 2000)]
    app = make_app(lambda since: list(rows))
    p = FakeProducer()
    run(app, Cursor(tmp_path / "c.db"), p, once=True)
    assert [t for t, *_ in p.sent] == ["events.hn", "events.hn"]
    assert Cursor(tmp_path / "c.db").get("hn") == 2000


def test_invalid_envelope_goes_to_dlq_without_cursor_move(tmp_path):
    rows = [envelope("hn:bad", "soon"), envelope("hn:1", 1000)]
    app = make_app(lambda since: list(rows))
    p = FakeProducer()
    run(app, Cursor(tmp_path / "c.db"), p, once=True)
    assert [t for t, *_ in p.sent] == ["events.hn.dlq", "events.hn"]
    headers = dict(p.sent[0][3])
    reason = headers["rat.error"].decode()
    assert reason.startswith("schema:")
    assert "ts_ms" in reason
    assert Cursor(tmp_path / "c.db").get("hn") == 1000


def test_set_stop_event_skips_the_sources(tmp_path):
    app = make_app(lambda since: [])
    p = FakeProducer()
    stop = threading.Event()
    stop.set()
    run(app, Cursor(tmp_path / "c.db"), p, stop=stop)
    assert p.sent == []


def test_run_is_interruptible_during_the_wait(tmp_path):
    app = make_app(lambda since: [])
    app.manifests["hn"] = {"interval": "3600s"}  # the wait alone would take an hour
    p = FakeProducer()
    stop = threading.Event()
    threading.Timer(0.05, stop.set).start()
    started = time.monotonic()
    run(app, Cursor(tmp_path / "c.db"), p, stop=stop)
    assert time.monotonic() - started < 60


def test_loader_attaches_payload_validator(tmp_path):
    d = tmp_path / "hn"
    d.mkdir()
    (d / "plugin.toml").write_text(
        'name = "hn"\ntopic = "events.hn"\ninterval = "60s"\n')
    (d / "schema.json").write_text('{"type": "object"}')
    app = App()
    load_all(app, [tmp_path])
    assert app.schemas["hn"] is not None
    assert app.schemas["hn"].is_valid({"title": "x"})
