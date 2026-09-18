from rat_producers.app import App
from rat_producers.cli import run
from rat_producers.cursor import Cursor


class FakeFuture:
    def get(self, timeout=None):
        return None


class FakeProducer:
    def __init__(self):
        self.sent: list[tuple[str, bytes, bytes, list | None]] = []

    def send(self, topic, key, value, headers=None):
        self.sent.append((topic, key, value, headers))
        return FakeFuture()


def envelope(ts_ms, payload, **overrides):
    env = {
        "event_id": "hn:1",
        "source": "hn",
        "entities": [],
        "ts_ms": ts_ms,
        "payload": payload,
    }
    env.update(overrides)
    return env


def make_app(rows, from_fields=("title", "url"), extractor=None):
    app = App()
    app.add_source("hn", poll=lambda since: list(rows))
    app.schemas["hn"] = None
    manifest = {"interval": "60s"}
    if from_fields is not None:
        manifest["entities"] = {"from_fields": list(from_fields)}
    app.manifests["hn"] = manifest
    if extractor is not None:
        app.add_extractor("hn", extractor)
    return app


def test_default_extraction_fills_entities_from_payload_fields(tmp_path):
    rows = [envelope(1000, {"title": "$AAPL and $MSFT", "url": "https://x/$AAPL"})]
    app = make_app(rows)
    run(app, Cursor(tmp_path / "c.db"), FakeProducer(), once=True)
    assert rows[0]["entities"] == ["AAPL", "MSFT"]


def test_registered_override_wins_and_overwrites_prefilled_entities(tmp_path):
    rows = [envelope(1000, {"title": "$AAPL"}, entities=["PREFILLED"])]
    app = make_app(rows, extractor=lambda text: ["OVERRIDE"])
    run(app, Cursor(tmp_path / "c.db"), FakeProducer(), once=True)
    assert rows[0]["entities"] == ["OVERRIDE"]


def test_missing_from_fields_yields_empty_entities(tmp_path):
    rows = [envelope(1000, {"title": "$AAPL", "url": "https://x/$MSFT"})]
    app = make_app(rows, from_fields=None)
    run(app, Cursor(tmp_path / "c.db"), FakeProducer(), once=True)
    assert rows[0]["entities"] == []


def test_partial_from_fields_skip_missing_keys(tmp_path):
    rows = [envelope(1000, {"title": "$AAPL", "url": "https://x/$MSFT"})]
    app = make_app(rows, from_fields=("title", "missing"))
    run(app, Cursor(tmp_path / "c.db"), FakeProducer(), once=True)
    assert rows[0]["entities"] == ["AAPL"]


def test_blank_and_duplicate_entities_are_normalized(tmp_path):
    rows = [envelope(1000, {"title": "x"})]
    app = make_app(
        rows, extractor=lambda text: [" AAPL ", "", "AAPL", "MSFT", None])
    run(app, Cursor(tmp_path / "c.db"), FakeProducer(), once=True)
    assert rows[0]["entities"] == ["AAPL", "MSFT"]


def test_extractor_failure_emits_nothing_and_does_not_advance_cursor(tmp_path):
    def boom(text):
        raise ValueError("bad extractor")

    rows = [envelope(1000, {"title": "$AAPL"})]
    app = make_app(rows, extractor=boom)
    p = FakeProducer()
    db = tmp_path / "c.db"
    run(app, Cursor(db), p, once=True)
    assert p.sent == []
    assert Cursor(db).get("hn") is None
