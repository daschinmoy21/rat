import json

from rat_producers import cli
from rat_producers.app import App
from rat_producers.cursor import Cursor
from rat_producers.status import (
    age,
    cursor_rows,
    dlq_depths,
    dlq_topics,
    human,
    interval_seconds,
    peek,
    sink_sizes,
)


def make_app():
    app = App()
    app.manifests["hn"] = {"topic": "events.hn", "interval": "60s"}
    app.manifests["rss"] = {"topic": "events.rss", "interval": "5m"}
    return app


def test_interval_seconds():
    assert interval_seconds("60s") == 60
    assert interval_seconds("5m") == 300
    assert interval_seconds("1h") == 3600


def test_age_formatting():
    assert age(500) == "now"
    assert age(5000) == "5s"
    assert age(300_000) == "5m"
    assert age(7_200_000) == "2h"
    assert age(3 * 86_400_000) == "3d"


def test_cursor_rows_never_ok_stale(tmp_path):
    app = make_app()
    cursor = Cursor(tmp_path / "c.db")
    now = 1_000_000_000_000
    cursor.put("hn", now - 3_600_000)  # 1h ago: fine
    rows = {r[0]: r for r in cursor_rows(app, cursor, now_ms=now)}
    assert rows["hn"][3] == "ok"
    assert rows["rss"][3] == "never"
    cursor.put("rss", now - 100 * 86_400_000)  # 100d ago: stale
    rows = {r[0]: r for r in cursor_rows(app, cursor, now_ms=now)}
    assert rows["rss"][3] == "stale"
    assert [r[0] for r in cursor_rows(app, cursor, now_ms=now)] == ["hn", "rss"]


def test_dlq_topics_come_from_manifests():
    assert dlq_topics(make_app()) == ["events.hn.dlq", "events.rss.dlq"]


class FakeConsumer:
    def __init__(self, parts, data):
        self._parts, self._data = parts, data
        self._assigned, self._seeks = [], {}

    def partitions_for_topic(self, t):
        return self._parts

    def assign(self, tps):
        self._assigned = tps

    def beginning_offsets(self, tps):
        return {tp: 0 for tp in tps}

    def end_offsets(self, tps):
        return {tp: 5 for tp in tps}

    def seek(self, tp, off):
        self._seeks[tp] = off

    def poll(self, timeout_ms=None, max_records=None):
        if not self._seeks:
            return {}
        self._seeks = {}
        return {
            tp: self._data.get((tp.topic, tp.partition), [])
            for tp in self._assigned
        }


def test_dlq_depths_sums_across_partitions():
    consumer = FakeConsumer([0, 1], {})
    depths = dlq_depths(consumer, ["events.hn.dlq"])
    assert depths == {"events.hn.dlq": 10}


def test_dlq_depths_missing_topic_is_zero():
    consumer = FakeConsumer(None, {})
    assert dlq_depths(consumer, ["events.ghost.dlq"]) == {"events.ghost.dlq": 0}


class FakeRecord:
    def __init__(self, offset, headers, value):
        self.offset, self.headers, self.value = offset, headers, value


def test_peek_returns_last_messages_with_error_header():
    value = json.dumps({"event_id": "hn:1"}).encode()
    rec = FakeRecord(4, [("rat.error", b"schema: ts_ms not an integer")], value)
    consumer = FakeConsumer([0], {("events.hn.dlq", 0): [rec]})
    rows = peek(consumer, "events.hn.dlq", n=5)
    assert len(rows) == 1
    assert rows[0]["topic"] == "events.hn.dlq"
    assert rows[0]["offset"] == 4
    assert rows[0]["error"] == "schema: ts_ms not an integer"
    assert rows[0]["value"] == {"event_id": "hn:1"}


def test_peek_of_empty_topic_is_empty():
    consumer = FakeConsumer([0, 1], {})
    assert peek(consumer, "events.hn.dlq", n=5) == []


def test_peek_survives_malformed_dlq_records():
    bad = FakeRecord(7, [("rat.error", None)], b"not json {{{")
    consumer = FakeConsumer([0], {("events.hn.dlq", 0): [bad]})
    rows = peek(consumer, "events.hn.dlq", n=5)
    assert rows[0]["error"] == "None"  # non-bytes header value degrades to str
    assert "raw" in rows[0]["value"]  # non-JSON payload kept as raw repr


def test_sink_sizes_counts_bytes_and_tolerates_missing(tmp_path):
    d = tmp_path / "correlated"
    d.mkdir()
    (d / "part-0.parquet").write_bytes(b"x" * 10)
    sizes = sink_sizes(tmp_path)
    assert sizes == {"correlated": 10, "events_raw": 0}


def test_human_sizes():
    assert human(0) == "0 B"
    assert human(1024) == "1.0 KiB"
    assert human(1536) == "1.5 KiB"
    assert human(1024 ** 3) == "1.0 GiB"


def test_print_status_broker_down_still_reports(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("RAT_CURSOR_DB", str(tmp_path / "c.db"))
    monkeypatch.setenv("RAT_SINK_DIR", str(tmp_path / "sinks"))
    monkeypatch.setattr(cli, "broker_ok", lambda b: False)
    cli.print_status(make_app())
    out = capsys.readouterr().out
    assert "DOWN" in out
    assert "(broker down)" in out
    assert "never" in out
    assert "correlated=0 B" in out
