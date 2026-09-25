import json
import os
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from importlib import resources

import pytest
from kafka.structs import TopicPartition

from rat_producers.app import App
from rat_producers.dash import (
    Dash,
    Feed,
    Store,
    _handler,
    _sync_consumer,
    spark_progress,
    summary,
)


def env(event_id, source="hn", entities=(), ts_ms=1000, **payload):
    return json.dumps({"event_id": event_id, "source": source, "entities": list(entities),
                       "ts_ms": ts_ms, "payload": payload}).encode()


class FakeConsumer:
    def __init__(self, positions):
        self.positions = positions
        self.assigned = None
        self.seeks = {}
        self.beginnings = []

    def partitions_for_topic(self, topic):
        return [0]

    def position(self, tp):
        return self.positions[tp]

    def assign(self, tps):
        self.assigned = set(tps)

    def seek(self, tp, offset):
        self.seeks[tp] = offset

    def seek_to_beginning(self, *tps):
        self.beginnings.extend(tps)


def test_sync_consumer_replaces_deleted_topics_and_preserves_positions():
    hn = TopicPartition("events.hn", 0)
    removed = TopicPartition("events.removed", 0)
    added = TopicPartition("events.news", 0)
    consumer = FakeConsumer({hn: 42})

    assigned = _sync_consumer(
        consumer, {hn, removed}, ["events.hn", "events.news"])

    assert assigned == {hn, added}
    assert consumer.assigned == {hn, added}
    assert consumer.seeks == {hn: 42}
    assert set(consumer.beginnings) == {added}


def test_dashboard_bounds_dlq_buffers():
    page = resources.files("rat_producers").joinpath("dash.html").read_text()
    assert "const DLQ_CAP = 500;" in page
    assert "if (S.dlq.length > DLQ_CAP)" in page
    assert "S.dlq.splice(0, S.dlq.length - DLQ_CAP)" in page
    assert "if (S.pendingDlq.length > DLQ_CAP)" in page
    assert "S.pendingDlq.splice(0, S.pendingDlq.length - DLQ_CAP)" in page


def test_sse_treats_oserror_as_disconnect():
    class DisconnectingDash:
        @staticmethod
        def snapshot():
            return {"seq": 0, "version": 0}

    class DisconnectingWriter:
        @staticmethod
        def write(data):
            raise ConnectionAbortedError("browser closed")

    Handler = _handler(DisconnectingDash(), threading.Event())
    handler = object.__new__(Handler)
    handler.send_response = lambda *args: None
    handler.send_header = lambda *args: None
    handler.end_headers = lambda *args: None
    handler.wfile = DisconnectingWriter()
    handler._stream()


def test_summary_prefers_quote_then_title():
    assert summary({"symbol": "TSLA", "price": 250.1, "currency": "USD"}) == "TSLA 250.1 USD"
    assert summary({"symbol": "TSLA", "title": "Recall"}) == "TSLA · Recall"
    assert summary({"title": "Show HN"}) == "Show HN"
    assert summary({"a": 1}) == '{"a": 1}'
    assert summary(None) == ""


def test_feed_indexes_events_and_counts_topics():
    feed = Feed()
    feed.add("events.hn", 0, 0, env("hn:1", title="x"))
    feed.add("events.hn", 0, 1, env("hn:2", title="y"))
    assert feed.counts["events.hn"] == 2
    assert feed.index["hn:2"]["summary"] == "y"
    events, dlq, seq = feed.since(1)
    assert [e["event_id"] for e in events] == ["hn:2"] and dlq == [] and seq == 2


def test_feed_ring_evicts_oldest_and_its_index_entry():
    feed = Feed(cap=2)
    for i in range(3):
        feed.add("events.hn", 0, i, env(f"hn:{i}"))
    assert [e["event_id"] for e in feed.events] == ["hn:1", "hn:2"]
    assert "hn:0" not in feed.index


def test_feed_routes_dlq_with_error_header():
    feed = Feed()
    feed.add("events.hn.dlq", 0, 0, env("hn:bad"), [("rat.error", b"schema: title missing")])
    assert not feed.events
    assert feed.dlq[0]["error"] == "schema: title missing"
    assert feed.counts["events.hn.dlq"] == 1


def test_feed_survives_non_json():
    feed = Feed()
    row = feed.add("events.rss", 0, 0, b"\xffnot json")
    assert row["source"] == "rss" and "raw" in row["payload"]


def test_rate_buckets_last_n_seconds():
    feed = Feed()
    feed.add("events.hn", 0, 0, env("a"), now=100.2)
    feed.add("events.hn", 0, 1, env("b"), now=100.9)
    feed.add("events.hn", 0, 2, env("c"), now=102.0)
    assert feed.rate(now=103, n=3) == [2, 0, 1]
    assert feed.rate(now=200, n=3) == [0, 0, 0]


def test_spark_progress_reads_checkpoint_logs(tmp_path):
    q = tmp_path / "raw"
    (q / "offsets").mkdir(parents=True)
    (q / "commits").mkdir()
    for i in range(3):
        (q / "offsets" / str(i)).write_text("")
    for i in range(2):
        (q / "commits" / str(i)).write_text("")
    (q / "offsets" / ".2.crc").write_text("")
    [p] = spark_progress(tmp_path)
    assert (p["name"], p["planned"], p["committed"]) == ("raw", 2, 1)
    assert p["committed_ago_ms"] >= 0
    assert spark_progress(tmp_path / "missing") == []


def test_store_without_sinks_is_empty(tmp_path):
    pytest.importorskip("duckdb")
    store = Store(str(tmp_path), str(tmp_path / "checkpoints"))
    store.refresh()
    assert store.error is None
    assert store.data["totals"] == {"correlated": 0, "events_raw": 0}


def test_store_reads_partitioned_parquet(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    corr = tmp_path / "correlated" / "dt=2026-09-25"
    raw = tmp_path / "events_raw" / "dt=2026-09-25" / "source=hn"
    corr.mkdir(parents=True)
    raw.mkdir(parents=True)
    con = duckdb.connect()
    con.execute(f"COPY (SELECT 'TSLA' AS entity, 'news:TSLA:1' AS a_id, "
                f"'stocks:TSLA:2' AS b_id, 2000::BIGINT AS ts_ms) "
                f"TO '{corr / 'part-0.parquet'}' (FORMAT parquet)")
    con.execute(f"COPY (SELECT 'hn:1' AS event_id, [] ::VARCHAR[] AS entities, "
                f"1000::BIGINT AS ts_ms, '{{\"title\": \"hi\"}}' AS payload) "
                f"TO '{raw / 'part-0.parquet'}' (FORMAT parquet)")
    con.close()
    store = Store(str(tmp_path), str(tmp_path / "checkpoints"))
    store.refresh()
    assert store.error is None
    assert store.data["totals"] == {"correlated": 1, "events_raw": 1}
    assert store.data["correlated"][0]["dt"] == "2026-09-25"
    [row] = store.data["events_raw"]
    assert (row["source"], row["summary"]) == ("hn", "hi")
    version = store.version
    store.refresh()
    assert store.version == version  # unchanged sinks do not re-send


def test_http_serves_page_and_snapshot(tmp_path, monkeypatch):
    app = App()
    app.manifests["hn"] = {"topic": "events.hn", "interval": "5m"}
    feed = Feed()
    feed.add("events.hn", 0, 0, env("hn:1", title="x"))
    store = Store(str(tmp_path), str(tmp_path / "cp"))
    dash = Dash(app, feed, store, "localhost:9092", os.path.join(tmp_path, "c.db"))
    stop = threading.Event()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(dash, stop))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        page = urllib.request.urlopen(base + "/").read().decode()
        assert "<title>rat dash</title>" in page
        snap = json.load(urllib.request.urlopen(base + "/api/snapshot"))
        assert snap["events"][0]["event_id"] == "hn:1"
        [src] = snap["stats"]["sources"]
        assert (src["name"], src["count"], src["state"]) == ("hn", 1, "never")
    finally:
        stop.set()
        server.shutdown()
        server.server_close()
