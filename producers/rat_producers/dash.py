"""rat dash: a live local view of what the pipeline ingests (Kafka), processes
(Spark checkpoints) and stores (Parquet sinks).

One process, stdlib HTTP + Server-Sent Events, one self-contained page:
  * a Kafka reader thread tails every events.* topic (DLQs included) from the
    beginning into a bounded in-memory ring;
  * a store thread re-reads the Parquet sinks with DuckDB and the Spark
    checkpoint directories every few seconds;
  * each browser gets a snapshot on connect, then deltas once a second.
"""

import json
import logging
import os
import re
import threading
import time
from collections import Counter, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from urllib.parse import urlparse

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

from rat_producers.cursor import Cursor
from rat_producers.status import cursor_rows, sink_sizes

log = logging.getLogger("rat.dash")

TOPICS = re.compile(r"^events\..+")
STORE_LIMIT = 2000


def summary(payload) -> str:
    """One line a person can scan: a quote, a headline, or the raw payload."""
    if not isinstance(payload, dict):
        return "" if payload is None else str(payload)[:200]
    if "price" in payload and "symbol" in payload:
        return f"{payload['symbol']} {payload['price']} {payload.get('currency') or ''}".strip()
    title = payload.get("title")
    if title:
        sym = payload.get("symbol")
        return f"{sym} · {title}" if sym else str(title)
    return json.dumps(payload, sort_keys=True)[:200]


def _header(headers, name):
    for key, val in headers or []:
        if key == name:
            return (val.decode("utf-8", "replace")
                    if isinstance(val, (bytes, bytearray)) else str(val))
    return None


class Feed:
    """What Kafka has carried: a ring of recent envelopes, DLQ entries, an
    event_id index (so correlated pairs can show what they paired), totals
    per topic and a per-second arrival histogram."""

    def __init__(self, cap=5000):
        self.lock = threading.Lock()
        self.cap = cap
        self.events = deque()
        self.dlq = deque(maxlen=500)
        self.index = {}
        self.seq = 0
        self.counts = Counter()
        self.buckets = Counter()
        self.broker = {"up": False, "error": None, "topics": 0}

    def add(self, topic, partition, offset, value, headers=None, now=None):
        now = time.time() if now is None else now
        try:
            env = json.loads(value.decode("utf-8", "replace")
                             if isinstance(value, (bytes, bytearray)) else value)
            if not isinstance(env, dict):
                raise TypeError("not an object")
        except (ValueError, TypeError):
            env = {"payload": {"raw": repr(value)[:500]}}
        payload = env.get("payload")
        row = {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "event_id": env.get("event_id"),
            "source": env.get("source") or topic.split(".")[1],
            "entities": env.get("entities") or [],
            "ts_ms": env.get("ts_ms"),
            "summary": summary(payload),
            "payload": payload,
        }
        with self.lock:
            self.seq += 1
            row["seq"] = self.seq
            self.counts[topic] += 1
            self.buckets[int(now)] += 1
            if topic.endswith(".dlq"):
                row["error"] = _header(headers, "rat.error")
                self.dlq.append(row)
                return row
            if len(self.events) >= self.cap:
                old = self.events.popleft()
                if self.index.get(old["event_id"]) is old:
                    del self.index[old["event_id"]]
            self.events.append(row)
            if row["event_id"]:
                self.index[row["event_id"]] = row
            return row

    def since(self, seq):
        """Rows (events, dlq) newer than seq."""
        with self.lock:
            return ([r for r in self.events if r["seq"] > seq],
                    [r for r in self.dlq if r["seq"] > seq],
                    self.seq)

    def rate(self, now=None, n=60):
        """Messages per second for the last n whole seconds, oldest first."""
        now = int(time.time() if now is None else now)
        with self.lock:
            for sec in [s for s in self.buckets if s < now - n]:
                del self.buckets[sec]
            return [self.buckets.get(s, 0) for s in range(now - n, now)]


def _sync_consumer(consumer, assigned, topics):
    tps = {TopicPartition(t, p) for t in topics
           for p in consumer.partitions_for_topic(t) or ()}
    if tps == assigned:
        return assigned
    live = assigned & tps
    kept = {tp: consumer.position(tp) for tp in live}
    new = tps - assigned
    consumer.assign(list(tps))
    for tp, pos in kept.items():
        consumer.seek(tp, pos)
    if new:
        consumer.seek_to_beginning(*new)
    return tps


def consume(feed, bootstrap, stop, refresh_s=10):
    """Tail every events.* topic from the beginning, picking up new topics
    as they appear. Reconnects after broker errors."""
    while not stop.is_set():
        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap, group_id=None,
                enable_auto_commit=False, auto_offset_reset="earliest",
                request_timeout_ms=5000)
            assigned = set()
            refreshed = 0.0
            while not stop.is_set():
                if time.time() - refreshed >= refresh_s:
                    topics = sorted(t for t in consumer.topics() if TOPICS.match(t))
                    assigned = _sync_consumer(consumer, assigned, topics)
                    with feed.lock:
                        feed.broker = {"up": True, "error": None, "topics": len(topics)}
                    refreshed = time.time()
                if not assigned:
                    stop.wait(1)
                    continue
                for tp, records in consumer.poll(timeout_ms=500).items():
                    for r in records:
                        # bucket by produce time so a replay from the
                        # beginning does not look like a burst of traffic
                        feed.add(tp.topic, tp.partition, r.offset, r.value, r.headers,
                                 now=r.timestamp / 1000 if r.timestamp else None)
        except Exception as err:
            log.warning("kafka: %s", err)
            with feed.lock:
                feed.broker = {"up": False, "error": str(err), "topics": 0}
            stop.wait(3)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception as err:
                    log.debug("kafka close: %s", err)


def _latest(d):
    """(batch id, path) of the highest-numbered file in a checkpoint log."""
    if not d.is_dir():
        return None
    ids = [(int(f.name), f) for f in d.iterdir() if f.name.isdigit()]
    return max(ids) if ids else None


def spark_progress(checkpoint_dir, now_ms=None):
    """Per streaming query: last planned and committed micro-batch, and how
    long ago the commit landed. A planned batch with no commit is running."""
    now_ms = time.time() * 1000 if now_ms is None else now_ms
    out = []
    root = Path(checkpoint_dir)
    if not root.is_dir():
        return out
    for query in sorted(p for p in root.iterdir() if p.is_dir()):
        planned, committed = _latest(query / "offsets"), _latest(query / "commits")
        out.append({
            "name": query.name,
            "planned": planned[0] if planned else None,
            "committed": committed[0] if committed else None,
            "committed_ago_ms": (now_ms - committed[1].stat().st_mtime * 1000
                                 if committed else None),
        })
    return out


class Store:
    """What Spark has written: newest Parquet rows and totals per sink."""

    def __init__(self, sink_dir, checkpoint_dir):
        self.sink_dir = sink_dir
        self.checkpoint_dir = checkpoint_dir
        self.lock = threading.Lock()
        self.version = 0
        self.data = {"correlated": [], "events_raw": [],
                     "totals": {"correlated": 0, "events_raw": 0}}
        self.error = None
        self.spark = []
        self.sizes = {}
        self._sig = None

    def _query(self, con, sql, table):
        glob = os.path.join(self.sink_dir, table, "**", "*.parquet")
        src = f"read_parquet('{glob}', hive_partitioning=true, union_by_name=true)"
        try:
            total = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
        except Exception as err:
            if "No files found" in str(err):
                return 0, []
            raise
        cur = con.execute(sql.format(src=src, limit=STORE_LIMIT))
        cols = [d[0] for d in cur.description]
        return total, [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def refresh(self):
        self.spark = spark_progress(self.checkpoint_dir)
        self.sizes = sink_sizes(self.sink_dir)
        try:
            import duckdb
        except ImportError as err:
            self.error = f"duckdb unavailable: {err}"
            return
        try:
            con = duckdb.connect()
            try:
                n_corr, corr = self._query(con, (
                    "SELECT entity, a_id, b_id, ts_ms, CAST(dt AS VARCHAR) AS dt "
                    "FROM {src} ORDER BY ts_ms DESC LIMIT {limit}"), "correlated")
                n_raw, raw = self._query(con, (
                    "SELECT event_id, source, ts_ms, payload, CAST(dt AS VARCHAR) AS dt "
                    "FROM {src} ORDER BY ts_ms DESC LIMIT {limit}"), "events_raw")
            finally:
                con.close()
        except Exception as err:
            self.error = f"parquet: {err}"
            return
        for row in raw:
            try:
                row["payload"] = json.loads(row["payload"])
            except (TypeError, ValueError):
                pass
            row["summary"] = summary(row["payload"])
        self.error = None
        sig = (n_corr, n_raw, corr[0]["ts_ms"] if corr else None,
               raw[0]["ts_ms"] if raw else None)
        with self.lock:
            if sig != self._sig:
                self._sig = sig
                self.version += 1
                self.data = {"correlated": corr, "events_raw": raw,
                             "totals": {"correlated": n_corr, "events_raw": n_raw}}

    def loop(self, stop, every_s=5):
        while not stop.is_set():
            try:
                self.refresh()
            except Exception as err:
                self.error = str(err)
            stop.wait(every_s)


class Dash:
    def __init__(self, app, feed, store, bootstrap, cursor_db):
        self.app = app
        self.feed = feed
        self.store = store
        self.bootstrap = bootstrap
        self.cursor_db = cursor_db
        self.started = time.time()

    def stats(self):
        now_ms = time.time() * 1000
        try:
            cursors = {name: (last, state) for name, last, _i, state
                       in cursor_rows(self.app, Cursor(Path(self.cursor_db)), now_ms)}
        except Exception:
            cursors = {}
        with self.feed.lock:
            counts = dict(self.feed.counts)
            broker = dict(self.feed.broker)
        sources = []
        for name, m in sorted(self.app.manifests.items()):
            last, state = cursors.get(name, (None, "never"))
            sources.append({
                "name": name, "topic": m["topic"], "interval": m["interval"],
                "count": counts.get(m["topic"], 0),
                "dlq": counts.get(m["topic"] + ".dlq", 0),
                "cursor": last, "state": state,
            })
        with self.store.lock:
            totals = dict(self.store.data["totals"])
        return {
            "now": now_ms,
            "uptime_s": time.time() - self.started,
            "broker": {"bootstrap": self.bootstrap, **broker},
            "sources": sources,
            "topics": counts,
            "rate": self.feed.rate(),
            "spark": self.store.spark,
            "sinks": {k: {"rows": totals.get(k, 0), "bytes": v}
                      for k, v in self.store.sizes.items()},
            "store_error": self.store.error,
            "sink_dir": self.store.sink_dir,
        }

    def stored(self):
        with self.store.lock:
            return self.store.version, self.store.data

    def snapshot(self):
        events, dlq, seq = self.feed.since(0)
        version, data = self.stored()
        return {"stats": self.stats(), "events": events, "dlq": dlq, "seq": seq,
                "stored": data, "version": version, "reset": True}


def _handler(dash, stop, tick_s=1.0):
    page = resources.files("rat_producers").joinpath("dash.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            log.debug(fmt, *args)

        def _send(self, body, ctype):
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/":
                self._send(page, "text/html; charset=utf-8")
            elif path == "/api/snapshot":
                self._send(json.dumps(dash.snapshot()).encode(), "application/json")
            elif path == "/api/stream":
                self._stream()
            else:
                self.send_error(404)

        def _stream(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            msg = dash.snapshot()
            seq, version = msg["seq"], msg["version"]
            try:
                while True:
                    self.wfile.write(b"data: " + json.dumps(msg).encode() + b"\n\n")
                    self.wfile.flush()
                    if stop.wait(tick_s):
                        return
                    events, dlq, seq_now = dash.feed.since(seq)
                    msg = {"stats": dash.stats(), "events": events, "dlq": dlq}
                    seq = seq_now
                    v, data = dash.stored()
                    if v != version:
                        version = v
                        msg["stored"] = data
            except OSError:
                return

    return Handler


def serve(app, host="127.0.0.1", port=8765, cap=5000, bootstrap="localhost:9092",
          sink_dir="/tmp/rat", checkpoint_dir=None, cursor_db="rat-cursors.db"):
    stop = threading.Event()
    feed = Feed(cap=cap)
    store = Store(sink_dir, checkpoint_dir or os.path.join(sink_dir, "checkpoints"))
    dash = Dash(app, feed, store, bootstrap, cursor_db)
    threading.Thread(target=consume, args=(feed, bootstrap, stop), daemon=True).start()
    threading.Thread(target=store.loop, args=(stop,), daemon=True).start()
    server = ThreadingHTTPServer((host, port), _handler(dash, stop))
    server.daemon_threads = True
    print(f"rat dash  http://{host}:{server.server_address[1]}/"
          f"   kafka={bootstrap}  sinks={sink_dir}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
