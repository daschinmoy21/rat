"""Ops surface: broker up, cursor freshness, DLQ depth, sink sizes."""

import json
import os
import time
from datetime import UTC, datetime

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

STALE_MS = int(os.environ.get("RAT_STALE_MS", str(24 * 3600 * 1000)))


def interval_seconds(interval: str) -> int:
    n, unit = int(interval[:-1]), interval[-1]
    return n * {"s": 1, "m": 60, "h": 3600}[unit]


def age(ms: float) -> str:
    if ms < 1000:
        return "now"
    s, m, h, d = 1000, 60_000, 3_600_000, 86_400_000
    if ms >= d:
        return f"{ms // d:.0f}d"
    if ms >= h:
        return f"{ms // h:.0f}h"
    if ms >= m:
        return f"{ms // m:.0f}m"
    return f"{ms // s:.0f}s"


def isotime(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def cursor_rows(app, cursor, now_ms=None):
    """(name, cursor ts, interval s, state) per source, sorted by name."""
    now_ms = now_ms if now_ms is not None else time.time() * 1000
    rows = []
    for name in sorted(app.manifests):
        interval = interval_seconds(app.manifests[name]["interval"])
        last = cursor.get(name)
        if last is None:
            state = "never"
        elif now_ms - last > STALE_MS:
            state = "stale"
        else:
            state = "ok"
        rows.append((name, last, interval, state))
    return rows


def dlq_topics(app):
    return sorted(f"{m['topic']}.dlq" for m in app.manifests.values())


def dlq_depths(consumer, topics):
    """Messages sitting in each DLQ topic: end offset minus beginning."""
    depths = {}
    for t in topics:
        parts = consumer.partitions_for_topic(t)
        if not parts:
            depths[t] = 0
            continue
        tps = [TopicPartition(t, p) for p in parts]
        begins = consumer.beginning_offsets(tps)
        ends = consumer.end_offsets(tps)
        depths[t] = sum(ends[tp] - begins[tp] for tp in tps)
    return depths


def peek(consumer, topic, n=5):
    """The last n messages of a topic (roughly — one seek per partition)."""
    parts = consumer.partitions_for_topic(topic) or []
    tps = [TopicPartition(topic, p) for p in parts]
    if not tps:
        return []
    consumer.assign(tps)
    for tp, end in consumer.end_offsets(tps).items():
        consumer.seek(tp, max(0, end - n))
    rows = []
    deadline = time.time() + 5
    while time.time() < deadline:
        batch = consumer.poll(timeout_ms=1000, max_records=n)
        if not batch:
            break
        for tp, records in batch.items():
            for r in records:
                error = None
                for head, val in (r.headers or []):
                    if head == "rat.error":
                        error = (val.decode("utf-8", "replace")
                                 if isinstance(val, (bytes, bytearray))
                                 else str(val))
                try:
                    value = json.loads(
                        r.value.decode("utf-8", "replace")
                        if isinstance(r.value, (bytes, bytearray)) else str(r.value))
                except (ValueError, AttributeError):
                    value = {"raw": repr(r.value)[:500]}
                rows.append({
                    "topic": tp.topic,
                    "offset": r.offset,
                    "error": error,
                    "value": value,
                })
    rows.sort(key=lambda row: (row["offset"], row["topic"]))
    return rows[-n:]


def sink_sizes(sink_dir):
    """Bytes under correlated/ and events_raw/ — 0 when not written yet."""
    sizes = {}
    for name in ("correlated", "events_raw"):
        root = os.path.join(sink_dir, name)
        total = 0
        if os.path.isdir(root):
            for dirpath, _dirnames, filenames in os.walk(root):
                for f in filenames:
                    try:
                        total += os.path.getsize(os.path.join(dirpath, f))
                    except OSError:  # file vanished mid-walk; keep totaling
                        pass
        sizes[name] = total
    return sizes


def human(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n} B"


def broker_ok(bootstrap="localhost:9092"):
    try:
        c = KafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=4000)
        try:
            c.topics()
        finally:
            c.close()
        return True
    except Exception:
        return False
