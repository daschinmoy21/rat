import argparse
import json
import logging
import os
import signal
import threading
import time
from pathlib import Path

from rat_producers.app import App
from rat_producers.cursor import Cursor
from rat_producers.extract import extract_entities
from rat_producers.loader import load_all
from rat_producers.producer import connect, dlq, emit
from rat_producers.schema import check
from rat_producers.status import (
    age,
    broker_ok,
    cursor_rows,
    dlq_depths,
    dlq_topics,
    human,
    interval_seconds,
    isotime,
    peek,
    sink_sizes,
)

log = logging.getLogger("rat")


def entity_text(manifest, envelope) -> str:
    """Text for extraction: payload values named by [entities].from_fields,
    in field order, missing keys skipped."""
    fields = (manifest.get("entities") or {}).get("from_fields") or []
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return ""
    return " ".join(str(payload[f]) for f in fields if f in payload)


def normalize_entities(entities) -> list[str]:
    """Drop blanks and duplicates, preserving order."""
    seen = set()
    out = []
    for entity in entities:
        if not isinstance(entity, str):
            continue
        entity = entity.strip()
        if not entity or entity in seen:
            continue
        seen.add(entity)
        out.append(entity)
    return out


def extract(manifest, envelope, extractor) -> list[str]:
    return normalize_entities(extractor(entity_text(manifest, envelope)))


def run(app, cursor, producer, once=False, stop=None):
    while stop is None or not stop.is_set():
        for name, poll in sorted(app.sources.items()):
            try:
                since = cursor.get(name)
                manifest = app.manifests.get(name, {})
                validator = app.schemas.get(name)
                extractor = app.extractors.get(name, extract_entities)
                for env in poll(since):
                    env["entities"] = extract(manifest, env, extractor)
                    problems = check(env, validator)
                    if problems:
                        dlq(env, producer, "schema: " + "; ".join(problems))
                        continue
                    emit(env, producer)
                    cursor.put(name, env["ts_ms"])
            except Exception as err:
                log.warning("source %s failed: %s", name, err)
        if once:
            return
        if not app.manifests:
            return
        wait = min(interval_seconds(m["interval"]) for m in app.manifests.values())
        if stop is None:
            time.sleep(wait)
        elif stop.wait(wait):
            return


def load_app():
    app = App()
    load_all(app, [
        "plugins",
        Path("/var/lib/rat/plugins"),
        Path.home() / ".config" / "rat" / "plugins",
    ])
    return app


def _consumer(bootstrap):
    from kafka import KafkaConsumer
    return KafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=4000)


def _bootstrap():
    return os.environ.get("RAT_BOOTSTRAP", "localhost:9092")


def print_status(app):
    bootstrap = _bootstrap()
    up = broker_ok(bootstrap)
    print(f"broker   {bootstrap}   {'up' if up else 'DOWN'}")

    db = os.environ.get("RAT_CURSOR_DB", "rat-cursors.db")
    rows = cursor_rows(app, Cursor(Path(db)))
    print("source   cursor                   age    interval  state")
    now_ms = time.time() * 1000
    for name, last, _interval, state in rows:
        ts = isotime(last) if last else "-"
        age_s = age(now_ms - last) if last else "-"
        interval = app.manifests[name]["interval"]
        print(f"{name:<8} {ts:<24} {age_s:<6} {interval:<9} {state}")

    if up:
        try:
            consumer = _consumer(bootstrap)
            try:
                depths = dlq_depths(consumer, dlq_topics(app))
            finally:
                consumer.close()
            print("dlq      " + "  ".join(f"{t}={d}" for t, d in depths.items()))
        except Exception as err:
            # broker dropped between the probe and the depth fetch
            print(f"dlq      (broker error: {err})")
    else:
        print("dlq      (broker down)")

    sizes = sink_sizes(os.environ.get("RAT_SINK_DIR", "/tmp/rat"))
    print("sinks    " + "  ".join(f"{k}={human(v)}" for k, v in sizes.items()))


def print_dlq(app, args):
    bootstrap = _bootstrap()
    # consumer construction is lazy: probe first, then guard every fetch
    if not broker_ok(bootstrap):
        print(f"broker {bootstrap}: DOWN")
        return
    try:
        consumer = _consumer(bootstrap)
    except Exception as err:
        print(f"broker {bootstrap}: DOWN ({err})")
        return
    topics = [args.topic] if args.topic else dlq_topics(app)
    try:
        depths = dlq_depths(consumer, topics)
        for t, d in depths.items():
            print(f"{t}  {d} messages")
        for t in topics:
            for row in peek(consumer, t, n=args.n):
                print(f"{row['topic']}  offset={row['offset']}  "
                      f"error={row['error'] or '-'}")
                print("  " + json.dumps(row["value"], sort_keys=True))
    except Exception as err:
        print(f"broker {bootstrap}: error fetching DLQ state ({err})")
    finally:
        consumer.close()


def main(argv=None):
    parser = argparse.ArgumentParser(prog="rat")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("plugins")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--once", action="store_true")
    sub.add_parser("status")
    dlq_parser = sub.add_parser("dlq")
    dlq_parser.add_argument("--topic", default=None,
                            help="one DLQ topic (default: every source's)")
    dlq_parser.add_argument("--n", type=int, default=5,
                            help="recent messages to print (default 5)")
    args = parser.parse_args(argv)

    app = load_app()
    if args.cmd == "plugins":
        for name, m in sorted(app.manifests.items()):
            print(f"{name}  {m['topic']}  {m['interval']}")
    elif args.cmd == "run":
        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
        logging.getLogger("kafka").setLevel(logging.WARNING)
        db = os.environ.get("RAT_CURSOR_DB", "rat-cursors.db")
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        producer = connect()
        try:
            run(app, Cursor(Path(db)), producer, once=args.once, stop=stop)
        finally:
            producer.flush(timeout=10)
            producer.close(timeout=10)
    elif args.cmd == "status":
        print_status(app)
    elif args.cmd == "dlq":
        print_dlq(app, args)
