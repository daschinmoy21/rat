import argparse
import logging
import os
import signal
import threading
import time
from pathlib import Path

from rat_producers.app import App
from rat_producers.loader import load_all
from rat_producers.producer import connect, dlq, emit
from rat_producers.schema import check

log = logging.getLogger("rat")


def _seconds(interval: str) -> int:
    n, unit = int(interval[:-1]), interval[-1]
    return n * {"s": 1, "m": 60, "h": 3600}[unit]


def run(app, cursor, producer, once=False, stop=None):
    while stop is None or not stop.is_set():
        for name, poll in sorted(app.sources.items()):
            try:
                since = cursor.get(name)
                validator = app.schemas.get(name)
                for env in poll(since):
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
        wait = min(_seconds(m["interval"]) for m in app.manifests.values())
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


def main(argv=None):
    parser = argparse.ArgumentParser(prog="rat")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("plugins")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)

    app = load_app()
    if args.cmd == "plugins":
        for name, m in sorted(app.manifests.items()):
            print(f"{name}  {m['topic']}  {m['interval']}")
    elif args.cmd == "run":
        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
        logging.getLogger("kafka").setLevel(logging.WARNING)
        from rat_producers.cursor import Cursor
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
