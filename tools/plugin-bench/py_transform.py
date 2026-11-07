"""In-process cost of the real Python plugins, with the network replaced.

`urllib.request.urlopen` is swapped for an in-memory lookup into
fixtures.responses() before the plugins load, so the unmodified adapters run
their real parse + map code and the real core path (cli.extract +
schema.check) runs on what they return. No socket is opened and no Kafka call
is made. This is a MICROBENCHMARK of CPU work only; it says nothing about
network waits.

  py_transform.py --once      one cycle of all plugins, then exit (for spawn.py)
  py_transform.py OUT_JSON    timed loops, tracemalloc peaks, written as JSON

Also measures "map-only" ports (bench code, not the adapters) that start from
already-decoded dicts, to line up with transform.lua, which starts from
already-decoded Lua tables.
"""

import email.utils
import io
import json
import os
import resource
import statistics
import sys
import time
import tracemalloc
import urllib.request
from datetime import datetime
from pathlib import Path

if os.environ.get("RAT_BENCH_HOME"):
    Path.home = classmethod(lambda cls: Path(os.environ["RAT_BENCH_HOME"]))

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fixtures

RESP = fixtures.responses()


class _Resp(io.BytesIO):
    pass


def fake_urlopen(req, timeout=None):
    url = req if isinstance(req, str) else req.full_url
    return _Resp(RESP[fixtures.path_of(url)])


urllib.request.urlopen = fake_urlopen

from rat_producers.cli import extract, load_app
from rat_producers.extract import extract_entities
from rat_producers.schema import check

PLUGINS = Path(os.environ.get("RAT_PLUGINS_DIR",
                              Path(__file__).resolve().parents[2] / "plugins"))


def app_for_bench():
    return load_app([PLUGINS])


def cycle(app, stage="full"):
    """One poll of every source, as cli.run does minus cursor/emit."""
    n = 0
    for name, poll in sorted(app.sources.items()):
        manifest = app.manifests[name]
        validator = app.schemas[name]
        extractor = app.extractors.get(name, extract_entities)
        for env in poll(None):
            if stage != "poll":
                env["entities"] = extract(manifest, env, extractor)
                if stage == "full":
                    problems = check(env, validator)
                    assert not problems, (name, problems)
            n += 1
    return n


def per_source(app):
    out = {}
    for name, poll in sorted(app.sources.items()):
        out[name] = len(poll(None))
    return out


# ---- map-only ports: same mapping, input already decoded ------------------

def _rfc2822_ms(s):
    return int(email.utils.parsedate_to_datetime(s).timestamp() * 1000)


def map_rss(items):
    return [{"event_id": f"rss:{i['guid'] or i['link']}", "source": "rss", "entities": [],
             "ts_ms": _rfc2822_ms(i["pubDate"]),
             "payload": {"title": i["title"], "link": i["link"]}} for i in items]


def map_hn(items):
    return [{"event_id": f"hn:{i['id']}", "source": "hn", "entities": [],
             "ts_ms": i["time"] * 1000,
             "payload": {"title": i.get("title"), "url": i.get("url"),
                         "score": i.get("score")}}
            for i in items if i and i.get("type") == "story"]


def map_stocks(charts):
    out = []
    for c in charts:
        meta = c["chart"]["result"][0]["meta"]
        ts = int(meta["regularMarketTime"] * 1000)
        out.append({"event_id": f"stocks:{meta['symbol']}:{ts}", "source": "stocks",
                    "entities": [], "ts_ms": ts,
                    "payload": {"symbol": meta["symbol"],
                                "price": float(meta["regularMarketPrice"]),
                                "currency": meta.get("currency", "USD")}})
    return out


def map_news(by_sym):
    out = []
    for sym, items in by_sym.items():
        for i in items:
            out.append({"event_id": f"news:{sym}:{i['guid'] or i['link']}", "source": "news",
                        "entities": [], "ts_ms": _rfc2822_ms(i["pubDate"]),
                        "payload": {"symbol": sym, "title": i["title"], "link": i["link"]}})
    out.sort(key=lambda e: e["ts_ms"])
    return out


def map_lobsters(stories):
    out = [{"event_id": f"lobsters:{s['short_id']}", "source": "lobsters", "entities": [],
            "ts_ms": int(datetime.fromisoformat(s["created_at"]).timestamp() * 1000),
            "payload": {"title": s["title"], "url": s.get("url") or None,
                        "score": s.get("score"), "tags": s.get("tags") or []}}
           for s in stories]
    out.sort(key=lambda e: e["ts_ms"])
    return out


def decoded():
    hn = [json.loads(RESP[f"/v0/item/{i}.json"])
          for i in json.loads(RESP["/v0/topstories.json"])]
    return {"rss": fixtures._rss_items(fixtures.RSS_ITEMS, "rss"), "hn": hn,
            "stocks": [fixtures.chart(s, i) for i, s in enumerate(fixtures.SYMBOLS)],
            "news": {s: fixtures._rss_items(fixtures.NEWS_ITEMS, f"news/{s}")
                     for s in fixtures.SYMBOLS},
            "lobsters": fixtures.lobsters()}


def map_cycle(app, data):
    n = 0
    for name, fn in (("hn", map_hn), ("lobsters", map_lobsters), ("news", map_news),
                     ("rss", map_rss), ("stocks", map_stocks)):
        manifest, extractor = app.manifests[name], app.extractors[name]
        for env in fn(data[name]):
            env["entities"] = extract(manifest, env, extractor)
            n += 1
    return n


# ---- timing ---------------------------------------------------------------

def timeit(fn, reps):
    for _ in range(max(3, reps // 10)):
        fn()
    xs = []
    for _ in range(reps):
        t0 = time.perf_counter_ns()
        fn()
        xs.append((time.perf_counter_ns() - t0) / 1e3)
    xs.sort()
    return {"median_us": statistics.median(xs), "min_us": xs[0],
            "p90_us": xs[int(0.9 * (len(xs) - 1))], "reps": reps}


def heap_peak(fn):
    tracemalloc.start()
    fn()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak


def main():
    if sys.argv[1:] == ["--once"]:
        cycle(app_for_bench())
        sys.stdout.write(Path("/proc/self/status").read_text())  # VmHWM for spawn.py
        return
    out = Path(sys.argv[1])
    reps = int(os.environ.get("REPS", "300"))
    t0 = time.perf_counter()
    app = app_for_bench()
    load_ms = (time.perf_counter() - t0) * 1e3
    data = decoded()
    res = {
        "python": sys.version,
        "events_per_cycle": cycle(app),
        "events_per_source": per_source(app),
        "load_app_ms_in_process": load_ms,
        "maxrss_after_load_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "cycle_poll_only": timeit(lambda: cycle(app, "poll"), reps),
        "cycle_poll_extract": timeit(lambda: cycle(app, "extract"), reps),
        "cycle_full_poll_extract_check": timeit(lambda: cycle(app, "full"), reps),
        "map_only_extract_from_decoded": timeit(lambda: map_cycle(app, data), reps),
        "heap_peak_bytes_full_cycle": heap_peak(lambda: cycle(app, "full")),
        "heap_peak_bytes_map_only": heap_peak(lambda: map_cycle(app, data)),
    }
    for name in sorted(app.sources):
        res[f"source_{name}_full"] = timeit(
            lambda n=name: _one(app, n), max(50, reps // 3))
    res["maxrss_end_kib"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    out.write_text(json.dumps(res, indent=1))
    for k, v in res.items():
        if isinstance(v, dict) and "median_us" in v:
            print(f"{k:36s} {v['median_us']:10.1f} us (median of {v['reps']})")
        else:
            print(f"{k:36s} {v}")


def _one(app, name):
    m, val = app.manifests[name], app.schemas[name]
    ex = app.extractors.get(name, extract_entities)
    for env in app.sources[name](None):
        env["entities"] = extract(m, env, ex)
        check(env, val)


if __name__ == "__main__":
    main()
