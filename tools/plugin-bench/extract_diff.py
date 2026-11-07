"""Correctness check for the Lua port: does it produce what Python produces?

1. Extractors: feed one corpus to the real Python extractors and to the Lua
   ports (transform.lua extract mode, under each Lua runtime), list every
   input where outputs differ.
2. Envelopes: compare transform.lua `dump` with py_transform's map-only ports
   on the same decoded fixtures (after core extract).

Usage: extract_diff.py OUT_JSON   (env: LUA54, LUAJIT, FIXTURES_LUA)
"""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))

from rat_producers.cli import extract
from rat_producers.extract import extract_entities


def _load(name):
    plugins = Path(os.environ.get("RAT_PLUGINS_DIR", ROOT / "plugins"))
    spec = importlib.util.spec_from_file_location(f"b_{name}", plugins / name / "adapter.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


stocks, news = _load("stocks"), _load("news")

CORPUS = [
    # ordinary inputs
    "$AAPL beats estimates", "Why $tsla fell", "$AAPL and $MSFT and $AAPL",
    "MSFT and the cloud", "NVDA, AMD; INTC (chips)", "BRK.B rallies", "AAPL",
    "$TOOLONG ticker", "$AB1 digits", "$AAPL_X underscore", "$AAPL's quarter",
    "($GOOGL)", "[AMZN]", "\"TSLA\"", "the and of", "https://example.com/$AAPL/x",
    "", "   ", "BRK-B up 2%", "A.B.C.D.E.F.G",
    # Unicode: Python str regex/\s/upper() are Unicode-aware; Lua is bytewise
    "$ABé accent after cashtag", "café $TSLA", "AAPL MSFT (no-break space)",
    "straße", "ÄPPLE", " TSLA em-space", "ﬁ ligature", "$ÅAPL",
]


def lua_extract(lua, lines):
    p = subprocess.run([lua, str(HERE / "transform.lua"), "-", "extract"],
                       input="\n".join(lines) + "\n", capture_output=True, text=True,
                       check=True)
    return [json.loads(x) for x in p.stdout.splitlines()]


def py_extract(line):
    return [extract_entities(line), stocks.extract_stocks(line), news.extract_symbol(line)]


def compare(py_envs, lua_envs):
    def norm(e):  # Lua drops nil fields from tables; Python keeps None
        e = json.loads(json.dumps(e))
        e["payload"] = {k: v for k, v in e["payload"].items() if v is not None}
        return json.dumps(e, sort_keys=True)

    a = sorted(norm(e) for e in py_envs)
    b = sorted(norm(e) for e in lua_envs)
    seq = lambda es: [(e["source"], e["ts_ms"]) for e in es]
    return {"python": len(a), "lua": len(b),
            "identical_after_none_normalization": a == b,
            "mismatched_envelopes": len(set(a) ^ set(b)),
            "order_identical": [e["event_id"] for e in py_envs] == [e["event_id"] for e in lua_envs],
            "order_identical_up_to_equal_ts": seq(py_envs) == seq(lua_envs),
            "only_python": sorted(set(a) - set(b))[:3], "only_lua": sorted(set(b) - set(a))[:3]}


def py_envelopes():
    import py_transform as pt
    app = pt.app_for_bench()
    out = []
    for name, fn in (("hn", pt.map_hn), ("lobsters", pt.map_lobsters), ("news", pt.map_news),
                     ("rss", pt.map_rss), ("stocks", pt.map_stocks)):
        for env in fn(pt.decoded()[name]):
            env["entities"] = extract(app.manifests[name], env, app.extractors[name])
            out.append(env)
    return out


def envelope_diff(lua):
    p = subprocess.run([lua, str(HERE / "transform.lua"), os.environ["FIXTURES_LUA"], "dump"],
                       capture_output=True, text=True, check=True)
    return compare(py_envelopes(), [json.loads(x) for x in p.stdout.splitlines()])


def main():
    out = Path(sys.argv[1])
    res = {"corpus_size": len(CORPUS)}
    py = [py_extract(x) for x in CORPUS]
    for label, lua in (("lua54", os.environ["LUA54"]), ("luajit", os.environ["LUAJIT"])):
        lu = lua_extract(lua, CORPUS)
        diffs = []
        for text, a, b in zip(CORPUS, py, lu):
            for fn, x, y in zip(("extract_entities", "extract_stocks", "extract_symbol"), a, b):
                if x != y:
                    diffs.append({"input": text, "fn": fn, "python": x, "lua": y})
        res[label] = {"extractor_mismatches": diffs, "envelopes": envelope_diff(lua)}
        print(f"{label}: {len(diffs)} extractor mismatches over {len(CORPUS)} inputs x 3 fns; "
              f"envelopes {res[label]['envelopes']}")
        for d in diffs:
            print(f"   {d['fn']:17s} {d['input']!r}: py={d['python']} lua={d['lua']}")
    for f in sorted(Path(os.environ.get("MLUA_DIR", "/nonexistent")).glob("mlua-*-envelopes.jsonl")):
        lua_envs = [json.loads(x) for x in f.read_text().splitlines() if x]
        res[f.name] = compare(py_envelopes(), lua_envs)
        res[f.name]["entities_not_a_list"] = sum(
            not isinstance(e["entities"], list) for e in lua_envs)
        print(f"{f.name}: {res[f.name]}")
    out.write_text(json.dumps(res, indent=1, ensure_ascii=False))


if __name__ == "__main__":
    main()
