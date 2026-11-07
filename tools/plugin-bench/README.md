# plugin-bench

Evidence for [docs/python-vs-lua.md](../../docs/python-vs-lua.md). Everything runs offline: no live
feed, no Kafka/Iggy, no service started or stopped.

```bash
uv sync --frozen                                  # .venv supplies jsonschema + kafka-python
tools/plugin-bench/run.sh docs/benchmarks/plugin-$(date +%F)
# knobs: RUNS=30 WARMUP=3 REPS=300 E2E_RUNS=5 DELAYS_MS=0,50,200 BENCH_REF=HEAD SKIP_MLUA=1
```

`run.sh` pulls Python 3.12, Lua 5.4 and LuaJIT from the nixpkgs revision in `flake.lock`. It
benchmarks `producers/` and `plugins/` from a `git archive` of `BENCH_REF`, so half-edited
worktree files never get measured. It redirects `PYTHONPYCACHEPREFIX` and uses
`RAT_BENCH_HOME` to mock Python's `Path.home()` inside measured plugin-loading
processes, so personal plugin overrides are ignored. The actual `HOME` stays unchanged.

| File | Kind | What it measures |
|---|---|---|
| `fixtures.py` | data | Deterministic synthetic upstream responses for all five plugins (43 URLs, 187 events per cycle). The sizes are guesses where the adapter code does not fix them. |
| `py_transform.py` | micro | Real adapters with `urlopen` swapped for an in-memory lookup, run through the real core `extract` + `schema.check`. Also map-only ports that start from decoded dicts. |
| `transform.lua` | micro | Lua port of the five map + extract steps, starting from decoded tables. Runs on Lua 5.4 and LuaJIT, and loads as a module into `mlua-host`. |
| `mlua-host/` | micro | Standalone Rust crate on mlua 0.12.2 with its own `[workspace]`, not part of the repo workspace. Measures state creation, serde→Lua→serde per cycle, error isolation, the memory limit, and hook-based stopping of runaway loops. |
| `extract_diff.py` | correctness | Python extractors/envelopes vs the Lua port and vs the mlua round trip. |
| `spawn.py` | process | Fresh-process wall time, CPU time and peak RSS (`VmHWM` from the child itself). |
| `loopback.py` | end-to-end (offline) | Fresh Python process, real urllib over TCP to a 127.0.0.1 fixture server with an injected per-request delay. Plain HTTP: no TLS, no DNS. |

`run-mlua.sh` needs cargo ≥ 1.88 and crates.io access for its first build. It keeps its target
dir outside the repo.
