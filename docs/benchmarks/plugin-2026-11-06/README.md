# Plugin benchmark run, 2026-11-06

Raw output of `tools/plugin-bench/run.sh docs/benchmarks/plugin-2026-11-06`. The analysis is in
[../../python-vs-lua.md](../../python-vs-lua.md).

- Code measured: `producers/` and `plugins/` at `eb1620c` (via `git archive HEAD`). Other agents'
  uncommitted changes in the worktree (Iggy client, Rust port) were **not** measured.
- Machine: 4 vCPU AMD EPYC 9645 VM, 7.9 GiB RAM, Linux 6.12. It was shared with other agents
  compiling at the time: load average 6.25 at the start and 3.47 at the end (`env.txt`). Wall-time
  medians are noisy as a result. Use the `min`/`p90` columns in the JSON files, and prefer CPU time
  and RSS, which load disturbs less.
- Tools: Python 3.12.14, Lua 5.4.7 and LuaJIT 2.1.1785763465 from nixpkgs `8ce4ef6c`; mlua 0.12.2
  with vendored Lua 5.4 and LuaJIT (`mlua-Cargo.lock`), built with cargo 1.98.1.

| File | Content |
|---|---|
| `env.txt` | versions, machine, load |
| `fixtures.txt` | fixture size (43 responses, 35 210 bytes) |
| `extract-diff.{txt,json}` | Lua vs Python correctness diffs |
| `py-transform.{txt,json}` | Python in-process microbenchmark |
| `lua-transform.jsonl` | standalone Lua microbenchmark (`os.clock` CPU time) |
| `mlua-{lua54,luajit}.json` | embedded mlua probe |
| `mlua-*-envelopes.jsonl` | envelopes after the mlua round trip, as consumed by `extract-diff` |
| `py-importtime.txt` | `python -X importtime` for `load_app()` (third warm run) |
| `spawn.{txt,json}` | per-process wall, CPU and VmHWM, 30 runs after 3 warmups |
| `loopback.{txt,json}` | offline end-to-end Python cycle, 5 runs per injected delay |
