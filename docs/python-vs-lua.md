# Python plugins vs Lua plugins for the Rust port

**Question.** The Rust port (`rat-rs`, [rust-interface.md](rust-interface.md)) replaces the Spark/Hive
side. The Python producers are kept and publish envelopes, through Iggy in the new path. Should the
five Python source plugins be rewritten in Lua and embedded in a Rust host with mlua?

**Answer: not now.** Keep the plugins in Python as a separate producer process that speaks the
existing envelope contract. Standalone Lua has a smaller footprint and starts much faster. But the
producer is a long-lived process whose cycle is dominated by network waits. On the measured
workload, an embedded Lua plugin does not save CPU, and a straight port does not reproduce today's
output. Revisit only on the triggers in [§8](#8-recommendation). If a revisit happens, use PUC
Lua 5.4/5.5 through mlua, not LuaJIT. The reasons are in §5.3.

---

## 1. Scope and method

| | Covered | Not covered |
|---|---|---|
| Code | `producers/` and `plugins/` at `eb1620c` (via `git archive`) | Uncommitted Iggy and Rust work in the worktree |
| Microbenchmarks | Python adapters in-process with the network stubbed; the Lua port standalone; the Lua port embedded via mlua 0.12.2 | Rust-side HTTP and JSON/XML parsing that a Lua host would also need |
| End-to-end | **Offline only**: a fresh Python process doing real urllib → TCP → a 127.0.0.1 fixture server with an injected delay | Live feeds (forbidden here), TLS, DNS, real upstream variance, Kafka/Iggy publish, `uv run`/nix start-up wrapper |
| Data | Synthetic and deterministic, 187 events per cycle, 43 HTTP requests | Real feed sizes. RSS/news/lobsters item counts are guesses (`tools/plugin-bench/fixtures.py`) |

Reproduce with `tools/plugin-bench/run.sh`. Raw output is in
[benchmarks/plugin-2026-11-06/](benchmarks/plugin-2026-11-06/). The host was a shared 4 vCPU VM
with a load average of 3.5–6.3 during the run (`env.txt`). Wall-clock medians are therefore noisy.
CPU time and RSS are more trustworthy, and the min and p90 values are kept in the JSON. "Micro" in
this document means one cycle of in-memory work. It is **not** a statement about end-to-end
latency. Whole-stack metrics are in [resource-comparison.md](resource-comparison.md).

## 2. What the plugins actually are

| Plugin | Requests per cycle | Upstream format | Stdlib used (adapter) | Adapter LOC | Adapter tests |
|---|---|---|---|---|---|
| hn | 31, sequential (topstories + 30 items) | JSON | `json`, `urllib.request` | 39 | 2 |
| lobsters | 1 | JSON | `json`, `urllib`, `http.client`, `datetime.fromisoformat`, `logging` | 53 | 10 |
| news | 5 (one per symbol) | RSS/XML | `xml.etree`, `email.utils`, `re`, `tomllib`, `urllib.parse` | 92 | 6 |
| rss | 1 per configured feed | RSS/XML | `xml.etree`, `email.utils`, `tomllib` | 82 | 5 |
| stocks | 5 (one per symbol) | JSON (Yahoo chart) | `json`, `re`, `tomllib` | 129 | 6 |

Line counts and test counts come from `git show HEAD:…`. Every adapter uses only the stdlib. The
core adds `jsonschema` for payload validation (`producers/rat_producers/schema.py`) and
`kafka-python` (`producers/pyproject.toml`). The plugin contract is a `register(app)` hook plus a
`poll(since_ms) -> list[envelope]` function, loaded with `importlib`
(`producers/rat_producers/loader.py:26-43`). Configuration comes from `plugin.toml`, overridden by
`~/.config/rat/<name>.toml`. Payloads are checked against each plugin's `schema.json`. Three
plugins use the core cashtag extractor (`extract.py`), stocks has its own regex and stop-word
extractor, and news has its own ticker regex.

How the runtime behaves today (`producers/rat_producers/cli.py:61-87`, HEAD):

- **One process, one thread.** Every source runs sequentially in `rat run`, under systemd
  `Restart=on-failure` (`deploy/systemd/user/rat-producers.service`).
- **Isolation is a `try/except Exception` per source** (`cli.py:77`). A plugin that raises is
  logged and skipped. A plugin that *hangs*, spins or exhausts memory stalls or kills every source.
  The only bound is `urlopen(timeout=10)`, which covers "blocking operations like the connection
  attempt" ([S12]), not the whole poll. hn makes 31 such calls in a row.
- **Every source polls on the shortest interval** (`wait = min(...)`, `cli.py:83`). The 5 m
  manifests for rss and lobsters are not honoured per source. This is existing behaviour, unrelated
  to the language choice, and it doubles as a reminder that the scheduler belongs in core.
- Plugins run arbitrary code with the user's privileges. They can be discovered from
  `/var/lib/rat/plugins` and `~/.config/rat/plugins` ([plugins.md](plugins.md) §2), and there is no
  sandbox.

## 3. Measurements

### 3.1 Process start-up and footprint (fresh process, 30 runs, `spawn.json`)

Peak RSS is the child's own `VmHWM`. `ru_maxrss` was rejected because Linux folds the forking
parent's RSS into it. That artifact pinned every small process at 14.4 MiB in an earlier run.

| Case | Wall median (min) | CPU median | Peak RSS |
|---|---|---|---|
| `python -I -S -c pass` | 9.2 ms (7.7) | 8.6 ms | 10.0 MiB |
| `python -c pass` (with site) | 14.6 ms (13.0) | 14.2 ms | 11.0 MiB |
| `import rat_producers.cli` | 230 ms (188) | 227 ms | 35.3 MiB |
| `load_app()`: 5 plugins + schemas | 307 ms (255) | 297 ms | 37.3 MiB |
| …plus one offline cycle (poll + extract + check) | 330 ms (254) | 289 ms | 39.0 MiB |
| `lua5.4 -e ''` | 0.99 ms (0.89) | 0.86 ms | 2.7 MiB |
| `luajit -e ''` | 0.78 ms (0.61) | 0.67 ms | 2.8 MiB |
| Lua 5.4: load decoded fixtures + one cycle | 3.8 ms (2.8) | 2.9 ms | 3.4 MiB |
| LuaJIT: the same | 3.3 ms (2.4) | 2.7 ms | 3.4 MiB |

Most of the Python start-up cost is imports, not plugins. In `-X importtime` ([S10],
`py-importtime.txt`), `rat_producers.cli` takes 167 ms cumulative. Of that, `kafka` takes 84 ms
(pulled in by `rat_producers.status`) and `jsonschema` 53 ms. The five adapters together add about
75 ms (307 − 230).

### 3.2 Transform work per cycle (in-process microbenchmark, 187 events)

| Variant | Median (min / p90) | Notes |
|---|---|---|
| Python, real adapters, `poll` only (parse + map) | 1.75 ms (1.42 / 2.16) | `urlopen` stubbed in memory |
| …+ core entity extract | 2.06 ms | |
| …+ `jsonschema` check | **5.93 ms** (5.07 / 6.89) | Validation is about ⅔ of the cycle and lives in core, not in plugins |
| Python map-only port, from decoded dicts | 0.84 ms (0.79 / 0.94) | Bench code, the counterpart of the Lua port |
| Lua 5.4 standalone, map-only, from decoded tables | 0.67 ms (0.62 / 0.74) | `os.clock` CPU time |
| LuaJIT standalone, the same | 0.34 ms (0.30 / 0.38) | |
| **mlua + Lua 5.4 embedded**: serde→Lua, call, Lua→serde | **1.65 ms** (to 0.24 + call 0.80 + from 0.60) | mlua 0.12.2, vendored |
| **mlua + LuaJIT embedded**: the same | **1.16 ms** (0.16 + 0.43 + 0.53) | |
| mlua: new state + load module | 0.28 ms (5.4) / 0.31 ms (JIT) | 73 KB / 102 KB `used_memory` |
| mlua: 5 extra plugin states in one process | +0.27 MiB (5.4) / +0.48 MiB (JIT) RSS | `rss_kib` in `mlua-*.json` |

Embedded, the Lua plugin is *slower* than the Python map-only code for this workload. Moving
values across the boundary (0.7–0.8 ms per cycle) costs more than the Lua code itself. Neither
variant includes JSON/XML parsing, which a Rust host would add (serde_json/quick-xml). That cost
was not measured here.

### 3.3 Offline end-to-end: where the time goes (`loopback.json`, 5 runs each)

| Injected delay per request | Process wall | Process CPU | Poll cycle wall | Peak RSS |
|---|---|---|---|---|
| 0 ms | 0.42 s | 0.31 s | 112 ms (runs: 36–167) | 41.6 MiB |
| 50 ms | 2.59 s | 0.34 s | 2.26 s | 41.5 MiB |
| 200 ms | 9.11 s | 0.34 s | 8.76 s | 41.6 MiB |

The cycle's wall time scales with 43 sequential requests × delay, while process CPU stays at about
0.3 s, and nearly all of that is start-up. With the network removed, the CPU work of one cycle is
about 6 ms (§3.2). Against the 60 s loop that is ≈0.01 % of one core whatever language does the
mapping. The costs that matter are **sequential network waits**, a function of how the adapters
issue requests and independent of the plugin language, and the **resident Python process**: about
40 MiB, which is ~0.5 % of this 7.9 GiB box.

## 4. Compatibility: does a Lua port produce the same events? (`extract-diff.txt`)

The comparison ran a 28-input corpus × 3 extractors, plus all 187 envelopes.

1. **ASCII text: identical.** The standalone Lua 5.4 port matched Python on all 187 envelopes once
   Python's `None` fields were removed (a Lua table cannot hold `nil`).
2. **Unicode text: 9/84 extractor results differ**, all on non-ASCII inputs. Python's `\b`, `\w`
   and `\s` are Unicode-aware for `str` patterns ([S9]). Lua's `%a`, `%w` and `%s` are bytewise
   and "depend on the current locale" ([S7] §6.4.1). Measured cases include: `"$ABé"` (Python `[]`,
   Lua `["AB"]`); no-break space and em-space separators (Python splits, Lua does not);
   `"straße".upper()` → `STRASSE` and `"ﬁ"` → `FI` in Python only. Each of these changes `entities`,
   which are the join keys, so they change correlation output.
3. **Ordering**: identical apart from ties on equal `ts_ms`. Lua's `table.sort` is not stable,
   while Python's `sort` is. News headlines share timestamps across symbols, so emit order changes.
4. **LuaJIT numbers**: LuaJIT has no integer subtype ([S6]: it is a Lua 5.1 implementation), so an
   integral float price `100.0` encodes as `100`. The schema still passes, but the bytes differ.
5. **mlua round trip: 49/187 envelopes came back with `"entities": {}`** on both flavours. Empty
   tables created in Lua serialize as JSON objects. Empty arrays that came *from* the host stayed
   arrays (0 lobsters `tags` broke). The current core would reject those 49 rows with "entities not
   a list of strings" and route them to the DLQ (`schema.py:28`). The fix exists, an array
   metatable that makes a table "systematically encode … as Array" ([S2] `serde/mod.rs`), but every
   plugin author has to remember to use it.

## 5. The debate

### 5.1 The case for Lua

1. **One binary, smaller footprint.** An embedded state costs about 0.3 ms and 70–100 KB. Five
   plugin states add under 0.5 MiB to a Rust process (§3.2), against about 40 MiB for a resident
   CPython producer (§3.3), and the separate interpreter, venv, `uv` and nix wrapper go away. On a
   small VPS or ARM box that is the difference between one supervised process and two.
2. **Start-up.** 3–4 ms for a full Lua cycle versus about 330 ms for Python (§3.1). If plugins were
   ever spawned per poll, or restarted often, Python's import cost (`kafka` + `jsonschema`) would
   dominate.
3. **Real in-process isolation primitives that the Python core lacks today.** With mlua on Lua 5.4:
   a Lua `error()` surfaced as a Rust `Err` and left the state usable; `set_memory_limit(4 MiB)`
   turned a runaway allocation into `memory error: not enough memory` (on LuaJIT as well,
   because x64 builds default to GC64, where `lua_newstate` accepts mlua's allocator [S4]); and an instruction hook
   stopped `while true do end` at its 200 ms deadline (`mlua-lua54.json`). `Lua::new` refuses the
   unsafe `debug` and `ffi` libraries and disables C modules ([S2] `stdlib.rs`, `state/raw.rs`).
   Today a hung Python plugin blocks every source (§2).
4. **A narrower plugin surface.** Lua's stdlib has no HTTP, JSON, XML or sockets ([S7] §6). The
   host would *have to* own fetching, decoding, timeouts, retries and concurrency. That centralises
   policy, such as per-source intervals, parallel fetches for hn's 31 calls (mlua async runs on
   coroutines with any executor [S1]), and one TLS stack. Plugins shrink to pure `map(decoded) ->
   envelopes`, which is what `transform.lua` is: about 310 lines for all five adapters.
5. **Language stability.** Lua 5.4 is final at 5.4.9 ("There will be no further releases of
   Lua 5.4", [S8]), so a plugin written against it never moves. CPython 3.12 leaves security
   support around October 2028 ([S13]).

### 5.2 The case for Python

1. **The savings are in the wrong place.** The producer is long-lived, so start-up is paid once per
   restart. Per-cycle CPU is about 6 ms per 60 s in either language. Wall time is network wait that
   grows with request count × RTT (§3.3). Embedded Lua was *slower* per cycle than Python map-only
   once the serde boundary is counted (1.16–1.65 ms vs 0.84 ms, §3.2). What remains is about
   40 MiB of RSS.
2. **The port is not a translation; it is a host API.** The five adapters use urllib,
   `http.client`, `json`, `xml.etree`, `email.utils`, `datetime.fromisoformat`, `tomllib`, `re`,
   `logging` and `pathlib` (§2). Lua has none of them (§5.1 point 4). The Rust side would have to
   design, document and version a host API for HTTP, decoding, config, logging and time parsing
   *before* the first plugin moves. That includes choosing a hardened XML parser for untrusted
   feeds, a concern Python's own docs raise for `xml.etree` [S11]. The bench port had to hand-write RFC 2822 and ISO-8601 parsers,
   and they cover less than `email.utils.parsedate_to_datetime`, for example obsolete zone names.
3. **A straight port changes the data.** It changed Unicode entity extraction (9/84), tie order,
   LuaJIT number encoding and, through mlua, 26 % of envelopes' `entities` shape (§4). Each of
   these is fixable, but each is a regression found only because a differential harness existed.
   The 29 adapter tests in `producers/tests/` would have to be rewritten in a Lua or Rust harness.
4. **Isolation is better done with a process boundary.** A separate producer process already keeps
   plugin crashes and leaks out of the Rust engine, and systemd restarts it. The per-plugin gaps in
   §2 (hangs, a shared thread) are fixable in Python with a per-source deadline, a worker thread or
   a subprocess per source. None of that requires a language change. mlua itself says it "does not
   provide absolute safety even without using `unsafe`" ([S1]). An in-process VM fault reaches the
   engine, whereas a sidecar fault does not.
5. **The contract already decouples the two sides.** The Rust consumer reads envelopes from Iggy
   and is indifferent to the producer's language ([rust-interface.md](rust-interface.md), "Contract
   with the Iggy / producer agent"). Contributors add a source by writing about 40–130 lines of
   ordinary Python ([plugins.md](plugins.md)). The pool of people who can review and debug that is
   larger than for Lua + mlua + Rust FFI. If Rust ever has to call Python in-process, PyO3 supports
   embedding CPython ≥ 3.9 ([S14]).

### 5.3 Cross-examination

| Claim | Verdict | Evidence |
|---|---|---|
| "Lua is faster" | True for start-up (~100×) and standalone transforms (LuaJIT ~2.5× Python map-only). **False** for embedded per-cycle cost on this workload. Irrelevant to wall time either way | §3.1, §3.2, §3.3 |
| "Lua is lighter" | True: ~3 MiB for a process, <0.5 MiB for 5 embedded states, vs ~40 MiB for CPython. This is the one material win | §3.1–3.3 |
| "Lua sandboxes plugins" | Partly. PUC Lua 5.4 + mlua: error, memory and instruction limits all worked. **LuaJIT: the instruction hook never fired** on two runaway loops (still running after 3 s, `mlua-luajit.json`), so CPU limits are not enforceable in-process. Luau's `sandbox()` is Luau-only ([S3]) | §4, mlua probe |
| "Python can't isolate plugins" | False in principle, true today. The current single-thread `try/except` doesn't bound hangs, but a process or worker per source does | `cli.py:61-87` |
| "Network waits make the language moot" | True for latency and CPU. Not for memory or deployment shape | §3.3 |
| "Lua plugins are as easy to write" | False today. There is no host API yet, and empty tables, Unicode and integers are footguns | §4, §5.2 |
| "LuaJIT is the obvious Lua" | No. It is Lua 5.1 with partial 5.2 ([S6]), has rolling releases from git with "No release tarballs" ([S5]), its hook limits failed here, and it adds number-encoding drift. Prefer `lua54` (or `lua55`, current 5.5.1 [S8]) in mlua | [S5], [S6], §4 |

## 6. Costs a migration would carry

The following is the scope of the work. Effort is not estimated in days here, because no
measurement supports a number.

- Rust host API: HTTP with timeouts and concurrency, JSON and XML decoding into Lua tables, TOML
  config with the `~/.config/rat` override order, logging, time parsing, and array-metatable
  helpers for lists.
- Manifest change from `adapter.py` to `adapter.lua`, plus loader and discovery.
- Five adapters (395 LOC) and 29 adapter tests.
- A differential gate: `tools/plugin-bench/extract_diff.py` extended to real recorded fixtures,
  plus a written Unicode policy, with **zero** envelope diffs required before cut-over.
- Two plugin ABIs during the transition, or a flag day.

## 7. What to do instead (cheap, language-neutral)

These address the measured problems. They are suggestions for the producer owner; this document
does not change runtime behaviour.

1. Bound each source by a wall-clock deadline (worker thread or subprocess per source) so one
   upstream cannot stall the rest (§2).
2. Schedule per source instead of `min(interval)` (§2).
3. Fetch hn items concurrently, or cap them: 31 sequential requests make up most of the requests in
   a cycle (§2, §3.3).
4. Lazy-import `kafka` and `jsonschema` paths that `rat run` does not need. They account for most of
   the 230 ms import cost (§3.1). This only matters if restarts are frequent.
5. Plan the CPython upgrade (3.13 or later) well ahead of the October 2028 end of 3.12 security
   support ([S13]).

## 8. Recommendation

**Do not migrate the Python plugins to Lua as part of the Rust port.** Keep the producer as a
supervised Python process on the envelope/Iggy contract. Spend the effort on §7.

Re-open the question if any of these becomes true, and decide with the harness rather than by
argument:

- Producers must run inside the single `rat-rs` binary, or on hardware where about 40 MiB of RSS
  per producer process is a real constraint.
- Third-party or untrusted plugins have to be supported, which needs in-process memory and CPU
  limits.
- Plugin count or restart frequency grows until CPython start-up and RSS show up in the parent's
  stack metrics.

If it is re-opened: use mlua with `lua54` or `lua55` (not LuaJIT); make the host own all I/O and
decoding, with Lua as `map(decoded) -> envelopes` only; enforce limits with `set_memory_limit` and
`set_hook`; and require a zero-diff differential run against the Python plugins first.

## 9. Sources

Primary sources, fetched 2026-11-06. Repo files are cited inline by path and line at `eb1620c`.

- [S1] mlua README, v0.12.2: <https://github.com/mlua-rs/mlua>. Features (`lua54`, `luajit`,
  `luau`, `vendored`, `async`, `send`, `serde`); "mlua does not provide absolute safety even
  without using `unsafe`"; panics wrapped as Lua errors.
- [S2] mlua v0.12.2 source: `src/state.rs` (`set_memory_limit`, `set_hook` doc "limited form of
  execution limits"), `src/state/raw.rs` (custom allocator falling back to `luaL_newstate`; `debug`
  and `ffi` refused in safe mode), `src/stdlib.rs` (`DEBUG`, `FFI` marked unsafe), and
  `src/serde/mod.rs` (`array_metatable`), all under
  <https://github.com/mlua-rs/mlua/tree/v0.12.2/src>. `Cargo.toml` gives `rust-version = "1.88"`.
- [S3] mlua API docs, `Lua`: <https://docs.rs/mlua/latest/mlua/struct.Lua.html>. `sandbox` and
  `set_interrupt` are Luau-only; `set_hook` is non-Luau.
- [S4] LuaJIT source, v2.1 branch: `src/lj_arch.h` (GC64 is the default on x64) and `src/lj_state.c`
  (`lua_newstate`), <https://github.com/LuaJIT/LuaJIT/tree/v2.1/src>.
- [S5] LuaJIT status: <https://luajit.org/status.html>. "LuaJIT uses rolling releases … No release
  tarballs or binaries are made available"; v2.1 is the production branch.
- [S6] LuaJIT extensions: <https://luajit.org/extensions.html>. "LuaJIT is API+ABI-compatible with
  Lua 5.1, which prevents implementing features that would otherwise break the Lua/C API and ABI
  (e.g. _ENV)"; only partial 5.2 compatibility.
- [S7] Lua 5.4 Reference Manual: <https://www.lua.org/manual/5.4/manual.html>. §6.4.1: "The
  definitions of letter, space, and other character groups depend on the current locale"; §6
  lists the standard libraries (basic, coroutine, package, string, utf8, table, math, io, os,
  debug). There is no JSON, XML, HTTP or regex library.
- [S8] Lua versions: <https://www.lua.org/versions.html>. The current release is Lua 5.5.1 (03 Aug
  2026); 5.4.9 (25 Aug 2026) is the last, and "There will be no further releases of Lua 5.4."
- [S9] Python 3.12 `re`: <https://docs.python.org/3.12/library/re.html>. For `str` patterns, `\b`,
  `\w` and `\s` are Unicode-aware.
- [S10] Python 3.12 command line: <https://docs.python.org/3.12/using/cmdline.html> (`-X importtime`,
  `-I`, `-S`).
- [S11] Python 3.12 `xml.etree.ElementTree`:
  <https://docs.python.org/3.12/library/xml.etree.elementtree.html>. "not secure against
  maliciously constructed data". This applies equally to whatever XML parser a Rust host would use
  for feeds.
- [S12] Python 3.12 `urllib.request.urlopen`:
  <https://docs.python.org/3.12/library/urllib.request.html>. The timeout covers "blocking
  operations like the connection attempt".
- [S13] PEP 693, Python 3.12 release schedule: <https://peps.python.org/pep-0693/>. Security fixes
  until approximately October 2028.
- [S14] PyO3 user guide: <https://pyo3.rs/latest/>. Embedding Python in Rust; supports CPython 3.9+
  and requires Rust 1.85+.

Firecrawl was not available in this session. The pages above were fetched directly (curl/WebFetch)
and the quotes checked against the fetched text.
