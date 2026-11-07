"""Process-level cost: wall time, CPU time and peak RSS of fresh processes.

Each case is a full fork+exec+exit. CPU time comes from os.wait4 rusage.
Peak memory is the child's own VmHWM, which it prints from /proc/self/status
just before exiting: Linux folds the pre-exec (forked parent) RSS into
ru_maxrss, so ru_maxrss has a floor at this harness's own RSS and is kept only
as a cross-check (maxrss_kib_inherited). Warmup runs are
discarded; they also populate PYTHONPYCACHEPREFIX so measured Python runs read
warm bytecode, like a long-lived install does.

Usage: spawn.py OUT_JSON  (tool paths come from env: PY, LUA54, LUAJIT, BENCH)
"""

import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

RUNS = int(os.environ.get("RUNS", "30"))
WARMUP = int(os.environ.get("WARMUP", "3"))


def measure(argv, env):
    rows = []
    for i in range(WARMUP + RUNS):
        t0 = time.perf_counter()
        p = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        _, status, ru = os.wait4(p.pid, 0)
        wall = time.perf_counter() - t0
        p.returncode = os.waitstatus_to_exitcode(status)
        out = p.stdout.read().decode(errors="replace")
        err = p.stderr.read().decode(errors="replace")
        p.stdout.close()
        p.stderr.close()
        hwm = [int(line.split()[1]) for line in out.splitlines() if line.startswith("VmHWM:")]
        if p.returncode != 0:
            raise SystemExit(f"{argv} exited {p.returncode}: {err[-2000:]}")
        if i >= WARMUP:
            if not hwm:
                raise SystemExit(f"{argv} did not report VmHWM")
            rows.append({"wall_ms": wall * 1e3, "cpu_ms": (ru.ru_utime + ru.ru_stime) * 1e3,
                         "vmhwm_kib": hwm[-1], "maxrss_kib_inherited": ru.ru_maxrss})
    return rows


def summary(rows):
    out = {}
    for k in rows[0]:
        xs = sorted(r[k] for r in rows)
        out[k] = {"median": statistics.median(xs), "min": xs[0], "max": xs[-1],
                  "p90": xs[int(0.9 * (len(xs) - 1))]}
    return out


def main():
    out = Path(sys.argv[1])
    py, lua54, luajit, bench = (os.environ[k] for k in ("PY", "LUA54", "LUAJIT", "BENCH"))
    base = {k: v for k, v in os.environ.items()
            if k in ("PATH", "HOME", "LANG", "LC_ALL", "PYTHONPATH",
                     "PYTHONPYCACHEPREFIX", "RAT_PLUGINS_DIR", "RAT_BENCH_HOME")}
    fx = os.environ["FIXTURES_LUA"]
    PYS = ";import sys;sys.stdout.write(open('/proc/self/status').read())"
    LUAS = "io.write(io.open('/proc/self/status'):read('*a'))"
    isolated = ('from pathlib import Path; import os; '
                'Path.home=classmethod(lambda cls: Path(os.environ["RAT_BENCH_HOME"])); ')
    cases = {
        "py_bare_IS": [py, "-I", "-S", "-c", "pass" + PYS],
        "py_site": [py, "-c", "pass" + PYS],
        "py_import_core": [py, "-c", "import rat_producers.cli" + PYS],
        "py_load_plugins": [py, "-c",
                            isolated + "from rat_producers.cli import load_app; load_app()" + PYS],
        "py_load_plugins_kafka": [py, "-c",
                                  isolated + "import kafka; from rat_producers.cli import load_app; "
                                  "load_app()" + PYS],
        "py_one_cycle_offline": [py, f"{bench}/py_transform.py", "--once"],
        "lua54_bare": [lua54, "-e", LUAS],
        "luajit_bare": [luajit, "-e", LUAS],
        "lua54_one_cycle": [lua54, f"{bench}/transform.lua", fx, "once"],
        "luajit_one_cycle": [luajit, f"{bench}/transform.lua", fx, "once"],
    }
    results = {}
    for name, argv in cases.items():
        rows = measure(argv, base)
        results[name] = {"argv": argv, "runs": RUNS, "summary": summary(rows), "raw": rows}
        s = results[name]["summary"]
        print(f"{name:24s} wall {s['wall_ms']['median']:8.2f} ms  "
              f"cpu {s['cpu_ms']['median']:8.2f} ms  "
              f"peak RSS (VmHWM) {s['vmhwm_kib']['median'] / 1024:7.2f} MiB", flush=True)
    out.write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
    main()
