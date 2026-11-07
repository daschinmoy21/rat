#!/usr/bin/env python3
"""Sample only named cgroup-v2 workloads; never measure whole-host deltas.

Example: python3 tools/stack-bench/sample.py --seconds 30 --container rat-kafka
  --unit rat-bench-spark.scope --out docs/benchmarks/legacy-idle.json
CPU percentages use one logical core = 100%. Memory includes descendant tasks.
"""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import statistics
import subprocess
import time


def output(*args):
    return subprocess.check_output(args, text=True).strip()


def pairs(path):
    return {k: int(v) for k, v in (line.split() for line in path.read_text().splitlines())}


def read_group(path):
    mem = pairs(path / "memory.stat")
    cpu = pairs(path / "cpu.stat")
    current = int((path / "memory.current").read_text())
    io = {k: 0 for k in ("rbytes", "wbytes", "rios", "wios")}
    for line in (path / "io.stat").read_text().splitlines():
        for entry in line.split()[1:]:
            k, v = entry.split("=")
            if k in io:
                io[k] += int(v)
    return {
        "memory_bytes": current,
        "working_set_bytes": max(0, current - mem.get("inactive_file", 0)),
        "anon_bytes": mem.get("anon", 0),
        "swap_bytes": int((path / "memory.swap.current").read_text()),
        "cpu_usec": cpu["usage_usec"],
        **io,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", action="append", default=[])
    parser.add_argument("--unit", action="append", default=[])
    parser.add_argument("--seconds", type=float, default=30)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    paths = {}
    for name in args.container:
        pid = int(output("podman", "inspect", name, "--format", "{{.State.Pid}}"))
        if not pid:
            raise RuntimeError(f"{name} is stopped")
        cg = Path(f"/proc/{pid}/cgroup").read_text().strip().split("::", 1)[1]
        paths[name] = Path("/sys/fs/cgroup") / cg.lstrip("/")
    for name in args.unit:
        cg = output("systemctl", "--user", "show", name, "-p", "ControlGroup", "--value")
        if not cg:
            raise RuntimeError(f"{name} has no cgroup")
        paths[name] = Path("/sys/fs/cgroup") / cg.lstrip("/")
    if not paths:
        parser.error("name at least one --container or --unit")
    start = time.monotonic()
    samples = []
    while True:
        elapsed = time.monotonic() - start
        samples.append({"elapsed_s": elapsed, "groups": {n: read_group(p) for n, p in paths.items()}})
        if elapsed >= args.seconds:
            break
        time.sleep(min(args.interval, args.seconds - elapsed))
    duration = samples[-1]["elapsed_s"] - samples[0]["elapsed_s"]
    summary = {}
    for name in [*paths, "TOTAL"]:
        rows = [s["groups"][name] if name != "TOTAL" else {
            k: sum(g[k] for g in s["groups"].values())
            for k in next(iter(s["groups"].values()))
        } for s in samples]
        summary[name] = {
            "mean_memory_mib": statistics.mean(r["memory_bytes"] for r in rows) / 2**20,
            "mean_working_set_mib": statistics.mean(r["working_set_bytes"] for r in rows) / 2**20,
            "peak_working_set_mib": max(r["working_set_bytes"] for r in rows) / 2**20,
            "mean_anon_mib": statistics.mean(r["anon_bytes"] for r in rows) / 2**20,
            "mean_swap_mib": statistics.mean(r["swap_bytes"] for r in rows) / 2**20,
            "cpu_seconds": (rows[-1]["cpu_usec"] - rows[0]["cpu_usec"]) / 1e6,
            "cpu_percent_one_core": (rows[-1]["cpu_usec"] - rows[0]["cpu_usec"]) / duration / 1e4,
            "read_bytes": rows[-1]["rbytes"] - rows[0]["rbytes"],
            "write_bytes": rows[-1]["wbytes"] - rows[0]["wbytes"],
        }
    result = {
        "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "duration_s": duration, "logical_cpus": os.cpu_count(),
        "cgroups": {n: str(p) for n, p in paths.items()},
        "summary": summary, "samples": samples,
        "method": "cgroup v2; working set = memory.current - inactive_file; CPU 100% = one core; excludes build tools and other services",
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
