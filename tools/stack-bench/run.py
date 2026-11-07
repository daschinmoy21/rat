#!/usr/bin/env python3
"""Measure a finite command, separating its stdout from resource statistics.

Peak VmHWM is sampled from /proc (5ms). wait4 also records CPU and kernel
ru_maxrss; the latter may include a forking parent's inherited footprint.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--out", type=Path, required=True)
p.add_argument("--stdout", type=Path, required=True)
p.add_argument("command", nargs=argparse.REMAINDER)
args = p.parse_args()
command = args.command[1:] if args.command[:1] == ["--"] else args.command
if not command:
    p.error("a command is required")
peak = 0
start = time.monotonic()
with args.stdout.open("w") as stdout:
    child = subprocess.Popen(command, stdout=stdout)
    while True:
        try:
            status = Path(f"/proc/{child.pid}/status").read_text()
            for line in status.splitlines():
                if line.startswith("VmHWM:"):
                    peak = max(peak, int(line.split()[1]))
        except (FileNotFoundError, ProcessLookupError):
            pass
        pid, code, usage = os.wait4(child.pid, os.WNOHANG)
        if pid:
            child.returncode = os.waitstatus_to_exitcode(code)
            break
        time.sleep(.005)
elapsed = time.monotonic() - start
result = {"command": command, "exit_code": child.returncode,
          "elapsed_s": elapsed, "cpu_seconds": usage.ru_utime + usage.ru_stime,
          "sampled_peak_rss_mib": peak / 1024,
          "kernel_peak_rss_mib": usage.ru_maxrss / 1024,
          "cpu_percent_one_core": (usage.ru_utime + usage.ru_stime) / elapsed * 100}
args.out.write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))
raise SystemExit(child.returncode)
