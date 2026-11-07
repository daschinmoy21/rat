"""Offline END-TO-END-ish run: fresh process, real urllib HTTP, real adapters.

A local HTTP server on 127.0.0.1 serves fixtures.responses(), with an
optional per-request delay standing in for upstream latency. A child process
(the unmodified Python plugins + core extract/check, no Kafka) has urlopen's
destination rewritten to the loopback server; the request still goes through
urllib/http.client and a real TCP socket. Plain HTTP: no TLS handshake, no DNS,
no real upstream variance. Nothing touches a live feed.

Usage: loopback.py OUT_JSON   (env: PY, DELAYS_MS="0,50,200", E2E_RUNS=5)
"""

import json
import os
import statistics
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import fixtures

RESP = fixtures.responses()
STATE = {"delay": 0.0, "hits": 0}

CHILD = r"""
import sys, time, urllib.request
sys.path.insert(0, sys.argv[2])
import fixtures
port = int(sys.argv[1])
orig = urllib.request.urlopen
def via_loopback(req, timeout=None):
    url = req if isinstance(req, str) else req.full_url
    new = f"http://127.0.0.1:{port}{fixtures.path_of(url)}"
    if not isinstance(req, str):
        new = urllib.request.Request(new, headers=dict(req.header_items()))
    return orig(new, timeout=timeout)
urllib.request.urlopen = via_loopback
t0 = time.perf_counter()
from rat_producers.cli import load_app
import py_transform  # its in-memory urlopen patch is replaced below
urllib.request.urlopen = via_loopback
app = load_app([py_transform.PLUGINS])
t1 = time.perf_counter()
n = py_transform.cycle(app)
t2 = time.perf_counter()
hwm = [l.split()[1] for l in open("/proc/self/status") if l.startswith("VmHWM:")][0]
print(f"{n} {(t1-t0)*1e3:.3f} {(t2-t1)*1e3:.3f} {hwm}")
"""


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        STATE["hits"] += 1
        if STATE["delay"]:
            time.sleep(STATE["delay"])
        body = RESP.get(self.path)
        if body is None:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def main():
    out = Path(sys.argv[1])
    py = os.environ["PY"]
    runs = int(os.environ.get("E2E_RUNS", "5"))
    delays = [float(x) for x in os.environ.get("DELAYS_MS", "0,50,200").split(",")]
    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    env = {k: v for k, v in os.environ.items()
           if k in ("PATH", "HOME", "LANG", "PYTHONPATH", "PYTHONPYCACHEPREFIX",
                    "RAT_PLUGINS_DIR", "RAT_BENCH_HOME")}
    res = {}
    for d in delays:
        STATE["delay"] = d / 1000
        rows = []
        for i in range(runs + 1):  # first run is warmup
            STATE["hits"] = 0
            t0 = time.perf_counter()
            p = subprocess.Popen([py, "-c", CHILD, str(port), str(HERE)], env=env,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            _, status, ru = os.wait4(p.pid, 0)
            wall = (time.perf_counter() - t0) * 1e3
            outp, err = p.stdout.read().decode(), p.stderr.read().decode()
            if os.waitstatus_to_exitcode(status) != 0:
                raise SystemExit(err)
            n, load_ms, cycle_ms, hwm = outp.split()
            if i:
                rows.append({"wall_ms": wall, "cpu_ms": (ru.ru_utime + ru.ru_stime) * 1e3,
                             "vmhwm_kib": int(hwm), "events": int(n),
                             "load_ms": float(load_ms), "cycle_ms": float(cycle_ms),
                             "http_requests": STATE["hits"]})
        med = {k: statistics.median(r[k] for r in rows) for k in rows[0]}
        res[f"delay_{int(d)}ms"] = {"median": med, "raw": rows}
        print(f"delay {d:5.0f} ms/req: wall {med['wall_ms']:8.1f} ms  cpu {med['cpu_ms']:7.1f} ms  "
              f"cycle {med['cycle_ms']:8.1f} ms  peak RSS {med['vmhwm_kib']/1024:6.1f} MiB  "
              f"requests {med['http_requests']:.0f}  events {med['events']:.0f}", flush=True)
    srv.shutdown()
    out.write_text(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
