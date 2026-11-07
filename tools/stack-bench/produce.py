#!/usr/bin/env python3
"""Replay fixture JSONL using the real acknowledged Python producer path."""
import argparse
import json
import time
from rat_producers.producer import connect, emit

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("input")
args = p.parse_args()
producer = connect()
start = time.monotonic()
count = 0
try:
    with open(args.input) as source:
        for line in source:
            emit(json.loads(line), producer)
            count += 1
    producer.flush(timeout=30)
finally:
    producer.close(timeout=30)
elapsed = time.monotonic() - start
print(json.dumps({"events_attempted": count, "elapsed_s": elapsed,
                  "attempts_per_s": count / elapsed,
                  "note": "emit may DLQ/spool after send failures; verify broker counts and sinks separately"}))
