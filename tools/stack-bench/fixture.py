#!/usr/bin/env python3
"""Network-free, deterministic input: 4 envelopes => 3 pairs and 1 raw row.

Every group has a unique entity; all times fit within the join window, so
broker partition ordering cannot affect this parity workload.
"""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--groups", type=int, default=2500)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with args.out.open("wb") as f:
        for group in range(args.groups):
            for i, source in enumerate(("hn", "rss", "stocks", "rss")):
                row = {
                    "event_id": f"bench:{group}:{i}", "source": source,
                    "entities": [f"ENTITY{group}", f"ENTITY{group}"] if i < 3 else [],
                    "ts_ms": 1700000000000 + i * 30000,
                    "payload": {"title": f"Deterministic group {group}", "value": group},
                }
                line = (json.dumps(row, separators=(",", ":")) + "\n").encode()
                f.write(line)
                digest.update(line)
    print(json.dumps({"events": args.groups * 4, "expected_pairs": args.groups * 3,
                      "expected_raw": args.groups, "sha256": digest.hexdigest()}))


if __name__ == "__main__":
    main()
