#!/usr/bin/env python3
"""Independent row parity check of two Parquet roots (requires duckdb).

Normalizes JSON payload key order and Hive partition types, preserving row
multiplicity. Checks actual rows, not just counts. DuckDB is a verifier only;
the Rust application's SQL engine is DataFusion.
"""
import argparse
import hashlib
import json
from pathlib import Path
import duckdb


def snapshot(root):
    c = duckdb.connect()
    result = {}
    for table, columns in {
        "events_raw": ["event_id", "source", "entities", "ts_ms", "payload", "dt"],
        "correlated": ["entity", "a_id", "b_id", "ts_ms", "dt"],
    }.items():
        files = sorted(str(p) for p in (root / table).rglob("*.parquet")
                       if not any(part.startswith((".", "_")) for part in p.relative_to(root).parts))
        rows = []
        if files:
            for values in c.execute("SELECT " + ",".join(columns)
                                    + " FROM read_parquet(?, hive_partitioning=true)", [files]).fetchall():
                row = dict(zip(columns, values))
                row["dt"] = str(row["dt"]) if row["dt"] is not None else None
                if "payload" in row and row["payload"] is not None:
                    try:
                        row["payload"] = json.loads(row["payload"])
                    except (ValueError, TypeError):
                        pass
                rows.append(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
        rows.sort()
        digest = hashlib.sha256("\n".join(rows).encode()).hexdigest()
        result[table] = {"rows": rows, "count": len(rows), "files": len(files), "sha256": digest}
    c.close()
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("legacy", type=Path)
    p.add_argument("rust", type=Path, nargs="?")
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    snapshots = {"legacy": snapshot(args.legacy)}
    if args.rust:
        snapshots["rust"] = snapshot(args.rust)
        assert all(snapshots["legacy"][t]["rows"] == snapshots["rust"][t]["rows"]
                   for t in snapshots["legacy"]), "Parquet row content differs"
    result = {name: {t: {k: v for k, v in d.items() if k != "rows"}
                     for t, d in tables.items()} for name, tables in snapshots.items()}
    if args.rust:
        result["identical_rows"] = True
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
