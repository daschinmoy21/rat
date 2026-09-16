# Contributing

Python producers. Scala job. Hive is query-only. No PySpark. Producers never write Hive.

## Shell

[docs/setup.md](docs/setup.md). Then:

```bash
direnv allow          # Nix
uv sync --frozen      # always
./scripts/check-deps.sh
```

Fix the machine if that fails.

## Where

| Change | Where |
|---|---|
| Source | plugin, see [docs/plugins.md](docs/plugins.md). Loaded by `rat_producers.loader` |
| Envelope | `events.py` and `Event.scala` in the same PR |
| Job | `spark/src/main/scala/rat/` |
| Python lib | `uv add`, commit `uv.lock` |
| Spark lib | `spark/build.sbt` (3.5.3 / 2.13.14) |
| Tool pins | `flake.nix` and `mise.toml` together |
| Topics | `plugins/*/plugin.toml` + one `./scripts/make-topics.sh` run |
| Ops | `rat status` (broker, cursors, DLQ depth, sinks) · `rat dlq [--topic T] [--n N]` |
| Docs | `docs/` |

RSS **feed** = config. New **kind** of source (HN, stocks, IMAP) = plugin.

## Envelope

Python: `event_id`, `ts_ms`. Scala: `eventId`, `tsMs`. Same fields. Payload stays plugin JSON.

## Git

Do not commit `.venv/`, `target/`, `.env`, secrets. Branch off `main`, small PR, say what you ran (`uv sync`, `sbt compile`).

Kafka runs in-tree: [infra/kafka/compose.yml](infra/kafka/compose.yml) — `uv run rat run --once` emits live. `Correlate` streams the `events.*` topics through the watermark join into checkpointed Parquet; with `RAT_HIVE_ENABLED=true` + HDFS and a metastore ([infra/hadoop](infra/hadoop/compose.yml) + [infra/hive](infra/hive/compose.yml)) those land as Hive tables over HDFS. `sbt "runMain rat.Query"` reads them back. Runbook: [docs/ops.md](docs/ops.md).
