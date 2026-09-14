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
| Source | plugin, see [docs/plugins.md](docs/plugins.md). Until the loader exists: `producers/rat_producers/` |
| Envelope | `events.py` and `Event.scala` in the same PR |
| Job | `spark/src/main/scala/rat/` |
| Python lib | `uv add`, commit `uv.lock` |
| Spark lib | `spark/build.sbt` (3.5.3 / 2.13.14) |
| Tool pins | `flake.nix` and `mise.toml` together |
| Docs | `docs/` |

RSS **feed** = config. New **kind** of source (HN, stocks, IMAP) = plugin.

## Envelope

Python: `event_id`, `ts_ms`. Scala: `eventId`, `tsMs`. Same fields. Payload stays plugin JSON.

## Git

Do not commit `.venv/`, `target/`, `.env`, secrets. Branch off `main`, small PR, say what you ran (`uv sync`, `sbt compile`).

Scaffold. `Correlate` starts Spark and stops. No Kafka in-tree yet.
