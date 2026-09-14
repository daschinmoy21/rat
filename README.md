# rat

Group project. Kafka → Spark Structured Streaming → Hive/HDFS.

Several sources write into Kafka. A Scala job joins events that share an entity in a short window. Parquet lands on HDFS. Hive is how we query it later.

Python producers, Scala 2.13 Spark job (Spark 3.5.3). The engine is 2.13 because that is what Spark ships. Do not "upgrade" the job to Scala 3.

This is still a scaffold. Shared `Event` type, toolchain, docs. No broker, no live job yet.

## Where to look

| I want to… | Open |
|---|---|
| Understand the point | [docs/overview.md](docs/overview.md) |
| See how pieces connect | [docs/architecture.md](docs/architecture.md) |
| Get a machine working | [docs/setup.md](docs/setup.md) |
| Change code without making a mess | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Add or change a source | plugin under `plugins/` (see [docs/plugins.md](docs/plugins.md)). Today still `producers/` until the loader exists |
| Change the shared envelope | `producers/rat_producers/events.py` **and** `spark/src/main/scala/rat/Event.scala` |
| Payload shape for one source | that plugin's `schema.json` |
| Agent / VPS tool surface | [docs/agents.md](docs/agents.md) |
| Correlation, windows, sink | `spark/` |
| Python deps | `uv add` / `uv remove` at repo root |
| Scala deps | `spark/build.sbt` |
| Tool versions (JDK, Python, sbt, uv) | `flake.nix` and `mise.toml` together |

## Layout

```
producers/     Python core helpers + envelope. Plugins will own sources.
spark/         Scala sbt job. Structured Streaming only lives here.
docs/          Why, architecture, setup, plugins, agents.
scripts/       check-deps.sh
flake.nix      Nix shell (JDK 17, sbt, uv, Metals).
mise.toml      Same versions without Nix.
pyproject.toml uv workspace. Python deps, not Nix packages.
CONTRIBUTING.md
```

## First clone

```bash
git clone git@github.com:daschinmoy21/rat.git
cd rat
```

Nix: `direnv allow` or `nix develop`.

Not Nix: JDK 17, sbt, uv, then `uv sync --frozen`. Full notes in [docs/setup.md](docs/setup.md).

```bash
./scripts/check-deps.sh
cd spark && sbt compile
```

## Docs

- [docs/overview.md](docs/overview.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/setup.md](docs/setup.md)
- [docs/plugins.md](docs/plugins.md)
- [docs/agents.md](docs/agents.md)
- [CONTRIBUTING.md](CONTRIBUTING.md)
