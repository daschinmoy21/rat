# rat

Kafka → Spark Structured Streaming → Hive/HDFS.

Python producers. Scala 2.13 Spark job (3.5.3). Sources are plugins. Scaffold: envelope + toolchain. No broker yet.

| I want… | Open |
|---|---|
| Why | [docs/overview.md](docs/overview.md) |
| Pipe | [docs/architecture.md](docs/architecture.md) |
| Install | [docs/setup.md](docs/setup.md) |
| Add a source | [docs/plugins.md](docs/plugins.md), then `producers/` until the loader exists |
| Envelope | `producers/rat_producers/events.py` and `spark/src/main/scala/rat/Event.scala` |
| Payload schema | plugin `schema.json` |
| Job | `spark/` |
| Python deps | `uv add` |
| Scala deps | `spark/build.sbt` |
| Tool pins | `flake.nix` and `mise.toml` |
| How to change code | [CONTRIBUTING.md](CONTRIBUTING.md) |

```
producers/     envelope + helpers. Plugins will own sources.
spark/         Structured Streaming job.
docs/
flake.nix      Nix: JDK 17, sbt, uv, Metals.
mise.toml      same pins without Nix.
pyproject.toml uv workspace.
```

```bash
git clone git@github.com:daschinmoy21/rat.git
cd rat
direnv allow          # or: nix develop
# no Nix: JDK 17, sbt, uv → uv sync --frozen
./scripts/check-deps.sh
cd spark && sbt compile
```

Non-Nix install: [docs/setup.md](docs/setup.md).
