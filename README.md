# rat

Kafka → Spark Structured Streaming → Hive/HDFS.

Python producers. Scala 2.13 Spark job (3.5.3). Sources are plugins. Scaffold: envelope + toolchain. No broker yet.

[overview](docs/overview.md) · [architecture](docs/architecture.md) · [plugins](docs/plugins.md) · [setup](docs/setup.md) · [contributing](CONTRIBUTING.md)

```bash
git clone git@github.com:daschinmoy21/rat.git
cd rat
direnv allow          # or: nix develop
# no Nix: JDK 17, sbt, uv → uv sync --frozen
./scripts/check-deps.sh
cd spark && sbt compile
```
