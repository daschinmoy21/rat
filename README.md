# rat

Python source plugins → Apache Iggy → Rust correlation engine → partitioned Parquet.
Query the output with Apache DataFusion, in the same Rust binary. The Rust
stack needs no JVM. Existing Python adapters and their manifests keep working.

[Rust migration and quickstart](docs/rust-migration.md) ·
[CLI reference](docs/rust-interface.md) · [Iggy protocol](docs/iggy-interface.md) ·
[resource measurements](docs/resource-comparison.md) ·
[Python versus Lua](docs/python-vs-lua.md) · [plugins](docs/plugins.md)

```bash
# Rust 1.97+, Python 3.12+, uv, Podman + podman-compose
uv sync --frozen
cargo build --locked --release -p rat-rs
./scripts/iggy-up.sh
export RAT_BROKER=iggy RAT_SINK_DIR="$HOME/.local/share/rat/sink"
target/release/rat-rs consume  # terminal 1
uv run rat run               # terminal 2, same exported env
target/release/rat-rs query   # terminal 3, same RAT_SINK_DIR
```

`uv run rat status`, `uv run rat dlq`, and `uv run rat dash` support Iggy when
`RAT_BROKER=iggy`. The dashboard's Spark-progress section is legacy-only.

The original Kafka/Spark/HDFS/Hive implementation remains under `spark/`,
`hive/`, and `infra/{kafka,hadoop,hive}/` for comparison and existing cluster
users. Its [getting started guide](GETSTARTED.md) and [deployment guide](deploy/README.md)
describe that legacy stack. Local Parquet replaces single-node HDFS; it does
not provide HDFS replication or HiveServer2 compatibility.
