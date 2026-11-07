# Rust stack

The new path is Python source plugins → Apache Iggy 0.9.0 → `rat-rs` →
partitioned Parquet. `rat-rs query` and `rat-rs sql` use Apache DataFusion.
Kafka, Spark, HDFS, the Hive metastore and HiveServer2 are absent from this
path. The original code remains available for comparisons and cluster users.

## Run locally

You need Rust 1.97+, Python 3.12+, uv, and Podman with podman-compose (or Docker
Compose). `nix develop` supplies the Rust/Python/container tools. The legacy
JVM tools are in `nix develop .#legacy`.

```bash
uv sync --frozen
CARGO_BUILD_JOBS=2 cargo build --locked --release -p rat-rs
./scripts/iggy-up.sh

# Export these in each terminal. The CLI defaults to /tmp/rat, which is
# convenient for experiments but unsuitable for durable deployment.
export RAT_BROKER=iggy
export RAT_SINK_DIR="$HOME/.local/share/rat/sink"

# Terminal 1. Start before the producers: new consumers default to latest.
target/release/rat-rs consume
# Terminal 2
uv run rat run
# Terminal 3
target/release/rat-rs query
target/release/rat-rs sql 'SELECT entity, count(*) AS n FROM correlated GROUP BY entity ORDER BY n DESC LIMIT 20'
uv run rat status
uv run rat dash
```

Use `RAT_STARTING_OFFSETS=earliest` on the first consumer start to replay
retained data. Once a checkpoint exists, its offsets take precedence. Stop
the consumer with SIGINT or SIGTERM; it completes its current batch before
exiting. `./scripts/iggy-up.sh --down` stops this project's broker and keeps
its volume.

The HTTP client and broker wire format are documented in
[iggy-interface.md](iggy-interface.md). Iggy is not Kafka protocol compatible;
pointing a Kafka client at its ports does not work. The Python CLI retains
Kafka as its compatibility default, so explicitly set `RAT_BROKER=iggy`.
Topic provisioning explicitly requires persisted durability. Older Iggy
topics created with the default `replicated` policy are refused; choose a
new `RAT_IGGY_STREAM` rather than deleting data to get past that check.

## JVM component replacements

| Original component | Rust path | Scope |
| --- | --- | --- |
| Kafka KRaft broker | Apache Iggy 0.9.0 | Persistent local message broker; HTTP transport, explicit offsets |
| Spark Structured Streaming | `rat-rs consume` | Single-process incremental event-time join with persisted state |
| HDFS NameNode + DataNode | Local partitioned Parquet + filesystem durability | One machine; no distributed filesystem protocol or replication |
| Hive metastore + HiveServer2 | DataFusion in `rat-rs query` / `rat-rs sql` | Reads Parquet partitions directly; no Thrift/JDBC server or shared Hive catalog |
| Python plugins | Existing Python process | Preserves adapters, JSON schemas, manifests and entity extraction |

Correlations retain source ordering (`a_id` belongs to the lexicographically
smaller source), inclusive ±window bounds, distinct entities per envelope,
and the later event timestamp. Same-source pairs are excluded. Only
entity-less events enter `events_raw`; events with entities are represented
by correlations rather than copied into that raw table. Payloads stay JSON
text in Parquet, as on the legacy path.

The watermark is a documented behavior change: the Rust implementation
advances it per join-eligible record, whereas Spark advances watermarks per
micro-batch. Very late input can therefore differ at a batch boundary. The
Rust policy is deterministic across batch sizes and restarts. For a finite
historical batch join use `--late-policy keep`, which disables late dropping
and eviction and uses unbounded join state. See
[rust-interface.md](rust-interface.md) for parsing and configuration details.

## Persistence and recovery

Parquet is partitioned as `correlated/dt=YYYY-MM-DD/` and
`events_raw/dt=YYYY-MM-DD/source=.../`. The consumer keeps its state and next
broker offsets in a local checkpoint. Staged Parquet files are fsynced,
then the checkpoint is atomically replaced and the files are published by
rename. Restart rolls forward committed staged files and removes files
from uncommitted batches. This provides replay protection at the consumer
offset boundary on the local filesystem; repeated producer sends remain
repeated events, matching the legacy producer semantics.

This is a single-writer system. Keep sink and checkpoint together in backups.
The broker also needs persistent topics: HTTP 201 with Iggy's default
replicated policy is insufficient to survive a process crash before flush.
The producer checks the persisted durability attestation before treating a
send as acknowledged.
Never delete a checkpoint and resume into an existing populated sink. Do
not share a sink between engines or reuse checkpoints with another broker,
input or topic filter. Do not alter a replay file while resuming it. There
is no migration of Spark state-store or Kafka offsets: start the Rust stack
with a fresh sink/checkpoint and explicitly choose where ingestion begins.
Retained Kafka messages need a deliberate export/replay into Iggy; changing
an environment variable does not migrate them.

Direct Parquet readers can see a committed batch's files become visible one
at a time. Recovery prevents duplicate files on restart; it is not a
multi-table snapshot transaction for concurrent queries. A single busy
entity can still generate quadratic pair output within its time window.

The existing producer's local spool is best-effort, including on disk-full
errors. A cursor advance is not an end-to-end exactly-once guarantee. The
Lua assessment discusses why retaining the Python adapters is currently the
preferred compatibility choice: [python-vs-lua.md](python-vs-lua.md).

## Services

After building the release binary:

```bash
./deploy/install.sh --broker iggy --start
systemctl --user status rat-iggy rat-producers rat-engine
journalctl --user -u rat-engine -f
```

The installer defaults new configurations to Iggy, while preserving an
existing `RAT_BROKER` setting unless `--broker` overrides it. Installing the
engine unit selects `earliest` on its first checkpoint so a simultaneous
producer start does not skip input; existing checkpoint offsets still win.
The Rust units does not stop or disable an existing legacy stack. Stop those
services explicitly when you are ready to switch; running both producer
services against the same cursor database is unsupported.

For a legacy environment use `./deploy/install.sh --broker kafka` and the
[legacy deployment guide](../deploy/README.md). HDFS and Hive clients cannot
connect to the Rust stack as a protocol-compatible replacement. For
distributed storage, cluster scheduling, Kerberos, or existing JDBC clients,
keep the legacy path until an appropriate replacement has been designed.

## Verification and measurements

```bash
cargo test --locked --workspace
cargo clippy --locked --all-targets -- -D warnings
uv run ruff check producers/
uv run pytest producers/tests/ -q
./scripts/smoke-rust.sh
```

See [resource-comparison.md](resource-comparison.md) for the measurement
method, workload, raw samples, verified row counts and limitations.
