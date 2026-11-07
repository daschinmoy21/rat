# Stack comparison

All scripts are read-only toward existing datasets unless their command
explicitly launches a new workload. Use isolated sinks, checkpoints, topics
and streams. No script samples whole-host resource deltas.

`sample.py` reads cgroup v2 counters for named Podman containers and transient
user systemd scopes. Its memory measure is `memory.current - inactive_file`;
anonymous memory and swap are separate. CPU is the change in `usage_usec`
over elapsed time, with one logical CPU = 100%. Filesystem block-I/O counters
exclude cache-only I/O and are not a count of application bytes.

`fixture.py` generates 10,000 envelopes (2,500 groups by default), each group
containing three sources with one shared entity and one raw-only record.
Each shared entity is duplicated within its envelope, testing distinct
entity handling. All timestamps are within 90 seconds, so late-data policy
and broker partition order cannot alter the expected 7,500 pairs / 2,500 raw
rows. This is a correctness and modest-load workload, not a stress test.

```bash
python3 tools/stack-bench/fixture.py --out /tmp/rat-bench-input.jsonl

# Compile the unmodified JVM code and export the runtime classpath:
(cd spark && sbt -batch compile 'export Runtime/fullClasspath') > /tmp/rat-classpath.log
# Launch the legacy job in an isolated cgroup, with the legacy broker/HDFS/
# Hive running and an empty, dedicated HDFS sink:
systemd-run --user --scope --unit=rat-bench-spark env \
  RAT_HIVE_ENABLED=true RAT_HIVE_METASTORE_URI=thrift://localhost:9083 \
  RAT_SINK_DIR=hdfs://localhost:8020/rat-bench \
  RAT_STARTING_OFFSETS=earliest 'RAT_TRIGGER=2 seconds' \
  python3 tools/stack-bench/legacy.py --classpath-file /tmp/rat-classpath.log

python3 tools/stack-bench/sample.py --seconds 90 \
  --container rat-kafka --container rat-namenode --container rat-datanode \
  --container rat-hive-metastore --container rat-hiveserver2 \
  --unit rat-bench-spark.scope --out /tmp/rat-legacy-loaded.json
# In another terminal, as soon as sampling starts:
RAT_BROKER=kafka uv run python tools/stack-bench/produce.py /tmp/rat-bench-input.jsonl
```

`legacy.py` uses exactly the Java options in `spark/build.sbt`, but launches
the compiled job directly rather than leaving sbt's additional JVM resident.
This makes the memory baseline conservative relative to the shipped legacy
systemd unit. The only pipeline tuning used here is a 2-second trigger on
both implementations, so the comparison need not wait 30 seconds per batch.
Spark's default 200 shuffle partitions are unchanged.

Stop the test legacy scope and containers before measuring Rust. With Iggy
running, topics provisioned, an empty `rat` stream and a fresh local sink.
Use a disk-backed path for the sink and checkpoint: `/tmp` is tmpfs on some
hosts, including the measured VM, and would invalidate storage comparisons.

```bash
systemd-run --user --scope --unit=rat-bench-rust env \
  RAT_SINK_DIR="$HOME/.local/state/rat/bench/sink" RAT_STARTING_OFFSETS=earliest \
  'RAT_TRIGGER=2 seconds' target/release/rat-rs consume
python3 tools/stack-bench/sample.py --seconds 90 --container rat-iggy \
  --unit rat-bench-rust.scope --out /tmp/rat-rust-loaded.json
# In another terminal, as soon as sampling starts:
RAT_BROKER=iggy uv run python tools/stack-bench/produce.py /tmp/rat-bench-input.jsonl
```

Sample settled idle services for 30 seconds after both sinks have caught up.
The Python producer is outside these service cgroups on both paths; its
acknowledged-replay timing is recorded separately. The plugin benchmark
measures its process footprint. DataFusion is a CLI read side with no idle
SQL-server process; HiveServer2 is a resident legacy service.

For an independent row-level comparison, export the two legacy Parquet
directories from HDFS into one local root, then:

```bash
uv run python tools/stack-bench/verify.py /tmp/rat-legacy-export "$HOME/.local/state/rat/bench/sink" \
  --out /tmp/rat-parity.json
```

The verifier uses the optional DuckDB Python package already used by the
dashboard. It compares sorted canonical rows, including multiplicity, while
normalizing JSON object key order and date partition representations. It
does not use the Rust engine's own digest implementation.

`run.py` records a finite command's CPU time and sampled peak RSS, e.g. for a
file replay or SQL query. A file replay bypasses both brokers; do not compare
its throughput directly with a live Kafka/Spark run.
