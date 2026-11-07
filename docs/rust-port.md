# Rust port: design, guarantees, limitations

`rat-rs` (crate `rust/rat-rs`) replaces three legacy pieces: the Spark job
(`spark/src/main/scala/rat/Correlate.scala`), HDFS as the sink filesystem, and
Hive plus `rat.Query` as the read side. The JVM sources stay in place for
comparison. Commands and env vars: [rust-interface.md](rust-interface.md).
The broker contract: [iggy-interface.md](iggy-interface.md).

## What maps to what

| Legacy | Rust |
|---|---|
| Kafka `subscribePattern` source | `rat-rs consume`: Iggy HTTP polling by explicit offset (reqwest) |
| finite test input (none) | `rat-rs process --input file.jsonl` (same engine, same commit path) |
| `from_json` + `filter(event_id, ts_ms not null)` | `envelope.rs` (PERMISSIVE semantics, see below) |
| `withWatermark` + stream-stream self-join | `engine.rs` (per-record watermark, bounded state) |
| Parquet file sink + `_spark_metadata` + checkpoint offsets | staged files + atomic `checkpoint.json` commit (`checkpoint.rs`) |
| HDFS (`hdfs://localhost:8020/rat`) | local filesystem, same Hive-style partition layout |
| Hive metastore + `MSCK REPAIR` + HiveServer2 | DataFusion `ListingTable`s, listed again on every query (`rat-rs sql`) |
| `rat.Query` | `rat-rs query` (same two queries) |

## Correlation semantics (same as legacy)

* An envelope with no entities (empty array) goes to the raw sink
  `events_raw/dt=/source=`.
* An envelope with entities is exploded per **distinct** entity
  (`array_distinct`). Null entities, entities blank after trimming spaces, and
  null or blank sources never pair.
* Two records pair when they share an entity, their sources differ, and
  `|ts_a - ts_b| <= RAT_WATERMARK`, with **both bounds inclusive**. `a_id` is
  the record whose source sorts lower, byte-wise as Spark's `UTF8String` does,
  so each unordered source pair appears once with no mirrored row. The pair's
  `ts_ms` is the later of the two times, and `dt` is derived from it.
* `dt` is `to_date(ts_ms)` in the session time zone. Spark's default zone is
  the JVM/host zone, so the default here is `RAT_DT_TIMEZONE=local`. Pass
  `utc` (and run Spark with `TZ=UTC`) to compare across machines.
* Partition values are escaped like Spark's `escapePathName`. A null or empty
  source lands in `source=__HIVE_DEFAULT_PARTITION__`.
* Parquet schema: strings `BYTE_ARRAY/UTF8`, `ts_ms INT64`, `entities` as a
  3-level `LIST` with element name `element`, Snappy compression. Partition
  columns live only in directory names. The existing Hive DDL
  (`hive/rat_events.sql`) mounts these directories unchanged.

### Streaming semantics: deliberate, documented differences

Spark advances the watermark once per micro-batch, so which out-of-order
records count as late depends on batch timing. `rat-rs` advances it **per
record**:

* `watermark = max(ts_ms of earlier join-eligible records) - RAT_WATERMARK`.
* A join-eligible record with `ts_ms < watermark` is late. It is counted in
  `late_dropped` and does not join. Raw rows are never dropped, because the
  legacy raw sink has no watermark either. A record exactly at the watermark
  is accepted, so the inclusive boundary holds for out-of-order input too.
* State older than `watermark - window` is evicted. Such entries can never pair
  with a non-late record, so eviction does not affect output.

The result is a **pure function of input order**: batch size, restart points
and crashes do not change the output. Tests check this. When no record is
more than `RAT_WATERMARK` behind the running maximum, the output equals the
legacy batch join (`Correlate.pairs`, what `CorrelateSpec` tests), and it
equals what Spark produces in any batching. `RAT_LATE_POLICY=keep` turns off
late-dropping and eviction, giving the exact batch join for any input at the
cost of unbounded state.

Across several Iggy partitions or topics, "input order" means the order the
consumer read them: topics by name, partitions by id, in polls of up to
`RAT_BATCH_MAX`. That order is not global event-time order, which is equally
true of Spark reading Kafka partitions.

### Input parsing (Spark `from_json` PERMISSIVE parity)

* Malformed JSON, invalid UTF-8, or a non-object top level: rejected
  (`malformed` / `not_object`).
* `event_id` missing or null: rejected. `ts_ms` missing, null, non-integer, or
  a string: rejected (Spark's `LongType` converter does not parse strings).
* A non-string scalar in a string field (for example `event_id: 7`) becomes its
  JSON text. An object or array `payload` becomes compact JSON text with key
  order preserved, like Jackson's `copyCurrentStructure`.
* A field of the wrong type (for example `entities: "AAPL"`) becomes null while
  the rest of the record survives (Spark's partial results).
* Blank lines in JSONL are skipped and not counted. An unterminated last line
  is read at EOF in `process`, but waited for in `process --follow`.

Rejected records are counted by reason (`rejected_by_reason`, and cumulatively
in the checkpoint). The legacy job dropped them without a trace. Producers
already route schema-invalid rows to DLQ topics, and those are never read.

### Known legacy divergence: null `entities`

The legacy raw filter is `coalesce(size(entities), 0) === 0`, and its comment
says a missing array counts as entity-less. But Spark 3.5 defaults to
`spark.sql.legacy.sizeOfNull=true` with ANSI off, so `size(NULL)` is `-1`, not
NULL, and the coalesce never fires: **legacy drops envelopes with a missing or
null `entities` from both sinks.** `rat-rs` follows the documented intent and
writes them to the raw sink. This comes from reading Spark 3.5 SQLConf; it was
not executed here. Producer envelopes always carry an array, and the parity
fixture has none of these records.

## Delivery guarantees

* **Exactly-once visible output per checkpoint**, for both `process` and
  `consume`. Each micro-batch writes hidden `.part-*.tmp` files, fsyncs them,
  then atomically replaces `checkpoint.json` (input position, engine state,
  list of staged files, then fsync of the file and the directory). That rename
  is the commit. Afterwards the staged files get their final `part-*.parquet`
  names.
  * Crash before the commit: the tmp files are deleted on restart, and the
    batch is re-read from the committed position. The engine is
    deterministic, so it produces the same rows.
  * Crash after the commit: the remaining renames are rolled forward on
    restart.
  * Readers (DataFusion, Hive, DuckDB) only see committed `part-*.parquet`
    files, because staged files start with `.` and end in `.tmp`.
* **Restart refusals instead of silent duplication.** `rat-rs` refuses to
  start in each of these cases:
  * the checkpoint was written with a different watermark, late policy, dt
    zone, or input;
  * the sink holds data but there is no checkpoint;
  * the input file is shorter than the committed offset;
  * another process holds the checkpoint lock (`flock` on `LOCK`).
* Iggy offsets are kept only in the rat-rs checkpoint (`auto_commit=false`,
  no server-side consumer offsets), so they always commit together with the
  output. `RAT_STARTING_OFFSETS` applies only to partitions that exist on the
  first run, and the resolved offsets are checkpointed immediately. Partitions
  and topics that appear later start from offset 0, as Spark's Kafka source
  does for new partitions.
* Producers are at-least-once. A duplicate envelope (same `event_id`, sent
  twice) is processed twice, as in the legacy job. There is no `event_id`
  dedup.
* An offset gap in Iggy (messages expired by retention before they were read)
  is skipped and counted in `iggy.offset_gaps`, as Kafka with
  `failOnDataLoss=false` would. Legacy Spark defaults to
  `failOnDataLoss=true`, which stops the query instead.

## What replacing HDFS + Hive gives up

| Lost | Why it mattered | Rust-side state |
|---|---|---|
| Block replication across DataNodes | survives a disk or node loss | single local disk: use RAID, ZFS, or backups. The in-tree HDFS had `dfs.replication=1`, so this never actually existed locally |
| Distributed, multi-writer namespace | several jobs and hosts writing one tree | one writer per checkpoint (locked), one host. Readers on other hosts need NFS, rsync, or object storage |
| Scale-out compute (Spark executors) | throughput beyond one machine | one process, one core for the engine. State must fit in RAM (bounded by the watermark window) |
| Hive metastore (schemas, partitions, ACLs, JDBC/Thrift for BI tools) | SQL clients like beeline, JDBC | `rat-rs sql` CLI only, with no server, no auth layer, and no JDBC. The layout stays Hive-compatible, so a metastore can still mount it |
| Spark's checkpoint format and tooling | offset inspection with standard tools | `checkpoint.json` is plain JSON |
| Small-file compaction (none in legacy either) | — | one file per partition per batch. Raise `RAT_BATCH_MAX` to cut file count. There is no compaction command yet |

Durability on a single host: each batch is fsynced (files, checkpoint, and
directories) before it counts as committed. A power loss can lose at most the
batch in flight, and that batch is re-read on restart.

## Testing

```bash
export PATH="$HOME/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin:$PATH"
cargo test -p rat-rs                         # everything, including SQL (DataFusion)
cargo test -p rat-rs --no-default-features   # without DataFusion: much faster to build
```

* `tests/correlate.rs`: a port of every `CorrelateSpec` case. Each case runs
  under both late policies, which must agree. The file also covers source
  order not depending on arrival order, byte-wise source order, out-of-order
  input within the watermark, the inclusive boundary reached out of order,
  late drop versus keep, and bounded state with unchanged output.
* `tests/pipeline.rs`:
  * output identical across batch sizes 7 and 100k and across a mid-file restart;
  * a re-run is a no-op;
  * crash before the commit (orphaned staged files) and crash after the commit
    (rename roll-forward);
  * the restart refusals: changed settings, sink without checkpoint, truncated
    input, second writer;
  * invalid-input accounting by reason, invalid UTF-8, unterminated last line;
  * hostile partition values (`../../etc`, empty, and null sources);
  * SQL counts and partition columns, with stray tmp files invisible;
  * against an in-process Iggy HTTP mock: all partitions read, DLQ and
    non-matching topics skipped, re-login after a 401, u128 message ids,
    resume from the checkpoint, `latest` applied only on the first run, and
    a new topic read from earliest.
* Unit tests cover envelope parsing, interval parsing, path escaping, and `dt`.

Not covered by these unit tests: a real Iggy server (`scripts/smoke-rust.sh`
covers that separately), a real power loss (fsync ordering is
designed but not fault-injected), and Spark's own late-row behaviour, which
depends on batch timing.
