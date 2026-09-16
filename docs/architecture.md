# Architecture

![plugins to Kafka to Spark to parquet/HDFS to Hive](rat-pipe.jpg)

Source: [rat-pipe.tldraw](rat-pipe.tldraw). Kafka buffers. Spark correlates. Hive is read-only SQL.

| Piece | Lang |
|---|---|
| Plugins / producers | Python |
| Job | Scala 2.13, Spark 3.5.3 |
| Queries | HiveQL |

No PySpark in `spark/`. Producers do not touch Hive. uv for Python, sbt for Scala. [setup.md](setup.md).

## Envelope

```
event_id    stable id (hn:123)
source      plugin name
entities    join keys, 0..n
ts_ms       event time
payload     plugin JSON
```

No entity → land raw, skip the join. Sources are plugins: [plugins.md](plugins.md).

## Job

`rat.Correlate` reads topics, parses the envelope, watermarks `ts_ms`, joins on entity + window, writes parquet. Hive mounts those paths. `sbt run` is `local[*]` until a cluster exists.

Config is env, not code (`rat.RatConfig`, defaults in parentheses):

| Var | Meaning | Default |
|---|---|---|
| `RAT_BOOTSTRAP` | Kafka servers | `localhost:9092` |
| `RAT_TOPIC_PATTERN` | topics to read, full-match regex | `events\.(?!.*\.dlq$).*` |
| `RAT_SINK_DIR` | Parquet root | `/tmp/rat` |
| `RAT_CHECKPOINT_DIR` | checkpoint root | `$RAT_SINK_DIR/checkpoints` |
| `RAT_WATERMARK` | watermark + join window | `10 minutes` |
| `RAT_STARTING_OFFSETS` | first run offset | `latest` |
| `RAT_TRIGGER` | micro-batch interval, empty = continuous | unset |
| `SPARK_MASTER` | master URL | `local[*]` |

The topic pattern means a new plugin's `events.<source>` topic is picked up with no rebuild; DLQ topics are never read. Paths get `file://` unless the dir already carries a scheme — the cluster swap-in is `RAT_SINK_DIR=hdfs://…`, not new code.

## Hive

Hive is read-only SQL over the Parquet the job wrote — no connector, no second writer. `rat.Hive` creates two EXTERNAL tables and repairs partitions:

| Table | Rows | Partitions | Location |
|---|---|---|---|
| `rat_events` | raw envelopes (no entities) | `dt`, `source` | `$RAT_HIVE_BASE/events_raw` |
| `correlated` | join pairs | `dt` | `$RAT_HIVE_BASE/correlated` |

`RAT_HIVE_ENABLED=true` turns the layer on: the job enables Hive support, bootstraps the tables, and repairs partitions after micro-batches — throttled to one `MSCK REPAIR TABLE` per `RAT_HIVE_MSCK_MIN_SECONDS` (30s default), so fresh partitions never wait for a human and the metastore is not hammered per batch. `rat.Query` (`sbt "runMain rat.Query"`) queries the same tables; without Hive enabled it mounts the Parquet paths directly.

The metastore is a separate process, not a lock inside the job: [infra/hive/compose.yml](../infra/hive/compose.yml) (Hive 3.1.3 — metastore thrift `localhost:9083` + HiveServer2 `localhost:10000` for beeline, on host network so table locations resolve the same for Spark and Hive). Spark 3.5's Hive 2.3.9 client is the compatibility ceiling: metastore 3.1.3, not 4.x. DDL is mirrored in [hive/rat_events.sql](../hive/rat_events.sql) — keep it in sync with `rat.Hive`.

Storage follows the same pattern: [infra/hadoop/compose.yml](../infra/hadoop/compose.yml) is a single-node HDFS (NameNode :8020 + DataNode), and the prod units point the sinks at `hdfs://localhost:8020/rat`. Local paths (`file://`) stay the dev/smoke profile — paths that carry a scheme are used as-is, so a real cluster is a hostname change.

## Infra

Kafka is in-tree: [infra/kafka/compose.yml](../infra/kafka/compose.yml) (single-node KRaft, `localhost:9092`). Producers run live against it. Runbook: [ops.md](ops.md). Code map: [CONTRIBUTING.md](../CONTRIBUTING.md).