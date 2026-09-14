# Architecture

```
plugins  →  Kafka (events.<source>)
                →  Spark Structured Streaming
                →  parquet / HDFS  →  Hive
```

Kafka buffers. Spark correlates. Hive is read-only SQL.

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

Scaffold. No Kafka in-tree. Next: broker. Code map: [CONTRIBUTING.md](../CONTRIBUTING.md).
