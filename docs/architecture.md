# Architecture

```
personal sources
      │
      ▼
Python producers  ──►  Kafka topics (one topic per source)
                              │
                              ▼
                   Scala Spark Structured Streaming
                   normalize → window → join → score
                              │
                              ▼
                   parquet on HDFS  ──►  Hive tables
```

Kafka decouples producers from the job. Spark is the only place that knows how sources relate. Hive is read-path SQL, not the ingestion path.

## Language split

| Piece | Language | Why |
|---|---|---|
| Producers | Python | JSON, APIs, messy source SDKs. uv project under `producers/`. |
| Correlation job | Scala | Spark's native Structured Streaming API. sbt project under `spark/`. |
| Queries | HiveQL | Tables on the parquet the job wrote. |

Do not mix PySpark and Scala in the same Spark app. Producers never talk to Hive.

Python deps go through uv (`pyproject.toml`, `uv.lock`). Scala deps go through `spark/build.sbt`. The toolchain is JDK 17, sbt 1.10.11, Python 3.12, uv. Nix (`flake.nix`) or mise (`mise.toml`) both pin those. Neither is the Kafka/Hive cluster. See [setup.md](setup.md).

## Event schema

Every producer emits the same shape. Python and Scala both have it.

```
event_id    stable id for this event
source      which producer (host, svc, feed, …)
entity_id   what we join on (host, user, ip, …)
ts_ms       event time, milliseconds
payload     source-specific fields
```

`entity_id` plus event time is the correlation key. If a source cannot name an entity, it does not belong on the correlated path. It can still land as raw, but it will not join.

## Spark job

`rat.Correlate` is the Structured Streaming app. It will:

1. Read each source topic.
2. Parse to `Event`.
3. Watermark on `ts_ms` so late events do not grow state forever.
4. Join sources on `entity_id` inside a bounded window.
5. Write parquet to HDFS, partitioned by date (and source if that stays useful).

Hive then mounts those paths as external tables. The job does not INSERT through HiveServer.

Until a cluster exists, `sbt run` uses `local[*]` (`SPARK_MASTER` can override).

## Status

Scaffold only. Flake, uv workspace, sbt project, shared `Event` type. No Kafka broker, no compose stack, no live correlation yet. Getting Kafka to stay up is the next piece. Everything else waits on that.

Where to put a change: [CONTRIBUTING.md](../CONTRIBUTING.md).
