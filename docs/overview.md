# What this is

rat is a group-built event pipeline for personal use. Several of our own sources dump into one bus, get correlated while the events are still new, and stay queryable after the stream has moved on.

It is not a product and not a SIEM. If a source is useful, it gets a producer. If it isn't, it does not.

## Why bother

Machines, services, and feeds already emit events. They do not share a format, they do not share a clock anyone fully trusts, and looking at each stream alone is how the thing that only shows up when two of them agree gets missed.

Batch jobs are the wrong shape for that. By the time a nightly job runs, the moment is gone. Minutes of delay, not hours.

## Aims

1. **One bus, many sources.** Producers are thin. Kafka is the contract. A new source is a new producer and a topic, not a new pipeline.
2. **A small shared event.** `source`, `entity_id`, `ts_ms`, `payload`. Correlation keys live on the event, not buried in whoever-sent-this JSON.
3. **Join on entity and time.** Spark Structured Streaming windows and stream-stream joins, not a folder of cron scripts.
4. **Land somewhere we can query.** Parquet on HDFS, Hive external tables on that path. Freshness first. Depth is whatever Hive retains.
5. **Keep the producers boring.** Python for ingestion. Scala 2.13 for the Spark job, because that is the API Spark 3.5 actually ships.

## What "done" looks like

A source can appear, emit, and show up in a Hive table without a one-off path for it. Two sources that share an `entity_id` in the same window become one correlated row. "What happened around this host in the last N minutes" is a Hive query, not grepping four log files.

## What it is not

Not multi-tenant. Not trying to replace Splunk or an alerting SaaS. Not a historical warehouse. Kafka and Spark are here because freshness matters more than depth for this. Years of archive later is a different job on the same HDFS files.
