# What this is

Group pipeline for our own sources. Kafka in, Spark joins while events are still new, Hive after.

Not a product. Not a SIEM. Useful source → plugin. Useless source → skip.

Batch is too slow. Minutes of delay, not hours.

## Aims

1. One bus. New source = plugin + topic, not a new pipe.
2. Envelope: `source`, `entities`, `ts_ms`, `payload`. Join keys on the envelope.
3. Spark joins on entity + time window.
4. Parquet on HDFS, Hive on that path. Fresh first.
5. Python ingest. Scala 2.13 job (Spark 3.5).

Done: a source shows up in Hive without a one-off path. Two sources sharing an entity in one window become one row.

Not multi-tenant. Not Splunk. Not a warehouse. Archive later is a different job on the same files.
