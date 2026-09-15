-- Mirror of the DDL in spark/src/main/scala/rat/Hive.scala — keep the two in sync.
-- Both tables are EXTERNAL: DROP TABLE removes only the metastore entry;
-- Spark owns the files. Locations follow RAT_HIVE_BASE (default = RAT_SINK_DIR);
-- on a cluster swap '/tmp/rat' to the matching hdfs:// path on the same clause.

CREATE EXTERNAL TABLE IF NOT EXISTS rat_events (
  event_id STRING,
  entities ARRAY<STRING>,
  ts_ms BIGINT,
  payload STRING
)
PARTITIONED BY (dt STRING, source STRING)
STORED AS PARQUET
LOCATION '/tmp/rat/events_raw';

CREATE EXTERNAL TABLE IF NOT EXISTS correlated (
  entity STRING,
  a_id STRING,
  b_id STRING,
  ts_ms BIGINT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/tmp/rat/correlated';

-- Fresh partition directories are invisible until registered. The job runs
-- these after every micro-batch; run them here if you wrote outside the job.
MSCK REPAIR TABLE rat_events;
MSCK REPAIR TABLE correlated;
