package rat

import org.scalatest.funsuite.AnyFunSuite

class HiveSpec extends AnyFunSuite {

  test("ddl creates both external tables with the sink locations") {
    val sql = Hive.ddl(RatConfig.fromEnv(Map.empty)).mkString("\n")
    assert(sql.contains("CREATE EXTERNAL TABLE IF NOT EXISTS rat_events"))
    assert(sql.contains("CREATE EXTERNAL TABLE IF NOT EXISTS correlated"))
    assert(sql.contains("event_id STRING"))
    assert(sql.contains("entities ARRAY<STRING>"))
    assert(sql.contains("ts_ms BIGINT"))
    assert(sql.contains("PARTITIONED BY (dt STRING, source STRING)"))
    assert(sql.contains("PARTITIONED BY (dt STRING)"))
    assert(sql.contains("STORED AS PARQUET"))
    assert(sql.contains("LOCATION '/tmp/rat/events_raw'"))
    assert(sql.contains("LOCATION '/tmp/rat/correlated'"))
  }

  test("ddl escapes single quotes in the configured base") {
    val sql =
      Hive.ddl(RatConfig.fromEnv(Map("RAT_HIVE_BASE" -> "hdfs://nn/it's/rat"))).mkString("\n")
    assert(sql.contains("LOCATION 'hdfs://nn/it''s/rat/events_raw'"))
    assert(sql.contains("LOCATION 'hdfs://nn/it''s/rat/correlated'"))
  }
}
