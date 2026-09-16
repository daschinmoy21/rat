package rat

import org.apache.spark.sql.SparkSession

/** The read side: latest raw envelopes + latest correlated pairs. With RAT_HIVE_ENABLED=true the
  * query runs against the Hive tables (metastore via RAT_HIVE_METASTORE_URI); otherwise the Parquet
  * paths are mounted directly.
  */
object Query {
  def main(args: Array[String]): Unit = {
    val cfg = RatConfig.fromEnv(sys.env)

    val spark = cfg
      .configure(
        SparkSession
          .builder()
          .appName("rat-query")
          .master(cfg.master)
      )
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if (cfg.hiveEnabled) {
      Hive.bootstrap(spark, cfg)
      println("== rat_events (raw) ==")
      spark
        .sql("SELECT event_id, source, ts_ms, dt FROM rat_events ORDER BY ts_ms DESC LIMIT 10")
        .show(200, false)
      println("== correlated ==")
      spark
        .sql("SELECT entity, a_id, b_id, ts_ms, dt FROM correlated ORDER BY ts_ms DESC LIMIT 10")
        .show(200, false)
    } else {
      val raw = spark.read.parquet(cfg.rawPath)
      raw.createOrReplaceTempView("rat_events")
      println("== rat_events (raw, direct parquet) ==")
      raw.orderBy(raw("ts_ms").desc).show(10, false)

      val corr = spark.read.parquet(cfg.correlatedPath)
      corr.createOrReplaceTempView("correlated")
      println("== correlated (direct parquet) ==")
      corr.orderBy(corr("ts_ms").desc).show(10, false)
    }

    spark.stop()
  }
}
