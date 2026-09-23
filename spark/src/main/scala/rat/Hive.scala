package rat

import org.apache.spark.sql.SparkSession

/** Hive read layer over the Parquet sinks: external tables + partition repair. DDL mirrors
  * hive/rat_events.sql — keep the two in sync.
  */
object Hive {

  def ddl(cfg: RatConfig): Seq[String] = {
    // Config values are quote-escaped so a ' in RAT_HIVE_BASE cannot break out
    // of the SQL string literal.
    val rawLoc = cfg.hiveRawLocation.replace("'", "''")
    val corrLoc = cfg.hiveCorrelatedLocation.replace("'", "''")
    Seq(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS rat_events (
         |  event_id STRING,
         |  entities ARRAY<STRING>,
         |  ts_ms BIGINT,
         |  payload STRING
         |)
         |PARTITIONED BY (dt STRING, source STRING)
         |STORED AS PARQUET
         |LOCATION '$rawLoc'""".stripMargin,
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS correlated (
         |  entity STRING,
         |  a_id STRING,
         |  b_id STRING,
         |  ts_ms BIGINT
         |)
         |PARTITIONED BY (dt STRING)
         |STORED AS PARQUET
         |LOCATION '$corrLoc'""".stripMargin
    )
  }

  /** Create tables if missing, ensuring locations exist on the filesystem first, then register
    * every partition directory on disk.
    */
  def bootstrap(spark: SparkSession, cfg: RatConfig): Unit = {
    ensureLocations(spark, cfg)
    ddl(cfg).foreach(spark.sql)
    msck(spark)
  }

  def ensureLocations(spark: SparkSession, cfg: RatConfig): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    Seq(cfg.hiveRawLocation, cfg.hiveCorrelatedLocation).foreach { loc =>
      try {
        val path = new org.apache.hadoop.fs.Path(loc)
        val fs = path.getFileSystem(hadoopConf)
        if (!fs.exists(path)) {
          fs.mkdirs(path)
        }
      } catch {
        case _: Exception =>
      }
    }
  }

  def msck(spark: SparkSession): Unit = {
    try {
      spark.sql("MSCK REPAIR TABLE rat_events")
    } catch {
      case _: Exception =>
    }
    try {
      spark.sql("MSCK REPAIR TABLE correlated")
    } catch {
      case _: Exception =>
    }
  }
}
