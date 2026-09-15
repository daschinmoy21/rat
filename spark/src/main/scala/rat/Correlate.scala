package rat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.types._

object Correlate {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val cfg = RatConfig.fromEnv(sys.env)

    val spark = cfg
      .configure(
        SparkSession
          .builder()
          .appName("rat-correlate")
          .master(cfg.master)
      )
      .getOrCreate()

    if (cfg.hiveEnabled) {
      Hive.bootstrap(spark, cfg)
      // Register partitions as each micro-batch lands so Hive reads stay
      // fresh — throttled: at most one MSCK per RAT_HIVE_MSCK_MIN_SECONDS,
      // CAS'd so both sinks' progress events cannot repair concurrently.
      // A failed repair rolls the throttle back so the next batch retries,
      // and never throws into the listener bus.
      val lastMsck = new java.util.concurrent.atomic.AtomicLong(0L)
      spark.streams.addListener(new StreamingQueryListener {
        override def onQueryProgress(e: StreamingQueryListener.QueryProgressEvent): Unit = {
          val now = System.currentTimeMillis()
          val last = lastMsck.get()
          if (
            now - last >= cfg.hiveMsckMinSeconds * 1000L &&
            lastMsck.compareAndSet(last, now)
          ) {
            try {
              Hive.msck(spark)
            } catch {
              case e: Exception =>
                log.error("MSCK REPAIR failed; will retry next batch", e)
                lastMsck.compareAndSet(now, last)
            }
          }
        }
        override def onQueryStarted(e: StreamingQueryListener.QueryStartedEvent): Unit = {}
        override def onQueryTerminated(e: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
      })
    }

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.bootstrap)
      .option("subscribePattern", cfg.topicPattern)
      .option("startingOffsets", cfg.startingOffsets)
      .load()

    val envelopeSchema = new StructType()
      .add("event_id", StringType)
      .add("source", StringType)
      .add("entities", ArrayType(StringType))
      .add("ts_ms", LongType)
      .add("payload", StringType)

    // Malformed JSON parses to null: never let a null ts_ms reach event-time logic.
    val parsed = raw
      .select(from_json(col("value").cast("string"), envelopeSchema).as("e"))
      .select("e.*")
      .filter(col("event_id").isNotNull && col("ts_ms").isNotNull)

    val withTime = parsed.withColumn("eventTime", (col("ts_ms") / 1000).cast("timestamp"))

    val left = withTime
      .filter(col("source") === "hn")
      .withWatermark("eventTime", cfg.watermark)
      .select(col("event_id").as("a_id"), col("entities"), col("ts_ms"), col("eventTime"))
      .withColumn("entity", explode(col("entities")))

    val right = withTime
      .filter(col("source") === "rss")
      .withWatermark("eventTime", cfg.watermark)
      .select(col("event_id").as("b_id"), col("entities"), col("ts_ms"), col("eventTime"))
      .withColumn("entity", explode(col("entities")))

    // Join window tracks the watermark: events pair up within the same freshness bound.
    val joined = left
      .as("l")
      .join(
        right.as("r"),
        expr(
          s"l.entity = r.entity AND " +
            s"l.eventTime BETWEEN r.eventTime - INTERVAL ${cfg.watermark} " +
            s"AND r.eventTime + INTERVAL ${cfg.watermark}"
        )
      )
      .select(
        col("l.entity").as("entity"),
        col("l.a_id"),
        col("r.b_id"),
        col("l.ts_ms"),
        to_date(col("l.eventTime")).as("dt")
      )

    def sink(
        df: org.apache.spark.sql.DataFrame,
        name: String,
        path: String,
        partitions: Seq[String]
    ): Unit = {
      val writer = df.writeStream
        .format("parquet")
        .partitionBy(partitions: _*)
        .option("path", path)
        .option("checkpointLocation", cfg.checkpoint(name))
        .outputMode("append")
      cfg.trigger.foreach(t => writer.trigger(Trigger.ProcessingTime(t)))
      writer.start()
    }

    sink(joined, "correlate", cfg.correlatedPath, Seq("dt"))
    sink(
      parsed
        .filter(size(col("entities")) === 0)
        .withColumn("dt", to_date((col("ts_ms") / 1000).cast("timestamp"))),
      "raw",
      cfg.rawPath,
      Seq("dt", "source")
    )

    spark.streams.awaitAnyTermination()
  }
}
