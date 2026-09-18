package rat

import org.apache.spark.sql.{DataFrame, SparkSession}
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

    sink(pairs(parsed, cfg.watermark), "correlate-v2", cfg.correlatedPath, Seq("dt"))
    sink(
      parsed
        // a missing entities array counts as entity-less for the raw sink
        .filter(coalesce(size(col("entities")), lit(0)) === 0)
        .withColumn("dt", to_date((col("ts_ms") / 1000).cast("timestamp"))),
      "raw",
      cfg.rawPath,
      Seq("dt", "source")
    )

    spark.streams.awaitAnyTermination()
  }

  /** Correlate envelopes from any sources on the bus: explode each envelope to one row per entity,
    * then self-join rows sharing an entity whose event times fall within the watermark window
    * (inclusive on both bounds, as before). l.source < r.source keeps each unordered source pair
    * exactly once — no self, same-source, or mirrored rows — and pins a_id to the lexicographically
    * smaller source, so hn still lands in a_id and rss in b_id like the original two-source job.
    * Duplicate entities inside one envelope collapse and cannot multiply output; null/blank source
    * or entity never pairs. ts_ms is the later of the pair's times, dt derives from it.
    */
  def pairs(df: DataFrame, watermark: String): DataFrame =
    perEntity(df, watermark)
      .as("l")
      .join(
        perEntity(df, watermark).as("r"),
        expr(
          s"l.entity = r.entity AND " +
            s"l.eventTime BETWEEN r.eventTime - INTERVAL $watermark " +
            s"AND r.eventTime + INTERVAL $watermark AND l.source < r.source"
        )
      )
      .select(
        col("l.entity").as("entity"),
        col("l.event_id").as("a_id"),
        col("r.event_id").as("b_id"),
        greatest(col("l.ts_ms"), col("r.ts_ms")).as("ts_ms"),
        to_date((greatest(col("l.ts_ms"), col("r.ts_ms")) / 1000).cast("timestamp")).as("dt")
      )

  /** One row per (envelope, entity): source and entity must be present (not null/blank), duplicate
    * entities collapse via array_distinct. The watermark bounds the join state in streaming only —
    * batch callers (tests) skip it and exercise the join predicate directly.
    */
  private def perEntity(df: DataFrame, watermark: String): DataFrame = {
    val exploded = df
      .withColumn("eventTime", (col("ts_ms") / 1000).cast("timestamp"))
      .filter(col("source").isNotNull && trim(col("source")) =!= "")
      .select(
        col("event_id"),
        col("source"),
        col("ts_ms"),
        col("eventTime"),
        explode(array_distinct(col("entities"))).as("entity")
      )
      .filter(col("entity").isNotNull && trim(col("entity")) =!= "")
    if (df.isStreaming) exploded.withWatermark("eventTime", watermark) else exploded
  }
}
