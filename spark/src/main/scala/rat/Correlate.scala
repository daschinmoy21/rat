package rat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Correlate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("rat-correlate")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "events.hn,events.rss")
      .option("startingOffsets", "latest")
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

    val left = withTime.filter(col("source") === "hn")
      .withWatermark("eventTime", "10 minutes")
      .select(col("event_id").as("a_id"), col("entities"), col("ts_ms"), col("eventTime"))
      .withColumn("entity", explode(col("entities")))

    val right = withTime.filter(col("source") === "rss")
      .withWatermark("eventTime", "10 minutes")
      .select(col("event_id").as("b_id"), col("entities"), col("ts_ms"), col("eventTime"))
      .withColumn("entity", explode(col("entities")))

    val joined = left.as("l").join(right.as("r"),
      expr("l.entity = r.entity AND " +
        "l.eventTime BETWEEN r.eventTime - INTERVAL 10 MINUTES AND r.eventTime + INTERVAL 10 MINUTES"))
      .select(col("l.entity").as("entity"), col("l.a_id"), col("r.b_id"),
              col("l.ts_ms"), to_date(col("l.eventTime")).as("dt"))

    joined.writeStream
      .format("parquet")
      .partitionBy("dt")
      .option("path", "file:///tmp/rat/correlated")
      .option("checkpointLocation", "file:///tmp/rat/checkpoints/correlate")
      .outputMode("append")
      .start()

    parsed.filter(size(col("entities")) === 0)
      .withColumn("dt", to_date((col("ts_ms") / 1000).cast("timestamp")))
      .writeStream.format("parquet").partitionBy("dt", "source")
      .option("path", "file:///tmp/rat/events_raw")
      .option("checkpointLocation", "file:///tmp/rat/checkpoints/raw")
      .outputMode("append")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
