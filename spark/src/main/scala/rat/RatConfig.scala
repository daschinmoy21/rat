package rat

import org.apache.spark.sql.SparkSession

/** Runtime config from the environment. Defaults match the in-tree single-node setup. */
final case class RatConfig(
    bootstrap: String,
    topicPattern: String,
    sinkDir: String,
    checkpointDir: String,
    watermark: String,
    startingOffsets: String,
    trigger: Option[String],
    master: String,
    hiveEnabled: Boolean,
    hiveMetastoreUri: String,
    hiveBase: String,
    hiveMsckMinSeconds: Int
) {
  def correlatedPath: String = withScheme(s"$sinkDir/correlated")
  def rawPath: String = withScheme(s"$sinkDir/events_raw")
  def checkpoint(name: String): String = withScheme(s"$checkpointDir/$name")

  /** Hive table locations follow the metastore's filesystem, not the sink scheme. */
  def hiveRawLocation: String = s"$hiveBase/events_raw"
  def hiveCorrelatedLocation: String = s"$hiveBase/correlated"

  /** Spark confs implied by the Hive layer (metastore thrift endpoint). */
  def sparkConfs: Map[String, String] =
    if (hiveMetastoreUri.isEmpty) Map.empty
    else Map("hive.metastore.uris" -> hiveMetastoreUri)

  /** Shared session setup: Hive support + thrift metastore when configured. */
  def configure(builder: SparkSession.Builder): SparkSession.Builder = {
    val withHive = if (hiveEnabled) builder.enableHiveSupport() else builder
    sparkConfs.foldLeft(withHive)((b, kv) => b.config(kv._1, kv._2))
  }

  private def withScheme(path: String): String =
    if (path.contains("://")) path else s"file://$path"
}

object RatConfig {
  // Anchored with .* at the end: Kafka matches subscribe patterns against the
  // full topic name (Matcher.matches), so the lookahead alone matches nothing.
  val defaults: RatConfig = RatConfig(
    bootstrap = "localhost:9092",
    topicPattern = "events\\.(?!.*\\.dlq$).*",
    sinkDir = "/tmp/rat",
    checkpointDir = "/tmp/rat/checkpoints",
    watermark = "10 minutes",
    startingOffsets = "latest",
    trigger = None,
    master = "local[*]",
    hiveEnabled = false,
    hiveMetastoreUri = "",
    hiveBase = "/tmp/rat",
    hiveMsckMinSeconds = 30
  )

  def fromEnv(env: Map[String, String]): RatConfig = {
    val sinkDir = env.getOrElse("RAT_SINK_DIR", defaults.sinkDir)
    defaults.copy(
      bootstrap = env.getOrElse("RAT_BOOTSTRAP", defaults.bootstrap),
      topicPattern = env.getOrElse("RAT_TOPIC_PATTERN", defaults.topicPattern),
      sinkDir = sinkDir,
      checkpointDir = env.getOrElse("RAT_CHECKPOINT_DIR", s"$sinkDir/checkpoints"),
      watermark = env.getOrElse("RAT_WATERMARK", defaults.watermark),
      startingOffsets = env.getOrElse("RAT_STARTING_OFFSETS", defaults.startingOffsets),
      trigger = env.get("RAT_TRIGGER").map(_.trim).filter(_.nonEmpty),
      master = env.getOrElse("SPARK_MASTER", defaults.master),
      hiveEnabled = env.get("RAT_HIVE_ENABLED").contains("true"),
      hiveMetastoreUri = env.getOrElse("RAT_HIVE_METASTORE_URI", defaults.hiveMetastoreUri),
      hiveBase = env.getOrElse("RAT_HIVE_BASE", sinkDir),
      hiveMsckMinSeconds = env
        .getOrElse("RAT_HIVE_MSCK_MIN_SECONDS", "30")
        .toIntOption
        .map(_.max(0))
        .getOrElse(30)
    )
  }
}
