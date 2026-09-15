package rat

/** Runtime config from the environment. Defaults match the in-tree single-node setup. */
final case class RatConfig(
    bootstrap: String,
    topicPattern: String,
    sinkDir: String,
    checkpointDir: String,
    watermark: String,
    startingOffsets: String,
    trigger: Option[String],
    master: String
) {
  def correlatedPath: String = withScheme(s"$sinkDir/correlated")
  def rawPath: String = withScheme(s"$sinkDir/events_raw")
  def checkpoint(name: String): String = withScheme(s"$checkpointDir/$name")

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
    master = "local[*]"
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
      master = env.getOrElse("SPARK_MASTER", defaults.master)
    )
  }
}
