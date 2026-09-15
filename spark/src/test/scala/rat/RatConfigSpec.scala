package rat

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class RatConfigSpec extends AnyFunSuite {

  test("defaults when env is empty") {
    val c = RatConfig.fromEnv(Map.empty)
    assert(c.bootstrap == "localhost:9092")
    assert(c.topicPattern == "events\\.(?!.*\\.dlq$).*")
    assert(c.sinkDir == "/tmp/rat")
    assert(c.checkpointDir == "/tmp/rat/checkpoints")
    assert(c.watermark == "10 minutes")
    assert(c.startingOffsets == "latest")
    assert(c.trigger.isEmpty)
    assert(c.master == "local[*]")
  }

  test("env overrides win") {
    val c = RatConfig.fromEnv(
      Map(
        "RAT_BOOTSTRAP" -> "broker:9092",
        "RAT_TOPIC_PATTERN" -> "events\\..*",
        "RAT_SINK_DIR" -> "/data/rat",
        "RAT_CHECKPOINT_DIR" -> "/data/ck",
        "RAT_WATERMARK" -> "5 minutes",
        "RAT_STARTING_OFFSETS" -> "earliest",
        "RAT_TRIGGER" -> "30 seconds",
        "SPARK_MASTER" -> "local[2]"
      )
    )
    assert(c.bootstrap == "broker:9092")
    assert(c.topicPattern == "events\\..*")
    assert(c.sinkDir == "/data/rat")
    assert(c.checkpointDir == "/data/ck")
    assert(c.watermark == "5 minutes")
    assert(c.startingOffsets == "earliest")
    assert(c.trigger == Some("30 seconds"))
    assert(c.master == "local[2]")
  }

  test("checkpoint dir derives from sink dir") {
    val c = RatConfig.fromEnv(Map("RAT_SINK_DIR" -> "/data/rat"))
    assert(c.checkpointDir == "/data/rat/checkpoints")
  }

  test("paths carry file:// unless the dir already has a scheme") {
    val c = RatConfig.fromEnv(Map.empty)
    assert(c.correlatedPath == "file:///tmp/rat/correlated")
    assert(c.rawPath == "file:///tmp/rat/events_raw")
    assert(c.checkpoint("correlate") == "file:///tmp/rat/checkpoints/correlate")

    val hdfs = RatConfig.fromEnv(
      Map(
        "RAT_SINK_DIR" -> "hdfs://nn/rat",
        "RAT_CHECKPOINT_DIR" -> "hdfs://nn/rat/checkpoints"
      )
    )
    assert(hdfs.correlatedPath == "hdfs://nn/rat/correlated")
    assert(hdfs.rawPath == "hdfs://nn/rat/events_raw")
    assert(hdfs.checkpoint("raw") == "hdfs://nn/rat/checkpoints/raw")
  }

  test("blank trigger means continuous") {
    assert(RatConfig.fromEnv(Map("RAT_TRIGGER" -> "")).trigger.isEmpty)
    assert(RatConfig.fromEnv(Map("RAT_TRIGGER" -> "   ")).trigger.isEmpty)
  }

  test("default topic pattern matches plugin topics, not DLQs") {
    val pattern = java.util.regex.Pattern.compile(RatConfig.fromEnv(Map.empty).topicPattern)
    assert(pattern.matcher("events.hn").matches())
    assert(pattern.matcher("events.rss").matches())
    assert(pattern.matcher("events.hn.dlq").matches() == false)
  }

  test("hive off by default, base follows sink dir") {
    val c = RatConfig.fromEnv(Map.empty)
    assert(!c.hiveEnabled)
    assert(c.hiveMetastoreUri.isEmpty)
    assert(c.hiveBase == "/tmp/rat")
    assert(c.hiveRawLocation == "/tmp/rat/events_raw")
    assert(c.hiveCorrelatedLocation == "/tmp/rat/correlated")
  }

  test("hive env overrides") {
    val c = RatConfig.fromEnv(
      Map(
        "RAT_HIVE_ENABLED" -> "true",
        "RAT_HIVE_METASTORE_URI" -> "thrift://localhost:9083",
        "RAT_HIVE_MSCK_MIN_SECONDS" -> "5",
        "RAT_SINK_DIR" -> "hdfs://nn/rat"
      )
    )
    assert(c.hiveEnabled)
    assert(c.hiveMetastoreUri == "thrift://localhost:9083")
    assert(c.hiveMsckMinSeconds == 5)
    assert(c.hiveBase == "hdfs://nn/rat")
    assert(c.hiveRawLocation == "hdfs://nn/rat/events_raw")
  }

  test("configure wires hive support and metastore uri") {
    val off = RatConfig.fromEnv(Map.empty)
    assert(off.sparkConfs.isEmpty)

    val on = RatConfig
      .fromEnv(Map("RAT_HIVE_ENABLED" -> "true", "RAT_HIVE_METASTORE_URI" -> "thrift://m:9083"))
    assert(on.sparkConfs == Map("hive.metastore.uris" -> "thrift://m:9083"))
    on.configure(SparkSession.builder()) // smoke: applies without throwing
  }

  test("negative msck interval clamps to zero, invalid falls back to 30") {
    assert(RatConfig.fromEnv(Map("RAT_HIVE_MSCK_MIN_SECONDS" -> "-5")).hiveMsckMinSeconds == 0)
    assert(RatConfig.fromEnv(Map("RAT_HIVE_MSCK_MIN_SECONDS" -> "junk")).hiveMsckMinSeconds == 30)
  }
}
