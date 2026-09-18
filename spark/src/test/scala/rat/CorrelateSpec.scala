package rat

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Batch tests for the correlate join: pairs() must work for any sources on the bus, order each
  * unordered source pair exactly once, and keep the inclusive window. The watermark is a
  * streaming-only concern, so these run without it and exercise the join predicate directly.
  */
class CorrelateSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("rat-correlate-spec")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  private val envelopeSchema = StructType(
    Seq(
      StructField("event_id", StringType),
      StructField("source", StringType),
      StructField("entities", ArrayType(StringType)),
      StructField("ts_ms", LongType)
    )
  )

  private val t0 = 1_700_000_000_000L

  private def env(id: String, source: String, entities: Seq[String], ts: Long): Row =
    Row(id, source, entities, ts)

  private def correlate(rows: Row*): Array[Row] = {
    val df: DataFrame =
      spark.createDataFrame(spark.sparkContext.parallelize(rows), envelopeSchema)
    Correlate.pairs(df, "10 minutes").orderBy("a_id", "b_id").collect()
  }

  private def id(row: Row, field: String): String = row.getAs[String](field)

  test("any two sources pair on a shared entity inside the window") {
    val rows = correlate(
      env("b1", "blog", Seq("ETH"), t0),
      env("p1", "podcast", Seq("ETH"), t0 + 60_000L)
    )
    assert(rows.length == 1)
    assert(id(rows.head, "entity") == "ETH")
    assert(id(rows.head, "a_id") == "b1") // lexicographically smaller source
    assert(id(rows.head, "b_id") == "p1")
    assert(rows.head.getAs[Long]("ts_ms") == t0 + 60_000L) // the later of the pair
    assert(rows.head.getAs[Any]("dt") != null)
  }

  test("hn and rss pair exactly once: a_id from hn, b_id from rss") {
    val rows = correlate(
      env("e:hn:1", "hn", Seq("AAPL"), t0),
      env("e:rss:1", "rss", Seq("AAPL"), t0 + 30_000L)
    )
    assert(rows.length == 1)
    assert(id(rows.head, "a_id") == "e:hn:1")
    assert(id(rows.head, "b_id") == "e:rss:1")
    assert(rows.head.getAs[Long]("ts_ms") == t0 + 30_000L)
  }

  test("no self or same-source pairs, no mirrored duplicates") {
    assert(
      correlate(
        env("h1", "hn", Seq("AAPL"), t0),
        env("h2", "hn", Seq("AAPL"), t0 + 1000L)
      ).isEmpty
    )
    assert(correlate(env("h1", "hn", Seq("AAPL"), t0)).isEmpty)
  }

  test("three sources sharing an entity give C(3,2) pairs") {
    val rows = correlate(
      env("a1", "alpha", Seq("AAPL"), t0),
      env("b1", "beta", Seq("AAPL"), t0 + 1000L),
      env("g1", "gamma", Seq("AAPL"), t0 + 2000L)
    )
    assert(
      rows.map(r => (id(r, "a_id"), id(r, "b_id"))).toSeq ==
        Seq(("a1", "b1"), ("a1", "g1"), ("b1", "g1"))
    )
  }

  test("duplicate entities inside one event do not multiply output") {
    val rows = correlate(
      env("h1", "hn", Seq("AAPL", "AAPL"), t0),
      env("r1", "rss", Seq("AAPL"), t0 + 1000L)
    )
    assert(rows.length == 1)
  }

  test("multiple shared entities give one row per entity") {
    val rows = correlate(
      env("h1", "hn", Seq("AAPL", "MSFT"), t0),
      env("r1", "rss", Seq("AAPL", "MSFT"), t0 + 1000L)
    )
    assert(rows.map(id(_, "entity")).toSeq == Seq("AAPL", "MSFT"))
    assert(rows.forall(r => id(r, "a_id") == "h1" && id(r, "b_id") == "r1"))
  }

  test("pair inside, exactly at, and outside the watermark window") {
    def pair(delta: Long): Array[Row] = correlate(
      env("h1", "hn", Seq("AAPL"), t0),
      env("r1", "rss", Seq("AAPL"), t0 + delta)
    )
    assert(pair(60_000L).length == 1) // inside
    assert(pair(600_000L).length == 1) // exact boundary, inclusive
    assert(pair(-600_000L).length == 1) // boundary from the other side
    assert(pair(600_001L).isEmpty) // one ms past the boundary
    assert(pair(-600_001L).isEmpty)
  }

  test("null or blank source and entity rows are excluded") {
    // null/blank sources never pair; the valid hn/rss pair still lands once
    val rows = correlate(
      env("n1", null, Seq("AAPL"), t0),
      env("n2", "  ", Seq("AAPL"), t0 + 1000L),
      env("h1", "hn", Seq("AAPL"), t0 + 2000L),
      env("r1", "rss", Seq("AAPL"), t0 + 3000L)
    )
    assert(rows.length == 1)
    assert(id(rows.head, "a_id") == "h1" && id(rows.head, "b_id") == "r1")

    // null/blank entity elements drop; the real entity pairs exactly once
    assert(
      correlate(
        env("h1", "hn", Seq(null, "", "  ", "AAPL"), t0),
        env("r1", "rss", Seq("AAPL"), t0 + 1000L)
      ).length == 1
    )

    // a null entities array behaves like empty: nothing to pair
    assert(
      correlate(
        env("h1", "hn", null, t0),
        env("r1", "rss", Seq("AAPL"), t0 + 1000L)
      ).isEmpty
    )
  }
}
