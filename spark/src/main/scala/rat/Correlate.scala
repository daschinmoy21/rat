package rat

import org.apache.spark.sql.SparkSession

object Correlate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("rat-correlate")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()

    spark.stop()
  }
}
