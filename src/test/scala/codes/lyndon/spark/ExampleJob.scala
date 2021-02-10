package codes.lyndon.spark

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory

import scala.util.Try

object ExampleJob {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val procs = Runtime.getRuntime.availableProcessors()

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master(s"local[$procs]")
      .appName("example")
      //.enableHiveSupport()
      .getOrCreate()

    run().failed.foreach { cause =>
      logger.error("Job failed", cause)
    }
  }

  def run(count: Long = 100000L)(implicit spark: SparkSession): Try[Unit] =
    Try {
      import spark.implicits._

      val countListener = RecordCountListener()
      spark.sparkContext.addSparkListener(countListener)

      val range = spark.range(count)

      val df = range
        .withColumnRenamed("0", "id")
        .withColumn("random", rand())
        .withColumn("sha256", sha2($"id".cast(StringType), 256))
        .withColumn("md5", md5($"id".cast(StringType)))
        .withColumn("is_even", ($"id" % 2) === lit(0))
        .withColumn("is_odd", !$"is_even")
        .withColumn("remainder", $"id" % 10)

      val db    = spark.catalog.currentDatabase
      val table = "example"
      df.write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", "/tmp/example.table")
        .partitionBy("remainder")
        .saveAsTable(s"$db.$table")

      val updated = ExternalCatalogHelper.updateStats(
        db,
        table,
        Some(countListener.totalRecordsWritten),
        Map(
          "id" -> CatalogColumnStat(
            distinctCount = Some(countListener.totalRecordsWritten),
            min = Some("0"),
            max = Some(s"$count"),
            nullCount = Some(0)
          )
        )
      )
      updated.failed.foreach { cause =>
        logger.warn("Count not update the external catalog", cause)
      }

      logger.info("Write Metrics:")
      countListener.currentMetrics.foreach(s => logger.info(s"\t$s"))

      spark.sparkContext.removeSparkListener(countListener)

    }

}
