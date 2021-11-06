package codes.lyndon.spark.job

import codes.lyndon.spark.{ExternalCatalogHelper, RecordCountListener}
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.util.Try

case class ExampleJobConfig(
    override val jobName: String,
    override val inputs: Seq[ReadTable],
    override val outputs: Seq[WriteTable]
) extends JobConfig

case class ExampleReadTable(
    override val name: String,
    override val source: DataSource
) extends ReadTable

case class ExampleWriteTable(
    override val name: String,
    override val source: DataSource
) extends WriteTable

object ExampleJob {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    implicit val jobConfig: ExampleJobConfig = ExampleJobConfig(
      "Example Job",
      Seq(),
      Seq(
        ExampleWriteTable("example", DataSource("tmp", LocalFileSystem, "/tmp"))
      )
    )

    implicit val lineageService: LineageService = new OpenLineageService(
      "example"
    )

    val procs = Runtime.getRuntime.availableProcessors()

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master(s"local[$procs]")
      .appName("example")
      //.enableHiveSupport()
      .getOrCreate()

    val runId = UUID.randomUUID()
    lineageService.startJob(jobConfig, runId)

    val ran = run(100000L, runId)

    ran.foreach { results =>
      logger.info("Job completed")
      val JobSuccess(lineageStats, lineageSchema) = results

      lineageService.completeJob(
        jobConfig,
        runId,
        lineageStats = lineageStats,
        tableSchemas = lineageSchema
      )
    }

    ran.failed.foreach { cause =>
      logger.error("Job failed", cause)
      lineageService.failJob(jobConfig, runId)
    }

    scala.io.StdIn.readLine()
  }

  def run(
      count: Long,
      runId: UUID
  )(implicit
      spark: SparkSession,
      config: ExampleJobConfig,
      lineage: LineageService
  ): Try[JobSuccess] =
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

      val db          = spark.catalog.currentDatabase
      val outputTable = config.outputs.head
      val table       = outputTable.name
      val outputPath  = outputTable.source.locationURI.resolve(table).toString
      logger.info(s"Writing out $table to $outputPath")
      df.write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", outputPath)
        .partitionBy("remainder")
        .saveAsTable(s"$db.$table")

      val stats = ExternalCatalogHelper.PreCollectedStats(
        Some(countListener.totalRecordsWritten),
        Some(countListener.totalBytesWritten),
        Map(
          "id" -> CatalogColumnStat(
            distinctCount = Some(countListener.totalRecordsWritten),
            min = Some("0"),
            max = Some(s"$count"),
            nullCount = Some(0)
          )
        )
      )

      val updated = ExternalCatalogHelper.updateStats(
        db,
        table,
        stats
      )
      updated.failed.foreach { cause =>
        logger.warn("Count not update the external catalog", cause)
      }

      logger.info("Write Metrics:")
      countListener.currentMetrics.foreach(s => logger.info(s"\t$s"))

      spark.sparkContext.removeSparkListener(countListener)

      ExternalCatalogHelper
        .currentStats(db, table)
        .foreach(stats => logger.info(s"Stats:\n$stats"))

      val statTuple =
        (config.outputs.head, LineageStatistics.from(stats)) match {
          case (table, Some(stats)) => Some(table, stats)
          case _                    => None
        }

      val schema = (config.outputs.head, LineageSchema.from(df))

      JobSuccess(statTuple.toMap, Map(schema))
    }

}
