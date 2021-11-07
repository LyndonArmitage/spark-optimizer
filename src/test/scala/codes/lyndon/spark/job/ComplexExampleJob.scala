package codes.lyndon.spark.job

import codes.lyndon.spark.{ExternalCatalogHelper, RecordCountListener}
import codes.lyndon.spark.job.JobOutcome._
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.util.Try

case class ComplexExampleJobConfig(
    override val jobName: String,
    override val inputs: Seq[ReadTable],
    override val outputs: Seq[WriteTable],
    count: Long
) extends JobConfig

case class ComplexExampleReadTable(
    override val name: String,
    override val source: DataSource
) extends ReadTable

case class ComplexExampleWriteTable(
    override val name: String,
    override val source: DataSource
) extends WriteTable

object ComplexExampleJob extends SparkJob[ComplexExampleJobConfig] {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    implicit val jobConfig: ComplexExampleJobConfig = ComplexExampleJobConfig(
      "Complex Example Job",
      Seq(
        ComplexExampleReadTable("hardcoded", DataSource("default", JDBC, "jdbc://default"))
      ),
      Seq(
        ComplexExampleWriteTable("example", DataSource("tmp", LocalFileSystem, "/tmp"))
      ),
      100000L
    )

    implicit val lineageService: LineageService = new OpenLineageService(
      "example"
    )

    implicit val spark: SparkSession = getSparkSession()

    spark.range(1000L)
      .withColumn("random", rand())
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", "/tmp/hardcoded")
      .saveAsTable(s"default.hardcoded")

    val runId = UUID.randomUUID()

    runJob(runId)

    logger.info(s"Run ID: $runId")
    scala.io.StdIn.readLine()
  }

  override protected def run(runId: UUID)(implicit
      spark: SparkSession,
      config: ComplexExampleJobConfig,
      lineage: LineageService
  ): Either[JobFailed, JobSucceeded] =
    Try {
      import spark.implicits._

      val countListener = RecordCountListener()
      spark.sparkContext.addSparkListener(countListener)
      val count = config.count
      val range = spark.range(count)

      val hardcodedTable = config.inputs.head
      val hardcoded = spark.table(s"${hardcodedTable.source.name}.${hardcodedTable.name}")

      val df = hardcoded
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

      ExternalCatalogHelper.updateStats(db, table, stats)
      countListener.reset()

      ExternalCatalogHelper
        .currentStats(db, table)
        .foreach(stats => logger.info(s"Stats:\n$stats"))

      val out1StatTuple =
      (config.outputs.head, LineageStatistics.from(stats)) match {
        case (table, Some(stats)) => Some(table, stats)
        case _                    => None
      }

      spark.sparkContext.removeSparkListener(countListener)

      val out1Schema = (config.outputs.head, LineageSchema.from(df))
      JobSucceeded(
        lineageSchemas = Map(out1Schema),
        lineageStats = out1StatTuple.toMap
      )
    }.asEither

  // In a production environment you'd provide this differently
  private def getSparkSession(
      procs: Int = Runtime.getRuntime.availableProcessors()
  ): SparkSession = {
    SparkSession
      .builder()
      .master(s"local[$procs]")
      .appName("example")
      //.enableHiveSupport
      .getOrCreate()
  }
}
