package codes.lyndon.spark.job

import org.slf4j.LoggerFactory

import java.net.URI
import java.time.{LocalDateTime, Month, ZoneId, ZonedDateTime}
import java.util.UUID
import scala.util.{Failure, Success}

object Tester {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  private case class TestConfig(
      override val jobName: String,
      override val inputs: Seq[ReadTable],
      override val outputs: Seq[WriteTable]
  ) extends JobConfig

  private case class TestReadTable(
      override val name: String,
      override val source: DataSource
  ) extends ReadTable
  private case class TestWriteTable(
      override val name: String,
      override val source: DataSource
  ) extends WriteTable

  def main(args: Array[String]): Unit = {
    val api = args.headOption
      .map(URI.create)
      .getOrElse(URI.create("http://localhost:5000/api/v1/lineage"))
    logger.info(s"Using API: $api")

    val lineage = new OpenLineageService("testing_java", api)

    simpleRun(lineage)
    runWithFailures(lineage)
    cyclicJob(lineage)

  }

  def cyclicJob(lineage: LineageService): Unit = {

    val myDb =
      DataSource("my-db", JDBC, "jdbc:oracle:thin:@192.168.0.1:5454:my-db")

    val originalTable = TestReadTable("incremental_table", myDb)
    val incrementalData = TestReadTable(
      "incremental_data",
      DataSource("incremental", S3FileSystem, "s3://testing/foo")
    )
    val updatedTable = TestWriteTable("incremental_table", myDb)
    val testConfig = TestConfig(
      "cyclic-job",
      Seq(originalTable, incrementalData),
      Seq(updatedTable)
    )

    val runId = UUID.randomUUID()
    logger.info(s"Using runId = $runId")

    val startTime = ZonedDateTime.of(
      LocalDateTime.of(2021, Month.NOVEMBER, 1, 9, 0, 0),
      ZoneId.systemDefault()
    )

    lineage.startJob(
      testConfig,
      runId,
      startTime,
      Map(
        originalTable   -> LineageStatistics(1000000L, 1024L * 10),
        incrementalData -> LineageStatistics(1000L, 1024L)
      )
    ) match {
      case Failure(e) => logger.error("Failed to start job", e)
      case Success(_) => logger.info("Succeeded in sending start job")
    }
    val endTime = startTime.plusMinutes(75)

    lineage.completeJob(testConfig, runId, endTime, Map(
      originalTable   -> LineageStatistics(1000000L, 1024L * 10),
      incrementalData -> LineageStatistics(1000L, 1024L),
      updatedTable -> LineageStatistics(1001000L, (1024L * 10) + 1024L)
    )) match {
      case Failure(e) => logger.error("Failed to complete job", e)
      case Success(_) => logger.info("Succeeded in sending complete job")
    }
  }

  def simpleRun(lineage: LineageService): Unit = {

    val myDb =
      DataSource("my-db", JDBC, "jdbc:oracle:thin:@192.168.0.1:5454:my-db")
    val testConfig = TestConfig(
      "test-blacklists",
      Seq(
        TestReadTable("users", myDb),
        TestReadTable("dim_accesses", myDb),
        TestReadTable("blacklist", myDb)
      ),
      Seq(
        TestWriteTable("security_events", myDb)
      )
    )

    val runId = UUID.randomUUID()
    logger.info(s"Using runId = $runId")

    val startTime = ZonedDateTime.of(
      LocalDateTime.of(2021, Month.MAY, 21, 9, 0, 0),
      ZoneId.systemDefault()
    )

    lineage.startJob(testConfig, runId, startTime) match {
      case Failure(e) => logger.error("Failed to start job", e)
      case Success(_) => logger.info("Succeeded in sending start job")
    }

    val endTime = ZonedDateTime.of(
      LocalDateTime.of(2021, Month.MAY, 21, 9, 30, 0),
      ZoneId.systemDefault()
    )

    lineage.completeJob(testConfig, runId, endTime) match {
      case Failure(e) => logger.error("Failed to complete job", e)
      case Success(_) => logger.info("Succeeded in sending complete job")
    }
  }

  def runWithFailures(lineage: LineageService): Unit = {

    val myDb =
      DataSource("my-db", JDBC, "jdbc:oracle:thin:@192.168.0.1:5454:my-db")
    val testConfig = TestConfig(
      "test-job",
      Seq(
        TestReadTable("users", myDb),
        TestReadTable("access-times", myDb)
      ),
      Seq(
        TestWriteTable("dim_accesses", myDb)
      )
    )

    val runId = UUID.randomUUID()
    logger.info(s"Using runId = $runId")

    val startTime = ZonedDateTime.of(
      LocalDateTime.of(2021, Month.MAY, 21, 9, 0, 0),
      ZoneId.systemDefault()
    )

    lineage.startJob(testConfig, runId, startTime) match {
      case Failure(e) => logger.error("Failed to start job", e)
      case Success(_) => logger.info("Succeeded in sending start job")
    }

    val abortTime = ZonedDateTime.of(
      LocalDateTime.of(2021, Month.MAY, 21, 9, 10, 10),
      ZoneId.systemDefault()
    )

    lineage.abortJob(testConfig, runId, abortTime) match {
      case Failure(e) => logger.error("Failed to abort job", e)
      case Success(_) => logger.info("Succeeded in sending abort job")
    }

//    val endTime = ZonedDateTime.of(
//      LocalDateTime.of(2021, Month.MAY, 21, 9, 30, 0),
//      ZoneId.systemDefault()
//    )
//
//    lineage.completeJob(testConfig, runId, endTime) match {
//      case Failure(e) => logger.error("Failed to complete job", e)
//      case Success(_) => logger.info("Succeeded in sending complete job")
//    }
  }
}
