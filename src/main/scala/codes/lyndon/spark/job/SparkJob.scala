package codes.lyndon.spark.job

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.util.{Failure, Success}

abstract class SparkJob[ConfigType <: JobConfig] {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  final def runJob(runId: UUID = UUID.randomUUID())(implicit
      spark: SparkSession,
      config: ConfigType,
      lineage: LineageService
  ): Either[JobFailed, JobSucceeded] = {
    val job = s"${config.jobName}:$runId"
    logger.info(s"Starting job: $job")

    val inputStats   = getReadStats()
    val inputSchemas = getReadSchemas()

    lineage.startJob(
      config,
      runId,
      lineageStats = inputStats.toMap,
      tableSchemas = inputSchemas.toMap
    ) match {
      case Failure(cause) =>
        logger.error("Could not reach lineage service to start job.")
        // Re-throw and cause the program to exit early
        throw cause
      case Success(_) =>
    }
    val ran = run(runId)

    val outcome = ran.asOutcome
    val mergedStats =
      LineageStatistics.mergeLatest(inputStats.toMap, outcome.lineageStats)
    val mergedSchemas =
      LineageSchema.mergeLatest(inputSchemas.toMap, outcome.lineageSchemas)

    ran match {
      case Right(success) =>
        val JobSucceeded(endTime, lineageSchemas, lineageStats) = success
        logger.info(s"Job $job completed at $endTime")

        lineage.completeJob(
          config,
          runId,
          endTime,
          mergedStats,
          mergedSchemas
        ) match {
          case Failure(cause) =>
            logger.error(
              s"Could not reach lineage service to report success of $job",
              cause
            )
          case Success(_) =>
        }
      case Left(failure) =>
        val JobFailed(message, cause, endTime, lineageSchemas, lineageStats) =
          failure
        logger.error(s"Job $job failed at $endTime")
        cause match {
          case Some(cause) => logger.error(s"Failed due to: $message", cause)
          case None        => logger.error(s"Failed due to: $message")
        }
        lineage.failJob(
          config,
          runId,
          endTime,
          mergedStats,
          mergedSchemas
        ) match {
          case Failure(cause) =>
            logger.error(
              s"Could not reach lineage service to report failure of $job",
              cause
            )
          case Success(_) =>
        }
    }

    ran
  }

  protected def run(
      runId: UUID
  )(implicit
      spark: SparkSession,
      config: ConfigType,
      lineage: LineageService
  ): Either[JobFailed, JobSucceeded]

  final protected def getReadStats()(implicit
      spark: SparkSession,
      config: ConfigType
  ): Map[ReadTable, LineageStatistics] = {
    config.inputs.flatMap { table =>
      LineageStatistics.fromCatalog(table) match {
        case Some(value) => Some(table, value)
        case None        => None
      }
    }.toMap
  }

  final protected def getReadSchemas()(implicit
      spark: SparkSession,
      config: ConfigType
  ): Map[ReadTable, LineageSchema] = {
    config.inputs.flatMap { table =>
      LineageSchema.fromCatalog(table) match {
        case Some(value) => Some(table, value)
        case None        => None
      }
    }.toMap
  }

}
