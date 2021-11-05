package codes.lyndon.spark.job

import java.time.ZonedDateTime
import java.util.UUID
import scala.util.Try

trait LineageService {

  def startJob(
      config: JobConfig,
      runId: UUID,
      eventTime: ZonedDateTime = ZonedDateTime.now(),
      lineageStats: Map[Table, LineageStatistics] = Map.empty
  ): Try[Unit]

  def completeJob(
      config: JobConfig,
      runId: UUID,
      eventTime: ZonedDateTime = ZonedDateTime.now(),
      lineageStats: Map[Table, LineageStatistics] = Map.empty
  ): Try[Unit]

  def abortJob(
      config: JobConfig,
      runId: UUID,
      eventTime: ZonedDateTime = ZonedDateTime.now()
  ): Try[Unit]

  def failJob(
      config: JobConfig,
      runId: UUID,
      eventTime: ZonedDateTime = ZonedDateTime.now()
  ): Try[Unit]

}
