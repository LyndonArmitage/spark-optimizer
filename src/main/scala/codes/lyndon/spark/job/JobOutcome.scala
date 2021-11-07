package codes.lyndon.spark.job

import java.time.ZonedDateTime
import scala.util.{Failure, Success, Try}

sealed trait JobOutcome {
  def succeeded: Boolean
  def endTime: ZonedDateTime
  def failed: Boolean = !succeeded
  def lineageStats: Map[Table, LineageStatistics]
  def lineageSchemas: Map[Table, LineageSchema]
}

final case class JobFailed(
    message: String,
    cause: Option[Throwable] = None,
    override val endTime: ZonedDateTime = ZonedDateTime.now(),
    override val lineageSchemas: Map[Table, LineageSchema] = Map.empty,
    override val lineageStats: Map[Table, LineageStatistics] = Map.empty
) extends JobOutcome {
  override def succeeded: Boolean = false
}

final case class JobSucceeded(
    override val endTime: ZonedDateTime = ZonedDateTime.now(),
    override val lineageSchemas: Map[Table, LineageSchema] = Map.empty,
    override val lineageStats: Map[Table, LineageStatistics] = Map.empty
) extends JobOutcome {
  override def succeeded: Boolean = true
}

object JobOutcome {

  implicit class EitherExtension(either: Either[JobFailed, JobSucceeded]) {

    def asTry: Try[JobSucceeded] = {
      either match {
        case Left(failure) =>
          Failure(
            failure.cause.getOrElse(new RuntimeException(failure.message))
          )
        case Right(success) => Success(success)
      }
    }

    def asOutcome: JobOutcome = {
      either match {
        case Left(value)  => value
        case Right(value) => value
      }
    }

  }

  implicit class TryExtension(t: Try[JobSucceeded]) {

    def asEither: Either[JobFailed, JobSucceeded] = {
      t match {
        case Failure(cause) =>
          Left(JobFailed(cause.getMessage, cause = Some(cause)))
        case Success(value) => Right(value)
      }
    }
  }
}
