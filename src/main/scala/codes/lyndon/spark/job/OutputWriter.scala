package codes.lyndon.spark.job

import codes.lyndon.spark.{ExternalCatalogHelper, RecordCountListener}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object OutputWriter {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  sealed abstract class WriteMode {
    def stringValue: String
    def sparkMode: SaveMode

    override def toString: String = stringValue
  }

  case object Overwrite extends WriteMode {
    override val stringValue: String = "overwrite"
    override val sparkMode: SaveMode = SaveMode.Overwrite
  }
  case object OverwriteOnly extends WriteMode {
    override val stringValue: String = "overwrite_only"
    override val sparkMode: SaveMode = SaveMode.Overwrite
  }
  case object CreateOnly extends WriteMode {
    override val stringValue: String = "create"
    override val sparkMode: SaveMode = SaveMode.ErrorIfExists
  }
  case object Append extends WriteMode {
    override val stringValue: String = "append"
    override val sparkMode: SaveMode = SaveMode.Append
  }

  final case class WriteFailuresException(
      failures: Seq[(WriteTable, Throwable)]
  ) extends Exception(
        failures
          .map {
            case (table, cause) =>
              s"${table.source.name}.${table.name} write failed: ${cause.getMessage}"
          }
          .mkString("\n"),
        failures.headOption.map(_._2).orNull
      )

  object WriteMode {
    private val modes: Seq[WriteMode] =
      Seq(Overwrite, OverwriteOnly, CreateOnly, Append)

    val validModesString: String = modes.map(_.stringValue).mkString(", ")

    def from(string: String): Option[WriteMode] = {
      modes.find(_.stringValue == string)
    }
  }

  def apply(
      output: Map[WriteTable, DataFrame],
      countListener: Option[RecordCountListener]
  )(implicit
      spark: SparkSession
  ): Try[(Map[WriteTable, LineageSchema], Map[WriteTable, LineageStatistics])] =
    Try {

      val writes = output.map {
        case (table, df) =>
          (table, apply(df, table, countListener))
      }

      val split = writes.groupBy { case (_, tried) => tried.isSuccess }
      val successes = split.getOrElse(true, Nil).map {
        case (table, tried) => (table, tried.get)
      }
      val failures = split.getOrElse(false, Nil).map {
        case (table, tried) => (table, tried.failed.get)
      }

      if (failures.nonEmpty) {
        val count = failures.size
        logger.error(s"$count write errors")
        throw WriteFailuresException(failures.toSeq)
      }

      val schemaMap = successes.map {
        case (table, (schema, _)) =>
          (table, schema)
      }.toMap

      val statsMap = successes.flatMap {
        case (table, (_, stats)) =>
          stats match {
            case Some(value) => Some((table, value))
            case None        => None
          }
      }.toMap

      (schemaMap, statsMap)
    }

  def apply(
      df: DataFrame,
      table: WriteTable,
      countListener: Option[RecordCountListener]
  )(implicit
      spark: SparkSession
  ): Try[(LineageSchema, Option[LineageStatistics])] =
    Try {
      val tableDbString = s"${table.source.name}.${table.name}"
      logger.info(s"Writing out $tableDbString")

      val mode = WriteMode.from(table.mode) match {
        case Some(value) => value
        case None =>
          throw new IllegalArgumentException(
            s"${table.mode} is not a valid mode for $tableDbString. " +
              s"Valid modes are: ${WriteMode.validModesString}"
          )
      }

      val format      = table.format
      val partitionBy = table.partitionBy.map(lit(_))
      val path        = table.source.locationURI.resolve(table.name).toString
      val writeToDB = table.source.`type` match {
        case JDBC => true
        case _    => false
      }

      logger.info(s"Writing $tableDbString to $path as $format in mode $mode")

      var partial = df.write
        .format(format)
        .option("path", path)

      if (partitionBy.nonEmpty) {
        logger.info(
          s"Partitioning $tableDbString by ${table.partitionBy.mkString(", ")}"
        )
        partial = partial.partitionBy(table.partitionBy: _*)
      }

      logger.debug(s"Starting write for $tableDbString")
      try {
        partial = partial.mode(mode.sparkMode)
        if (writeToDB) {
          partial.saveAsTable(tableDbString)
        } else {
          partial.save()
        }
      } catch {
        case e: Throwable =>
          // reset the count listener on a failure and re-throw
          countListener.foreach(_.reset())
          throw e
      }

      val lineageStats = countListener.flatMap { listener =>
        val writeTasks = listener.totalWriteTasks
        val rowCount   = listener.totalRecordsWritten
        val bytes      = listener.totalBytesWritten
        listener.reset()

        logger.info(
          s"Writing stats for $writeTasks write tasks for $tableDbString (rows=$rowCount, bytes=$bytes)"
        )

        val stats = ExternalCatalogHelper.PreCollectedStats(
          rowCount = Some(rowCount),
          bytesWritten = Some(bytes)
        )

        if (writeToDB) {
          ExternalCatalogHelper.updateStats(
            table.source.name,
            table.name,
            stats
          ) match {
            case Failure(cause) =>
              logger.error(
                s"Failed to write stats to external catalog for $tableDbString",
                cause
              )
            case Success(_) =>
          }
        }

        LineageStatistics.from(stats) match {
          case Some(stats) => Some(stats)
          case _           => None
        }
      }

      val lineageSchema = LineageSchema.from(df)

      (lineageSchema, lineageStats)
    }

}
