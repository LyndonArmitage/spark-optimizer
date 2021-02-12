package codes.lyndon.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{
  CatalogColumnStat,
  CatalogStatistics
}
import org.apache.spark.sql.execution.command.CommandUtils
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Helper for manipulating Spark's external catalog.
  *
 * Primarily useful for providing statistics for an table you have just written
  * and know some information about without having to run a full analysis.
  */
object ExternalCatalogHelper {

  @transient
  private[this] val logger = LoggerFactory.getLogger(getClass)

  def currentStats(database: String, table: String)(implicit
      spark: SparkSession
  ): Option[CatalogStatistics] = {
    val catalog   = spark.sharedState.externalCatalog
    val tableData = catalog.getTable(database, table)
    tableData.stats
  }

  /**
    * Case class containing various pre-collected statistics for an external
    * catalog
    *
    * @param rowCount The total count of rows
    * @param bytesWritten The total bytes written
    * @param columnStats Any additional column stats
    */
  case class PreCollectedStats(
      rowCount: Option[BigInt] = None,
      bytesWritten: Option[BigInt] = None,
      columnStats: Map[String, CatalogColumnStat] = Map.empty
  )

  def updateStats(
      database: String,
      table: String,
      stats: PreCollectedStats = PreCollectedStats()
  )(implicit spark: SparkSession): Try[Unit] =
    Try {
      val PreCollectedStats(rowCount, bytesWritten, columnStats) = stats
      val catalog                                                = spark.sharedState.externalCatalog
      if (catalog.getDatabase(database).locationUri == null) {
        // Warn about badly setup database and exit early
        throw new IllegalArgumentException(
          s"Missing Database location URI for Database: $database"
        )
      }
      logger.info(s"Updating External Catalog Stats for $database.$table")

      val sessionState     = spark.sessionState
      val tableIdentWithDB = TableIdentifier(table, Some(database))
      val tableData        = catalog.getTable(database, table)
      val tableMeta        = sessionState.catalog.getTableMetadata(tableIdentWithDB)
      val previousStats    = tableData.stats

      val sizeInBytes = bytesWritten.getOrElse(
        CommandUtils.calculateTotalSize(spark, tableMeta)
      )
      logger.debug(s"$database.$table is $sizeInBytes bytes large")

      val oldSizeMatches =
        previousStats.exists(old => old.sizeInBytes == sizeInBytes)

      // If we have an old row count and the size has not changed then assume
      // the row count is accurate so we don't lose that metadata
      val actualRowCount: Option[BigInt] = if (oldSizeMatches) {
        rowCount.orElse { previousStats.flatMap { old => old.rowCount } }
      } else {
        rowCount
      }

      // If the size hasn't changed and we have column stats we can also try
      // to preserve these
      val actualColStats: Map[String, CatalogColumnStat] = if (oldSizeMatches) {
        combineColStats(
          previousStats.map(_.colStats).getOrElse(Map.empty),
          columnStats
        )
      } else {
        columnStats
      }

      val newStats = CatalogStatistics(
        sizeInBytes = sizeInBytes,
        rowCount = actualRowCount,
        colStats = actualColStats
      )

      logger.trace(s"Resolved stats for $database.$table: $newStats")
      // Finally alter the actual stats, this could fail hence the Try
      catalog.alterTableStats(database, table, Some(newStats))
    }

  private[this] def combineColStats(
      previous: Map[String, CatalogColumnStat],
      updated: Map[String, CatalogColumnStat]
  ): Map[String, CatalogColumnStat] = {
    if (previous.isEmpty) return updated
    if (updated.isEmpty) return previous
    logger.trace(s"Merging column stats: $previous and $updated")

    val merged = previous.toSeq ++ updated.toSeq
    merged
      .groupBy(_._1)
      .mapValues(_.map(_._2).last) // use the updated value for any clashes
  }
}
