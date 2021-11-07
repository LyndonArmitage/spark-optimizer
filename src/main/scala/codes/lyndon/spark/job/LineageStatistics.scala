package codes.lyndon.spark.job

import codes.lyndon.spark.ExternalCatalogHelper
import codes.lyndon.spark.ExternalCatalogHelper.PreCollectedStats
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics

import scala.util.Try

/**
  * Simple case class defining the kind stats that are useful for tracking in
  * a Lineage Service
  *
 * @param rowCount The count of rows in a table
  * @param byteSize The total size of a table in bytes
  */
case class LineageStatistics(
    rowCount: Long = 0,
    byteSize: Long = 0
)

object LineageStatistics {

  def from(catalogStats: CatalogStatistics): LineageStatistics = {
    val CatalogStatistics(bytes, rowCount, _) = catalogStats
    LineageStatistics(
      rowCount.map(_.longValue()).getOrElse(-1L),
      bytes.longValue()
    )
  }

  def from(collectedStats: PreCollectedStats): Option[LineageStatistics] = {
    val PreCollectedStats(rows, bytes, _) = collectedStats
    (rows, bytes) match {
      case (None, None) => None
      case _ =>
        Some(
          LineageStatistics(
            rows.map(_.longValue()).getOrElse(-1L),
            bytes.map(_.longValue()).getOrElse(-1L)
          )
        )
    }
  }

  def fromCatalog(
      table: Table
  )(implicit sparkSession: SparkSession): Option[LineageStatistics] = {
    val catalog     = sparkSession.catalog
    val tableSource = table.source
    tableSource.`type` match {
      case S3FileSystem | LocalFileSystem => None
      case JDBC =>
        if (catalog.tableExists(table.name, tableSource.name)) {
          ExternalCatalogHelper
            .currentStats(table.name, tableSource.name)
            .map(from)
        } else {
          None
        }
    }
  }

  def merge(
      a: Map[Table, LineageStatistics],
      b: Map[Table, LineageStatistics]
  ): Map[Table, List[LineageStatistics]] = {
    val mergedSeq = a.toSeq ++ b.toSeq
    mergedSeq.groupBy(_._1).mapValues(_.map(_._2).toList)
  }

  def mergeLatest(
      old: Map[Table, LineageStatistics],
      latest: Map[Table, LineageStatistics]
  ): Map[Table, LineageStatistics] = {
    merge(old, latest).mapValues(_.last)
  }
}
