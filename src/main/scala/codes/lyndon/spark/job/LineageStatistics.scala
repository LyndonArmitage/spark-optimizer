package codes.lyndon.spark.job

import codes.lyndon.spark.ExternalCatalogHelper.PreCollectedStats
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics

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

}
