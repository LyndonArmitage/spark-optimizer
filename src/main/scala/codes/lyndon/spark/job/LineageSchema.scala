package codes.lyndon.spark.job

import codes.lyndon.spark.ExternalCatalogHelper
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

case class LineageSchema(
    fieldsToType: Map[String, String]
)

object LineageSchema {

  def from(df: DataFrame): LineageSchema = {
    from(df.schema)
  }

  def from(schema: StructType): LineageSchema = {
    val fieldsToTypes = schema.fields.map { field =>
      (field.name, field.dataType.sql)
    }.toMap
    LineageSchema(fieldsToTypes)
  }

  def fromCatalog(
      table: Table
  )(implicit sparkSession: SparkSession): Option[LineageSchema] = {
    val catalog     = sparkSession.catalog
    val tableSource = table.source
    tableSource.`type` match {
      case S3FileSystem | LocalFileSystem => None
      case JDBC =>
        if (catalog.tableExists(tableSource.name, table.name)) {
          val schema =
            ExternalCatalogHelper.currentSchema(tableSource.name, table.name)
          Some(from(schema))
        } else {
          None
        }
    }
  }

  def merge(
      a: Map[Table, LineageSchema],
      b: Map[Table, LineageSchema]
  ): Map[Table, List[LineageSchema]] = {
    val mergedSeq = a.toSeq ++ b.toSeq
    mergedSeq.groupBy(_._1).mapValues(_.map(_._2).toList)
  }

  def mergeLatest(
      old: Map[Table, LineageSchema],
      latest: Map[Table, LineageSchema]
  ): Map[Table, LineageSchema] = {
    merge(old, latest).mapValues(_.last)
  }
}
