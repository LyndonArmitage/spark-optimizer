package codes.lyndon.spark

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{
  AnalyzePartitionCommand,
  AnalyzeTableCommand
}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try

/**
  * Shortcut API for analyzing tables saved in Spark similar to the
  * `ANALYZE TABLE` SQL commands.
  *
  * The default apply will analyze both the table overall and it's partitions if
  * it has any.
  *
  * It is important that the external Spark catalog is set up correctly for
  * these analysis to work.
  */
object TableAnalyzer {

  /**
    * Analyze a table similar to the SQL
    * `ANALYSE TABLE database.table PARTITION COMPUTE STATISTICS;`
    *
    * @param database The database
    * @param table The table name
    * @param noscan Same as in the SQL command. If true will only scan file size
    *               and not row counts. Default is true
    * @param spark The Spark session
    * @return The result of the analysis wrapped in a Try
    */
  def apply(
      database: String,
      table: String,
      noscan: Boolean = true
  )(implicit
      spark: SparkSession
  ): Try[Seq[Row]] = {
    val tableId = TableIdentifier(table, Some(database))
    apply(tableId, noscan)
  }

  /**
    * Analyze a table similar to the SQL
    * `ANALYSE TABLE database.table PARTITION COMPUTE STATISTICS;`
    *
    * @param identifier The table identifier
    * @param noscan Same as in the SQL command. If true will only scan file size
    *               and not row counts. Default is true
    * @param spark The Spark session
    * @return The result of the analysis wrapped in a Try
    */
  def apply(
      identifier: TableIdentifier,
      noscan: Boolean
  )(implicit
      spark: SparkSession
  ): Try[Seq[Row]] =
    for {
      analysis   <- analyzeTable(identifier, noscan)
      partitions <- analyzePartitions(identifier, noscan)
    } yield analysis ++ partitions

  /**
    * Analyze a table similar to the SQL
    * `ANALYSE TABLE database.table COMPUTE STATISTICS;`
    *
    * @param database The database
    * @param table The table name
    * @param noscan Same as in the SQL command. If true will only scan file size
    *               and not row counts. Default is true
    * @param spark The Spark session
    * @return The result of the analysis wrapped in a Try
    */
  def analyzeTable(
      database: String,
      table: String,
      noscan: Boolean = true
  )(implicit
      spark: SparkSession
  ): Try[Seq[Row]] =
    analyzeTable(TableIdentifier(table, Some(database)), noscan)

  /**
    * Analyze a table similar to the SQL
    * `ANALYSE TABLE database.table COMPUTE STATISTICS;`
    *
    * @param identifier The table identifier
    * @param noscan Same as in the SQL command. If true will only scan file size
    *               and not row counts. Default is true
    * @param spark The Spark session
    * @return The result of the analysis wrapped in a Try
    */
  def analyzeTable(
      identifier: TableIdentifier,
      noscan: Boolean
  )(implicit
      spark: SparkSession
  ): Try[Seq[Row]] =
    Try {
      val cmd = AnalyzeTableCommand(
        identifier,
        noscan = noscan
      )
      cmd.run(spark)
    }

  /**
    * Analyze a table similar to the SQL
    * `ANALYSE TABLE database.table PARTITION COMPUTE STATISTICS;`
    *
    * @param database The database
    * @param table The table name
    * @param noscan Same as in the SQL command. If true will only scan file size
    *               and not row counts. Default is true
    * @param spark The Spark session
    * @return The result of the analysis wrapped in a Try
    */
  def analyzePartitions(
      database: String,
      table: String,
      noscan: Boolean = true
  )(implicit
      spark: SparkSession
  ): Try[Seq[Row]] =
    analyzePartitions(TableIdentifier(table, Some(database)), noscan)

  /**
    * Analyze a table similar to the SQL
    * `ANALYSE TABLE database.table PARTITION COMPUTE STATISTICS;`
    *
    * @param identifier The table identifier
    * @param noscan Same as in the SQL command. If true will only scan file size
    *               and not row counts. Default is true
    * @param spark The Spark session
    * @return The result of the analysis wrapped in a Try
    */
  def analyzePartitions(
      identifier: TableIdentifier,
      noscan: Boolean
  )(implicit
      spark: SparkSession
  ): Try[Seq[Row]] =
    Try {
      val tableMeta = spark.sessionState.catalog.getTableMetadata(identifier)
      val partitionSpec =
        tableMeta.partitionColumnNames.map(k => k -> None).toMap
      val cmd = AnalyzePartitionCommand(identifier, partitionSpec, noscan)
      cmd.run(spark)
    }

}
