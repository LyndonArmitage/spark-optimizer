package codes.lyndon.spark

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{AnalyzePartitionCommand, AnalyzeTableCommand}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try

/**
  * Shortcut API for initializing analysis on tables
  */
object TableAnalyzer {

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

  def analyzeTable(
      database: String,
      table: String,
      noscan: Boolean = true
  )(implicit
      spark: SparkSession
  ): Try[Seq[Row]] =
    analyzeTable(TableIdentifier(table, Some(database)), noscan)

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

  def analyzePartitions(
      database: String,
      table: String,
      noscan: Boolean = true
  )(implicit
      spark: SparkSession
  ): Try[Seq[Row]] =
    analyzePartitions(TableIdentifier(table, Some(database)), noscan)

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
