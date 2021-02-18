package codes.lyndon.spark

import codes.lyndon.spark.test.SparkSessionFunSpec
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.CommandUtils

class RecordCountListenerTest extends SparkSessionFunSpec {

  test("counts 0 for nothing written") { spark =>
    val listener = RecordCountListener()
    spark.sparkContext.addSparkListener(listener)

    spark
      .range(100)
      .withColumnRenamed("1", "row")
      .count()

    assert(listener.totalRecordsWritten === 0)
  }

  test("counts correct") { spark =>
    val listener = RecordCountListener()
    spark.sparkContext.addSparkListener(listener)

    val tempDir  = makeTempDir("countCorrect")
    val tempPath = tempDir.resolve("test")
    val count    = 199
    spark
      .range(count)
      .write
      .mode(SaveMode.Overwrite)
      .save(tempPath.toFile.getAbsolutePath)

    assert(listener.totalRecordsWritten === count)
  }

  test("size matches estimate") { spark =>
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types.StringType

    val listener = RecordCountListener()
    spark.sparkContext.addSparkListener(listener)
    assert(listener.totalRecordsWritten === 0)

    val tempDir  = makeTempDir("sizeMatch")
    val tempPath = tempDir.resolve("test")

    val db    = spark.catalog.currentDatabase
    val table = "test_size"
    val count = 299

    spark
      .range(count)
      .withColumn("foo", sha1($"id".cast(StringType)))
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("path", tempPath.toFile.getAbsolutePath)
      .saveAsTable(s"$db.$table")

    spark.sparkContext.removeSparkListener(listener)

    assert(listener.totalRecordsWritten === count, "Wrong count of records")
    val totalBytesWritten = listener.totalBytesWritten
    assert(totalBytesWritten > 0, "Should have written some bytes")

    val sessionState     = spark.sessionState
    val tableIdentWithDB = TableIdentifier(table, Some(db))
    val tableMeta        = sessionState.catalog.getTableMetadata(tableIdentWithDB)
    val estimatedBytesWritten =
      CommandUtils.calculateTotalSize(spark, tableMeta)

    assert(
      estimatedBytesWritten === totalBytesWritten,
      "Estimated bytes written should match reality"
    )

  }

}
