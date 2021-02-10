package codes.lyndon.spark

import org.apache.spark.sql.SaveMode

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

    val tempDir = makeTempDir("countCorrect")
    val tempPath = tempDir.resolve("test")
    val count = 199
    spark
      .range(count)
      .write
      .mode(SaveMode.Overwrite)
      .save(tempPath.toFile.getAbsolutePath)

    assert(listener.totalRecordsWritten === count)
  }

}
