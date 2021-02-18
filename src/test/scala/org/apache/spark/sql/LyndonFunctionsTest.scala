package org.apache.spark.sql

import codes.lyndon.spark.test.{SharedSparkSessionFunSuite, SparkSessionFunSpec}
import org.apache.spark.sql.LyndonFunctions.dates_between

import java.sql.Date

class LyndonFunctionsTest extends SharedSparkSessionFunSuite {
  test("works as expected") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df = Seq(
      (Date.valueOf("2020-01-15"), Date.valueOf("2020-01-20")),
      (null, null)
    ).toDF("start", "end")
      .withColumn("between", dates_between($"start", $"end"))

    df.show()
    df.explain(true)

  }

}
