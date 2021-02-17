package org.apache.spark.sql

import codes.lyndon.spark.SparkSessionFunSpec
import org.apache.spark.sql.LyndonFunctions.dates_between

import java.sql.Date

class LyndonFunctionsTest extends SparkSessionFunSpec {

  test("works as expected") { spark =>
    implicit val sparkSession: SparkSession = spark
    import spark.implicits._

    val df = Seq(
      (Date.valueOf("2020-01-15"), Date.valueOf("2020-01-20")),
      (null, null)
    ).toDF("start", "end")
      .withColumn("between", dates_between($"start", $"end"))

    df.show()
    df.explain(true)

  }

}
