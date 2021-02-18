package org.apache.spark.sql

import codes.lyndon.spark.test.SharedSparkSessionFunSuite
import codes.lyndon.spark.test.TestHelpers._
import org.apache.spark.sql.LyndonFunctions.dates_between
import org.apache.spark.sql.types.{ArrayType, DateType, StructField, StructType}

import java.sql.Date

class LyndonFunctionsTest extends SharedSparkSessionFunSuite {
  test("works as expected") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df = Seq(
      (Date.valueOf("2020-01-15"), Date.valueOf("2020-01-20")),
      (null, null),
      (Date.valueOf("2020-01-15"), null),
      (null, Date.valueOf("2020-01-20")),
    ).toDF("start", "end")
      .withColumn("between", dates_between($"start", $"end"))

    val expected: DataFrame = Seq(
      Row(
        Date.valueOf("2020-01-15"),
        Date.valueOf("2020-01-20"),
        Seq[Date](
          Date.valueOf("2020-01-15"),
          Date.valueOf("2020-01-16"),
          Date.valueOf("2020-01-17"),
          Date.valueOf("2020-01-18"),
          Date.valueOf("2020-01-19"),
          Date.valueOf("2020-01-20")
        )
      ),
      Row(null, null, null),
      Row(Date.valueOf("2020-01-15"), null, null),
      Row(null, Date.valueOf("2020-01-20"), null)
    ).toDF(
      StructType(
        Seq(
          StructField("start", DateType, nullable = true),
          StructField("end", DateType, nullable = true),
          StructField(
            "between",
            ArrayType(DateType, containsNull = false),
            nullable = true
          )
        )
      )
    )

    assertSmallDataFrameEquality(df, expected)
  }

}
