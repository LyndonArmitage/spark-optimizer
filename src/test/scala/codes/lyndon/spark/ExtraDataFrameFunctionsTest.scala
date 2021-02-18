package codes.lyndon.spark

import codes.lyndon.spark.ExtraDataFrameFunctions._
import codes.lyndon.spark.test.SparkSessionFunSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, last, when}

import java.sql.Date

class ExtraDataFrameFunctionsTest extends SparkSessionFunSpec {

  test("densifies as expected on simple data") { spark =>
    implicit val sparkSession: SparkSession = spark

    import spark.implicits._

    val df = Seq(
      (Date.valueOf("2020-01-15"), 0),
      (Date.valueOf("2020-01-16"), 1),
      (Date.valueOf("2020-01-17"), 1),
      (Date.valueOf("2020-01-21"), 2),
      (Date.valueOf("2020-02-01"), 5)
    ).toDF("date", "val")

    val dense = df.densify_on_date("date")

    val count = dense.count()
    assert(count == 18, "Wrong count of entries")
  }

  test("densify includes null entries") { spark =>
    implicit val sparkSession: SparkSession = spark

    import spark.implicits._

    val df = Seq(
      (Date.valueOf("2020-01-15"), 0),
      (null, 4),
      (Date.valueOf("2020-02-01"), 5),
      (null, 3)
    ).toDF("date", "val")

    val dense = df.densify_on_date("date")
    dense.cache()

    val count = dense.count()
    assert(count == 20, "Wrong count of entries")
    val nullCount = dense.filter($"date".isNull).count()
    assert(nullCount == 2, "Wrong count of nulls")
    dense.show()
    dense.unpersist()
  }

  test("densify and window") { spark =>
    implicit val sparkSession: SparkSession = spark

    import spark.implicits._

    val df = Seq(
      (Date.valueOf("2020-01-15"), 0),
      (Date.valueOf("2020-01-16"), 1),
      (Date.valueOf("2020-01-17"), 2),
      (Date.valueOf("2020-01-18"), 0),
      (Date.valueOf("2020-01-20"), 1),
      (null, 4),
      (Date.valueOf("2020-02-01"), 5),
      (null, 3)
    ).toDF("date", "val")

    val dense = df.densify_on_date("date")
//
//    val filled = dense.na.fill(0, Seq("val"))
//    filled.explain(true)

    val window = Window
    // Note this windows over all the data as a single partition
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val windowed = dense.withColumn(
      "val",
      when(
        col("val").isNull,
        last("val", ignoreNulls = true).over(window)
      )
        .otherwise(col("val"))
    )
    windowed.explain(true)
    windowed.show()
  }

}
