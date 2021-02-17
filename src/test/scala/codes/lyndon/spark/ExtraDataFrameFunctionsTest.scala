package codes.lyndon.spark

import org.apache.spark.sql.LyndonFunctions.dates_between
import org.apache.spark.sql.SparkSession

import codes.lyndon.spark.ExtraDataFrameFunctions._

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

}
