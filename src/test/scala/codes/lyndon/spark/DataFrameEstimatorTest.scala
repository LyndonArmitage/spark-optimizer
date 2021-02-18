package codes.lyndon.spark

import codes.lyndon.spark.DataFrameEstimator._
import codes.lyndon.spark.test.SharedSparkSessionFunSuite
import org.apache.spark.sql.functions.lit

class DataFrameEstimatorTest extends SharedSparkSessionFunSuite {

  test("estimation returns expected size") {
    val estimate = spark
      .range(1)
      .withColumn("foo", lit(1))
      .withColumn("bar", lit("abc"))
      .withColumn("baz", lit(0.1))
      .withColumn("oof", lit(false))
      .estimatedRowSize

    assert(estimate == 77, "Estimation differs")
  }

  test("Estimation and actual size match") {
    val df = spark
      .range(1)
      .withColumn("foo", lit(1))
      .cache()

    val sampled = df.sampleRowSize()
    sampled.show(false)

    val sampledSize = sampled
      .select("max")
      .head()
      .getLong(0)

    val estimate = df.estimatedRowSize
    assert(sampledSize == estimate, "Sample differs from estimate")

    df.unpersist()
  }

  test("Estimate and sample within range of each other") {
    val df = spark
      .range(1)
      .withColumn("foo", lit(1))
      .withColumn("bar", lit(0.1))
      .withColumn("str", lit("str"))
      .withColumn("oof", lit(false))
      .cache()

    val sampled = df.sampleRowSize()
    sampled.show(false)

    val sampledSize = sampled
      .select("max")
      .head()
      .getLong(0)

    val estimate = df.estimatedRowSize

    assert(sampledSize != estimate, "Sample should differ from estimate")
    assertWithinOrderOfMagnitudes(sampledSize, estimate)
    df.unpersist()
  }

  test("Estimate with hint and sample match") {
    val df = spark
      .range(1)
      .withColumn("str", lit("str"))
      .select("str")
      .cache()

    val sampled = df.sampleRowSize()
    sampled.show(false)

    val sampledSize = sampled
      .select("max")
      .head()
      .getLong(0)

    val estimate = df.estimatedRowSize(
      "str" -> SizeHint.LengthHint(3)
    )

    assert(sampledSize == estimate, "Sample should match")
    assertWithinOrderOfMagnitudes(sampledSize, estimate)
    df.unpersist()
  }

  private[this] def assertWithinOrderOfMagnitudes(
      actual: Long,
      expected: Long
  ): Unit = {
    assert(
      actual.toString.length == expected.toString.length,
      "Should be within order of magnitude"
    )
  }

}
