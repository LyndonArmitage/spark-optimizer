package codes.lyndon.spark

import org.apache.spark.sql.functions.lit
import DataFrameEstimator._

class DataFrameEstimatorTest extends SparkSessionFunSpec {

  test("estimation returns expected size") { spark =>
    val estimate = spark
      .range(1)
      .withColumn("foo", lit(1))
      .withColumn("bar", lit("abc"))
      .withColumn("baz", lit(0.1))
      .withColumn("oof", lit(false))
      .estimatedRowSize

    assert(estimate == 41, "Estimation differs")
  }

  test("Estimation and actual size match") { spark =>
    val df = spark
      .range(1)
      .withColumn("foo", lit(1))
      .cache()

    val sampled = df
      .sampleRowSize()
      .select("max")
      .head()
      .getLong(0)

    val estimate = df.estimatedRowSize
    assert(sampled == estimate, "Sample differs from estimate")

  }

}
