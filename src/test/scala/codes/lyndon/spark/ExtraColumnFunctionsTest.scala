package codes.lyndon.spark

import codes.lyndon.spark.ExtraColumnFunctions._
import codes.lyndon.spark.test.SparkSessionFunSpec

class ExtraColumnFunctionsTest extends SparkSessionFunSpec {

  test("randLong returns expected number") { spark =>
    val min = 100L
    val max = 9200L
    val seed = 1L

    val number = spark
      .range(1)
      .withColumn("rand", randLong(min, max, seed))
      .drop("id")
      .head()
      .getLong(0)

    assert(number < max, "Should be less than max")
    assert(number >= min, "Should be at least the min")
    assert(number == 8773L, "Should match expected")
  }

  test("randLong does not return out of range") { spark =>
    val min = 50L
    val max = 2000L

    val count = 5000
    val numbers = spark
      .range(count)
      .withColumn("rand", randLong(min, max))
      .drop("id")
      .collect()
      .map {row => row.getLong(0)}
      .toSeq

    numbers.foreach { number =>
      assert(number < max, "Should be less than max")
      assert(number >= min, "Should be at least the min")
    }

  }

}
