package codes.lyndon.spark

import org.apache.spark.util.SizeEstimator
import org.scalatest.funsuite.AnyFunSuite

class SizeEstimatorTests extends AnyFunSuite{

  test("sizes match expectations") {
    assert(
      SizeEstimator.estimate(Integer.valueOf(1)) == 16,
      "Integer does not match expectation"
    )
  }

}
