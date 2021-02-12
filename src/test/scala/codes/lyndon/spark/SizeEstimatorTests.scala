package codes.lyndon.spark

import org.apache.spark.util.SizeEstimator
import org.scalatest.funsuite.AnyFunSuite

class SizeEstimatorTests extends AnyFunSuite {

  test("sizes match expectations") {
    assert(
      SizeEstimator.estimate(Integer.valueOf(1)) == 16,
      "Integer does not match expectation"
    )
  }

  test("String sizes match") {

    // Test to figure out the pattern in size estimation of strings
    // Looks like they are 40+ (length rounded up to nearest multiple of 8 bytes)

    val sizesMap: Map[String, Int] = Map(
      ""    -> 40,
      "1"   -> 48,
      "str" -> 48,
      "abcdefg" -> 48,
      "a" * 7 -> 48,
      "a" * 8 -> 48,
      "a" * 9 -> 56,
      "a" * 10 -> 56,
      "a" * 15 -> 56,
      "a" * 16 -> 56,
      "a" * 32 -> 72,
      "a" * 128 -> 168,
      "a" * 254 -> 296,
      "a" * 255 -> 296,
      "a" * 256 -> 296
    )

    val results = sizesMap.map {
      case (str, expected) =>
        val actual = SizeEstimator.estimate(str)
        (str, expected, actual, expected == actual)
    }

    val failures = results
      .filterNot(_._4)
    if (failures.nonEmpty) {
      fail(
        failures
          .map {
            case (str, expected, actual, _) =>
              s"$str should be of length $expected was $actual"
          }
          .mkString(
            "Failures:\n",
            "\n",
            ""
          )
      )
    }
  }

}
