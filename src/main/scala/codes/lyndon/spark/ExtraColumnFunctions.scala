package codes.lyndon.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{floor, lit, rand}
import org.apache.spark.sql.types.LongType

/**
  * Extra useful Spark Column functions
  */
object ExtraColumnFunctions {

  /**
    * Generate a random Long number
    *
    * @param minInclusive The minimum long number inclusive
    * @param maxExclusive The maximum long number exclusive
    * @param seed A seed to use for the RNG
    * @return A column containing a random number
    */
  def randLong(
      minInclusive: Long,
      maxExclusive: Long,
      seed: Long
  ): Column = randLong(lit(minInclusive), lit(maxExclusive), seed)

  /**
    * Generate a random Long number
    *
   * @param minInclusive The minimum long number inclusive
    * @param maxExclusive The maximum long number exclusive
    * @param seed A seed to use for the RNG
    * @return A column containing a random number
    */
  def randLong(
      minInclusive: Column,
      maxExclusive: Column,
      seed: Long
  ): Column = {
    val multiplier = maxExclusive - minInclusive
    floor((rand(seed) * multiplier) + minInclusive).cast(LongType)
  }

  /**
    * Generate a random Long number
    *
    * @param minInclusive The minimum long number inclusive
    * @param maxExclusive The maximum long number exclusive
    * @return A column containing a random number
    */
  def randLong(
      minInclusive: Long,
      maxExclusive: Long
  ): Column = randLong(lit(minInclusive), lit(maxExclusive))

  /**
    * Generate a random Long number
    *
    * @param minInclusive The minimum long number inclusive
    * @param maxExclusive The maximum long number exclusive
    * @return A column containing a random number
    */
  def randLong(
      minInclusive: Column,
      maxExclusive: Column
  ): Column = {
    val multiplier = maxExclusive - minInclusive
    floor((rand() * multiplier) + minInclusive).cast(LongType)
  }

}
