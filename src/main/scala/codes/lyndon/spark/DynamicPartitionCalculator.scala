package codes.lyndon.spark

import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.SizeEstimator
import org.slf4j.{Logger, LoggerFactory}

import scala.math.BigDecimal.RoundingMode
import scala.math.{ceil, max, min}

/**
  * Utility class for calculating the number of partitions a DataFrame should
  * have based on the average size of the data within it.
  */
object DynamicPartitionCalculator {

  private[this] val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * Calculate the partitions required for a given DataFrame using the given
    * options. This method invokes [[guessPartitionCount()]] see that method
    * for more details on how this is achieved.
    *
   * @param dataFrame The DataFrame to calculate the partitions count for
    * @param options The options to use
    * @return The estimated dynamic partition count
    */
  def apply(
      dataFrame: DataFrame,
      options: DynamicPartitionOptions = DynamicPartitionOptions()
  ): Int =
    guessPartitionCount(dataFrame, options)

  /**
    * <p>
    * Calculate the partitions required for a given DataFrame using the given
    * options.
    * </p>
    * <p>
    * This method involves getting a count of all rows, sampling some rows for
    * their size and attempting to fit a number of them within each partition
    * based on the options given.
    * </p>
    * <p>
    * <b>Note:</b> This method assumes all data within the DataFrame given will
    * be partitioned evenly so will not work well if your data is heavily skewed.
    * </p>
    *
   * @param dataFrame The DataFrame to calculate the partitions count for
    * @param options The options to use
    * @return The estimated dynamic partition count
    */
  def guessPartitionCount(
      dataFrame: DataFrame,
      options: DynamicPartitionOptions
  ): Int = {
    // Convert the given user options into our own internal vars for use
    val DynamicPartitionOptions(
      maxSizePerPartition,
      maxTotalPartitions,
      minTotalPartitions,
      executorsPerNode,
      clusterNodes
    ) = options

    val currentPartitions = dataFrame.rdd.getNumPartitions
    logger.debug(s"Currently there are $currentPartitions partitions")

    logger.trace(s"Calculating row count")
    // Could potentially use rdd.countApprox if we need this result quick,
    // However countApprox will still use up resources after we get the initial
    // value
    val totalRowCount = dataFrame.count()
    logger.debug(s"$totalRowCount total rows")
    // TODO: Consider caching the DataFrame

    val sampleFraction = calculateSampleFraction(totalRowCount)
    val sampleCount    = totalRowCount * sampleFraction
    val rowSize        = getAverageRowSize(dataFrame, sampleFraction)
    logger.debug(
      s"With a sample fraction of $sampleFraction ($sampleCount) the average row is $rowSize bytes"
    )

    val totalSize               = BigDecimal(totalRowCount) * rowSize
    val sizePerCurrentPartition = (totalSize / currentPartitions).toLong
    logger.debug(
      s"Currently $currentPartitions partitions should be roughly " +
        s"$sizePerCurrentPartition bytes each (assuming no skew)"
    )

    val recommendedPartitionCount =
      (totalSize / maxSizePerPartition).setScale(0, RoundingMode.CEILING).toInt

    logger.debug(
      s"$recommendedPartitionCount Recommended partitions to achieve " +
        s"partitions $maxSizePerPartition bytes in size"
    )

    val newPartitions = max(
      min(
        maxTotalPartitions,
        recommendedPartitionCount
      ),
      minTotalPartitions
    )

    logger.debug(
      s"Will use $newPartitions partitions (adjusted for min and max)"
    )
    newPartitions
  }

  private def calculateSampleFraction(
      totalRowCount: Long,
      sampleRange: (Double, Double) = (0.01, 1.0),
      rowCountRange: (Long, Long) = (1000000, 10000000)
  ): Double = {
    // use math.min and math.max here to ensure that if we ever put the ranges the wrong
    // way round it doesn't matter.

    // The basic idea is that we map the increasing row count to a decreasing
    // sample fraction.
    val lowerBound      = min(rowCountRange._1, rowCountRange._2)
    val outerLowerBound = max(rowCountRange._1, rowCountRange._2)

    val minSampleSize = min(sampleRange._1, sampleRange._2)
    val maxSampleSize = max(sampleRange._1, sampleRange._2)

    if (totalRowCount <= lowerBound) {
      // There are so few rows we can sample them all for accuracy
      maxSampleSize
    } else if (totalRowCount > outerLowerBound) {
      minSampleSize
    } else {
      // Simple linear relationship between the large range and small range
      // https://rosettacode.org/wiki/Map_range
      // The idea is to smooth out the boundary
      maxSampleSize +
        (totalRowCount - lowerBound) *
          (minSampleSize - maxSampleSize) / (outerLowerBound - lowerBound)
    }
  }

  private def getAverageRowSize(
      combinedDataFrame: DataFrame,
      sampleFraction: Double
  ): Long = {
    import combinedDataFrame.sparkSession.implicits._

    val rowsSizes = combinedDataFrame
      .sample(sampleFraction) // Limit the rows we check to be a small sample
      .mapPartitions(
        // Calculate each row's size in bytes
        // We do this in the mapPartitions method to avoid any shuffling at this
        // stage
        rows => rows.map(row => getRowSizeInBytes(row))
      )
    // Get the average row size from the sample we have been using
    val agg   = rowsSizes.agg(avg(col(rowsSizes.columns.head)))
    val value = agg.head().getDouble(0)
    // Round up the result to be conservative in the estimates
    ceil(value).toLong
  }

  /**
    * Get the size of a row in bytes
    * @param row The row to get the size of
    * @return The row size in bytes.
    *         This may not be wholly accurate depending upon the types used
    *         within the row.
    *         It also does not take into account any kind of compression that
    *         may happen at read/write level (e.g. snappy compression in Parquet).
    */
  private def getRowSizeInBytes(row: Row): Long = {
    var total = 0L
    for (i <- 0 until row.length) {
      val raw = row.get(i)

      try {
        val item = raw.asInstanceOf[AnyRef]
        val size = SizeEstimator.estimate(item)
        total += size
      } catch {
        case e: Exception =>
          logger.warn(s"Could not cast $raw to AnyRef", e)
          // TODO: Should we use some better heuristic of our own to guess a size here?
          // this is a cheap and cheerful approximation of size
          // Complex types without decent toString methods may be undervalued
          // and those with descriptive toString methods may be overvalued
          raw.toString.getBytes.length
      }
    }
    total
  }

}
