package codes.lyndon.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types.LongType
import org.slf4j.{Logger, LoggerFactory}

import scala.math.{max, min}

/**
  * Utility class for calculating the number of partitions a DataFrame should
  * have based on the average size of the data within it.
  */
object DynamicPartitionCalculator {

  private[this] val logger: Logger = LoggerFactory.getLogger(getClass)

  case class PartitionRecommendations(
      estimatedTotalSize: BigInt,
      maxSizePerPartition: Long,
      recommendedPartitionCount: Int,
      alignedRecommendedPartitionCount: Int
  )

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
  ): PartitionRecommendations =
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
    * @param sampleSize The sample size to use for estimating row sizes
    * @return The estimated dynamic partition count
    */
  def guessPartitionCount(
      dataFrame: DataFrame,
      options: DynamicPartitionOptions,
      sampleSize: Int = 10000
  ): PartitionRecommendations = {
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

    val rowSize = getRowSize(dataFrame, sampleSize, totalRowCount)

    recommend(
      totalRowCount,
      rowSize,
      maxSizePerPartition,
      maxTotalPartitions,
      minTotalPartitions,
      executorsPerNode,
      clusterNodes
    )

  }

  def recommend(
      totalRowCount: Long,
      rowSize: Long,
      maxSizePerPartition: Long,
      maxTotalPartitions: Int,
      minTotalPartitions: Int,
      executorsPerNode: Int,
      clusterNodes: Int
  ): PartitionRecommendations = {
    val totalExecutors = executorsPerNode * clusterNodes
    val totalSize      = BigInt(totalRowCount) * rowSize

    val recommendedPartitionCount =
      (totalSize / maxSizePerPartition).toInt

    val newPartitions = max(
      min(
        maxTotalPartitions,
        recommendedPartitionCount
      ),
      minTotalPartitions
    )

    // Adjust to align with executors available
    val unusedExecutors      = newPartitions % totalExecutors
    val alignedNewPartitions = newPartitions + unusedExecutors

    PartitionRecommendations(
      totalSize,
      maxSizePerPartition,
      newPartitions,
      alignedNewPartitions
    )
  }

  private def getRowSize(
      df: DataFrame,
      sampleSize: Int,
      rowCount: Long
  ): Long = {
    import DataFrameEstimator._
    import df.sparkSession.implicits._
    val rowSizeDf = df.sampleRowSize(sampleSize, rowCount)
    rowSizeDf.select($"max".cast(LongType)).head().getLong(0)
  }

}
