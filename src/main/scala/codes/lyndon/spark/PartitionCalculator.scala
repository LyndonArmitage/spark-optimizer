package codes.lyndon.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types.LongType
import org.slf4j.{Logger, LoggerFactory}

import scala.math.{max, min}

/**
  * Utility class for calculating the number of partitions a DataFrame should
  * have based on the average size of the data within it.
  */
object PartitionCalculator {

  @transient
  private[this] val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * 128MiB in Bytes
    */
  val _128MiB: Long = 128L * 1024L * 1024L

  /**
    * Default option values
    */
  object Default {

    /**
      * 100,000 partitions seems reasonable as when combined with the above
      * default we'd be able to handle ~12 terabytes of data.
      * With smaller values it would work relatively okay as well.
      */
    val maxTotalPartitions: Int = 100000

    /**
      * When dealing with smaller data sets a lower limit of 80 partitions should
      * ensure we still get decent parallelism, e.g. machines with 16 cores will
      * be given 5 tasks.
      */
    val minTotalPartitions: Int = 16

    /**
      * Default to the number of available processors
      */
    val executorsPerNode: Int = Runtime.getRuntime.availableProcessors()

    /**
      * Defaults to 1, assuming a single node
      */
    val clusterNodes: Int = 1

    /**
      * A reasonable sample size to use when sampling a DataFrame, generally if
      * a DataFrame is small enough we can sample the whole thing
      */
    val sampleSize: Int = 10000
  }

  /**
    * Recommendations for how to size partitions
    * @param estimatedTotalSize The estimated total size of the data in bytes
    * @param maxSizePerPartitionBytes The rough max size per partition in bytes
    * @param recommendedPartitionCount The recommended count of partitions
    * @param alignedRecommendedPartitionCount The recommended count of
    *                                         partitions aligned with the count
    *                                         of executors
    * @param totalExecutors The total count of executors
    */
  case class PartitionRecommendations(
      estimatedTotalSize: Long,
      maxSizePerPartitionBytes: Long,
      recommendedPartitionCount: Int,
      alignedRecommendedPartitionCount: Int,
      totalExecutors: Int
  )

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
    * @param sampleSize The sample size to use for estimating row sizes
    * @param maxSizePerPartitionBytes The max size per partition,
    *                                 normally good to use something around 128MiB
    * @param maxTotalPartitions The maximum amount of partitions to recommend
    * @param minTotalPartitions The minimum amount of partitions to recommend,
    *                           should be a multiple of the amount of executors generally
    * @param executorsPerNode The number of executors per node in the cluster
    * @param clusterNodes The number of cluster nodes
    * @return The estimated dynamic partition count
    */
  def recommendForDataFrame(
      dataFrame: DataFrame,
      sampleSize: Int = 10000,
      maxSizePerPartitionBytes: Long = _128MiB,
      maxTotalPartitions: Int = Default.maxTotalPartitions,
      minTotalPartitions: Int = Default.minTotalPartitions,
      executorsPerNode: Int = Default.executorsPerNode,
      clusterNodes: Int = Default.clusterNodes
  ): PartitionRecommendations = {

    if (sampleSize < 1)
      throw new IllegalArgumentException(
        s"sampleSize must be greater than 0, was $sampleSize"
      )

    val currentPartitions = dataFrame.rdd.getNumPartitions
    logger.debug(s"Currently there are $currentPartitions partitions")

    // Could potentially use rdd.countApprox if we need this result quick,
    // However countApprox will still use up resources after we get the initial
    // value
    val totalRowCount = dataFrame.count()
    logger.debug(s"$totalRowCount total rows")

    val rowSize = getRowSize(dataFrame, sampleSize, totalRowCount)
    logger.debug(s"Row size is $rowSize")

    recommend(
      totalRowCount,
      rowSize,
      maxSizePerPartitionBytes,
      maxTotalPartitions,
      minTotalPartitions,
      executorsPerNode,
      clusterNodes
    )

  }

  /**
    * Make a recommendation on the amount of partitions that should be used for
    * a given stage of a job based on the amount of data being processed, shape
    * of it and the fixed cluster size.
    *
   * @param totalRowCount The total count of rows
    * @param rowSizeBytes The size of a single row in bytes
    * @param maxSizePerPartitionBytes The max size per partition,
    *                                 normally good to use something around 128MiB
    * @param maxTotalPartitions The maximum amount of partitions to recommend
    * @param minTotalPartitions The minimum amount of partitions to recommend,
    *                           should be a multiple of the amount of executors generally
    * @param executorsPerNode The number of executors per node in the cluster
    * @param clusterNodes The number of cluster nodes
    * @return
    */
  def recommend(
      totalRowCount: Long,
      rowSizeBytes: Long,
      maxSizePerPartitionBytes: Long = _128MiB,
      maxTotalPartitions: Int = Default.maxTotalPartitions,
      minTotalPartitions: Int = Default.minTotalPartitions,
      executorsPerNode: Int = Default.executorsPerNode,
      clusterNodes: Int = Default.clusterNodes
  ): PartitionRecommendations = {
    val totalExecutors: Int = executorsPerNode * clusterNodes
    val totalSize: Long     = totalRowCount * rowSizeBytes

    val recommendedPartitionCount =
      (totalSize / maxSizePerPartitionBytes).toInt

    val newPartitions = max(
      min(
        maxTotalPartitions,
        recommendedPartitionCount
      ),
      minTotalPartitions
    )

    // Adjust to align with executors available
    val alignedNewPartitions = (
      math.ceil(newPartitions.toDouble / totalExecutors.toDouble) 
      * totalExecutors
    ).toInt

    PartitionRecommendations(
      totalSize,
      maxSizePerPartitionBytes,
      newPartitions,
      alignedNewPartitions,
      totalExecutors
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

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      println("Missing arguments:")
      println("arg 1: row count")
      println("arg 2: row size (in bytes)")
      println("arg 3: executors per node")
      println("arg 4: nodes")
      println("e.g. 100000 1024 2 8")
      return
    }

    val rowCount = args(0).toLong
    val rowSize = args(1).toLong
    val executorsPerNode = args(2).toInt
    val clusterNodes = args(3).toInt


    val recommendation = recommend(
      totalRowCount = rowCount,
      rowSizeBytes = rowSize,
      executorsPerNode = executorsPerNode,
      clusterNodes = clusterNodes
    )
    val totalExecutors = executorsPerNode * clusterNodes

    println(s"$rowCount total rows")
    println(s"1 row = $rowSize bytes")
    println(s"total size = ${recommendation.estimatedTotalSize} bytes")
    println(
      s"$clusterNodes nodes each with $executorsPerNode executors for total of " +
        s"$totalExecutors executors"
    )
    println(s"Recommended partition count is ${recommendation.recommendedPartitionCount}")
    println(
      s"Recommended partition count aligned to executors is ${recommendation.alignedRecommendedPartitionCount}"
    )
  }

}
