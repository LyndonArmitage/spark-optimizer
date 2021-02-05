package codes.lyndon.spark

case class DynamicPartitionOptions(
    maxSizePerPartition: Long =
      DynamicPartitionOptions.Default.maxSizePerPartition,
    maxTotalPartitions: Int =
      DynamicPartitionOptions.Default.maxTotalPartitions,
    minTotalPartitions: Int =
      DynamicPartitionOptions.Default.minTotalPartitions,
    executorsPerNode: Int = DynamicPartitionOptions.Default.executorsPerNode,
    clusterNodes: Int = DynamicPartitionOptions.Default.clusterNodes
)

object DynamicPartitionOptions {

  /**
    * 128MiB in Bytes
    */
  val _128MiB: Long = 128L * 1024L * 1024L

  /**
    * Default option values
    */
  object Default {

    /**
      * Approximately 128 Megabytes
      */
    val maxSizePerPartition: Long = _128MiB

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
    val minTotalPartitions: Int = 80

    /**
      * Default to the number of available processors
      */
    val executorsPerNode: Int = Runtime.getRuntime.availableProcessors()

    /**
      * Defaults to 1, assuming a single node
      */
    val clusterNodes: Int = 1
  }

}
