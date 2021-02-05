package codes.lyndon.spark

case class DynamicPartitionOptions(
    maxSizePerPartition: Option[Long] = Some(DynamicPartitionDefaults.maxSizePerPartition),
    maxTotalPartitions: Option[Int] = Some(DynamicPartitionDefaults.maxTotalPartitions),
    minTotalPartitions: Option[Int] = Some(DynamicPartitionDefaults.minTotalPartitions),
    executorsPerNode: Option[Int] = Some(8),
    clusterNodes: Option[Int] = Some(1)
)

object DynamicPartitionDefaults {

  // Approximately 128 Megabytes
  val maxSizePerPartition: Long = 128 * 1000 * 1000

  // 100,000 partitions seems reasonable as when combined with the above
  // default we'd be able to handle ~12 terabytes of data.
  // With smaller values it would work relatively okay as well.
  val maxTotalPartitions: Int = 100000

  // When dealing with smaller data sets a lower limit of 80 partitions should
  // ensure we still get decent parallelism, e.g. machines with 16 cores will
  // be given 5 tasks
  val minTotalPartitions: Int = 80

}
