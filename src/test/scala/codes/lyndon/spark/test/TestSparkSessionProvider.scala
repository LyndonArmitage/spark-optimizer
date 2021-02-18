package codes.lyndon.spark.test

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

trait TestSparkSessionProvider {

  private[this] val logger: Logger = LoggerFactory.getLogger(getClass)

  protected final def createSession(
      appName: String = getClass.getCanonicalName,
      procs: Int = Runtime.getRuntime.availableProcessors(),
      shufflePartitions: Int = 1
  ): SparkSession = {
    logger.debug(s"Creating SparkSession $appName with $procs executors")
    SparkSession
      .builder()
      .master(s"local[$procs]")
      .appName(appName)
      .config("spark.sql.shuffle.partitions", s"$shufflePartitions")
      .getOrCreate()
  }

}
