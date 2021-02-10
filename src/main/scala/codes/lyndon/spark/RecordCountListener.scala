package codes.lyndon.spark

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._

/**
  * Simple Spark Listener that counts the number of written records and bytes.
  *
  * This gives you a reliable count of the records written out without incurring
  * a call to count() them after writing.
  *
  * Additionally you can get individual count information based on the tasks that
  * have executed, this could be useful for judging if writes are skewed.
  */
final case class RecordCountListener() extends SparkListener {

  // We collect as a list to avoid overflowing atomic counts and allow for
  // inspection of individual values, unfortunately it needs to be synchronized
  // since many threads could be writing and reading from it
  private[this] val counter = Collections.synchronizedList(
    new util.ArrayList[MetricEntry](200)
  )
  private[this] val tasksProcessed = new AtomicLong(0)

  /**
    * A Metric entry
    * @param records A count of records written
    * @param bytes The size of bytes written
    * @param executorRunTime The time the executor spent doing the task where
    *                        these records were written
    */
  case class MetricEntry(
      records: Long,
      bytes: Long,
      executorRunTime: Long
  ) {
    override def toString: String =
      s"MetricEntry($records rows, $bytes bytes, $executorRunTime runtime)"
  }

  /**
    * @return The total number of records written at this time
    */
  def totalRecordsWritten: BigInt = {
    counter.synchronized {
      counter.asScala
        .map(_.records)
        .foldLeft(BigInt(0)) {
          case (sum, value) =>
            sum + value
        }
    }
  }

  /**
    * @return The total number of bytes written at this time
    */
  def totalBytesWritten: BigInt = {
    counter.synchronized {
      counter.asScala
        .map(_.bytes)
        .foldLeft(BigInt(0)) {
          case (sum, value) =>
            sum + value
        }
    }
  }

  /**
    * @return The current stored data, useful for performing analysis
    */
  def currentMetrics: Seq[MetricEntry] =
    counter.synchronized {
      counter.asScala.clone()
    }

  /**
    * @return The total count of tasks that have written records
    */
  def totalWriteTasks: Long = tasksProcessed.get()

  /**
    * Reset this listener so all values will return 0
    */
  def reset(): Unit = {
    counter.clear()
    tasksProcessed.set(0)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    if (
      taskEnd != null &&
      taskEnd.taskMetrics != null &&
      taskEnd.taskMetrics.outputMetrics != null
    ) {
      val metrics        = taskEnd.taskMetrics
      val outputMetrics  = metrics.outputMetrics
      val recordsWritten = outputMetrics.recordsWritten
      val bytesWritten   = outputMetrics.bytesWritten

      counter.add(
        MetricEntry(
          recordsWritten,
          bytesWritten,
          metrics.executorRunTime
        )
      )
      tasksProcessed.incrementAndGet()
    }
  }

}
