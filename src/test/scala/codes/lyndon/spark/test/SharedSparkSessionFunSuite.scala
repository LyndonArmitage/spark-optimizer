package codes.lyndon.spark.test

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

abstract class SharedSparkSessionFunSuite
    extends AnyFunSuite
    with BeforeAndAfterAll
    with TempDirHelper
    with TestSparkSessionProvider {
  protected final val logger: Logger = LoggerFactory.getLogger(getClass)

  protected implicit var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = createSession()
  }

  override protected def afterAll(): Unit = {
    spark.close()
    deleteTempDirs()
  }
}
