package codes.lyndon.spark.test

import org.apache.spark.sql.SparkSession
import org.scalatest.Outcome
import org.scalatest.funsuite.FixtureAnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

abstract class SparkSessionFunSpec
    extends FixtureAnyFunSuite
    with TempDirHelper
    with TestSparkSessionProvider {

  protected final val logger: Logger = LoggerFactory.getLogger(getClass)

  type FixtureParam = SparkSession

  override protected def withFixture(test: OneArgTest): Outcome = {
    val session = createSession()
    try {
      withFixture(test.toNoArgTest(session))
    } finally {
      deleteTempDirs()
      logger.debug("Closing SparkSession")
      session.close()
    }
  }

}
