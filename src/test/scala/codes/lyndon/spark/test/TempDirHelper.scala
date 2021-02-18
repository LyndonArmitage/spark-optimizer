package codes.lyndon.spark.test

import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable

trait TempDirHelper {

  private[this] final val logger: Logger = LoggerFactory.getLogger(getClass)

  protected final val tempDirs: mutable.ListBuffer[Path] =
    mutable.ListBuffer[Path]()

  protected final def makeTempDir(name: String): Path = {
    val dir = Files.createTempDirectory(name)
    tempDirs.append(dir)
    dir
  }

  protected final def deleteRecursive(path: Path): Unit = {
    import java.nio.file.Files
    try {
      Files
        .walk(path)
        .iterator()
        .asScala
        .toSeq
        .reverse
        .foreach { path =>
          logger.trace(s"Deleting $path")
          Files.deleteIfExists(path)
        }
    } catch {
      case e: Exception =>
        logger.error(
          s"Could not delete recursive path: ${path.toFile.getAbsolutePath}",
          e
        )
    }
  }

  protected final def deleteTempDirs(): Unit = {
    if (tempDirs.nonEmpty) {
      logger.debug(s"Deleting $tempDirs temporary directories")
      tempDirs.foreach(deleteRecursive)
    }
  }

}
