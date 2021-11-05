package codes.lyndon.spark.job

import java.net.URI
import java.nio.file.Paths

case class DataSource(
    name: String,
    `type`: DataSourceType,
    location: String
) {
  def locationURI: URI = {
    `type` match {
      case S3FileSystem => URI.create(location)
      case LocalFileSystem => Paths.get(location).toUri
      case JDBC => URI.create(location)
    }
  }
}

sealed trait DataSourceType

case object S3FileSystem    extends DataSourceType
case object LocalFileSystem extends DataSourceType
case object JDBC            extends DataSourceType
