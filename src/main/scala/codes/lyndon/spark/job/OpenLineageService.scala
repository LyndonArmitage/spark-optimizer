package codes.lyndon.spark.job

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.openlineage.client.OpenLineage
import org.slf4j.LoggerFactory
import sttp.model.{MediaType, Uri}

import java.net.{InetAddress, URI}
import java.time.ZonedDateTime
import java.util.UUID
import scala.util.Try
import scala.collection.JavaConverters._

class OpenLineageService(
    val namespace: String,
    val apiURI: URI,
    val producer: Option[URI] = None
) extends LineageService {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  private sealed abstract class EventType(val asString: String)
      extends Product
      with Serializable {
    override def toString: String = asString
  }

  private case object StartEvent    extends EventType("START")
  private case object CompleteEvent extends EventType("COMPLETE")
  private case object AbortEvent    extends EventType("ABORT")
  private case object FailEvent     extends EventType("FAIL")
  private case object OtherEvent    extends EventType("OTHER")

  private val openLineage = new OpenLineage(
    producer.getOrElse(URI.create(InetAddress.getLocalHost.toString))
  )
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(new JavaTimeModule())

  override def startJob(
      config: JobConfig,
      runId: UUID,
      eventTime: ZonedDateTime = ZonedDateTime.now()
  ): Try[Unit] = {
    sendEvent(newEvent(config, runId, StartEvent, eventTime))
  }

  override def completeJob(
      config: JobConfig,
      runId: UUID,
      eventTime: ZonedDateTime = ZonedDateTime.now()
  ): Try[Unit] = {
    sendEvent(newEvent(config, runId, CompleteEvent, eventTime))
  }

  override def abortJob(
      config: JobConfig,
      runId: UUID,
      eventTime: ZonedDateTime = ZonedDateTime.now()
  ): Try[Unit] = {
    sendEvent(newEvent(config, runId, AbortEvent, eventTime))
  }

  override def failJob(
      config: JobConfig,
      runId: UUID,
      eventTime: ZonedDateTime = ZonedDateTime.now()
  ): Try[Unit] = {
    sendEvent(newEvent(config, runId, FailEvent, eventTime))
  }

  private def newEvent(
      config: JobConfig,
      runId: UUID,
      eventType: EventType,
      eventTime: ZonedDateTime = ZonedDateTime.now()
  ): OpenLineage.RunEvent = {

    val job     = buildJob(config)
    val run     = buildRun(config, runId)
    val inputs  = buildInputs(config)
    val outputs = buildOutputs(config)

    openLineage.newRunEvent(
      eventType.asString,
      eventTime,
      run,
      job,
      inputs.asJava,
      outputs.asJava
    )
  }

  private def buildJob(config: JobConfig): OpenLineage.Job = {
    openLineage
      .newJobBuilder()
      .namespace(namespace)
      .name(config.jobName)
      // TODO: Facets
      .build()
  }

  private def buildRun(
      config: JobConfig,
      runId: UUID
  ): OpenLineage.Run = {
    openLineage
      .newRunBuilder()
      .runId(runId)
      // TODO: Facets
      .build()
  }

  private def buildInputs(config: JobConfig): Seq[OpenLineage.InputDataset] =
    config.inputs.map(buildInput)

  private def buildInput(table: ReadTable): OpenLineage.InputDataset = {
    val facets = openLineage
      .newDatasetFacetsBuilder()
      .dataSource(dataSourceFor(table))
      // TODO: Schema
      // TODO: Documentation
      .build()
    openLineage
      .newInputDatasetBuilder()
      .namespace(namespace)
      .name(table.name)
      .facets(facets)
      .build()
  }

  private def buildOutputs(config: JobConfig): Seq[OpenLineage.OutputDataset] =
    config.outputs.map(buildOutput)

  private def buildOutput(table: WriteTable): OpenLineage.OutputDataset = {
    val facets = openLineage
      .newDatasetFacetsBuilder()
      .dataSource(dataSourceFor(table))
      // TODO: Schema
      // TODO: Documentation
      .build()
    openLineage
      .newOutputDatasetBuilder()
      .namespace(namespace)
      .name(table.name)
      .facets(facets)
      // TODO: Output stats
      .build()
  }

  private def dataSourceFor(
      table: Table
  ): OpenLineage.DatasourceDatasetFacet = {

    val dataSourceType = table.source.`type` match {
      case S3FileSystem => "S3_FILE"
      case LocalFileSystem => "LOCAL_FILE"
      case JDBC => "DB_TABLE"
    }

    openLineage
      .newDatasourceDatasetFacetBuilder()
      .name(table.name)
      .uri(table.source.locationURI)
      .put("type", dataSourceType)
      .build()
  }

  private def sendEvent(runEvent: OpenLineage.RunEvent): Try[Unit] =
    Try {

      logger.info(s"Sending Event: $runEvent")
      val runEventJson = objectMapper.writeValueAsString(runEvent)

      import sttp.client3._
      val backend = HttpURLConnectionBackend()
      val request = basicRequest
        .post(Uri(apiURI))
        .body(runEventJson)
        .contentType(MediaType.ApplicationJson)

      val response = backend.send(request)
      logger.debug(s"Response: ${response.show()}")

      if (!response.isSuccess) {
        val message = if (response.isServerError) {
          s"Server error: ${response.show()}"
        } else {
          s"Client error: ${response.show()}"
        }
        throw OpenLineageException(message)
      }
    }

  case class OpenLineageException(
      message: String,
      cause: Throwable = null
  ) extends Exception(message, cause)
}
