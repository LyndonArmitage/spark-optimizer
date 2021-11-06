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
    val apiURI: URI = URI.create("http://localhost:5000/api/v1/lineage"),
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
      eventTime: ZonedDateTime = ZonedDateTime.now(),
      lineageStats: Map[Table, LineageStatistics] = Map.empty,
      tableSchemas: Map[Table, LineageSchema] = Map.empty
  ): Try[Unit] = {
    sendEvent(newEvent(config, runId, StartEvent, eventTime, lineageStats, tableSchemas))
  }

  override def completeJob(
      config: JobConfig,
      runId: UUID,
      eventTime: ZonedDateTime = ZonedDateTime.now(),
      lineageStats: Map[Table, LineageStatistics] = Map.empty,
      tableSchemas: Map[Table, LineageSchema] = Map.empty
  ): Try[Unit] = {
    sendEvent(newEvent(config, runId, CompleteEvent, eventTime, lineageStats, tableSchemas))
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
      eventTime: ZonedDateTime = ZonedDateTime.now(),
      lineageStats: Map[Table, LineageStatistics] = Map.empty,
      tableSchemas: Map[Table, LineageSchema] = Map.empty
  ): OpenLineage.RunEvent = {

    val job     = buildJob(config)
    val run     = buildRun(config, runId)
    val inputs  = buildInputs(config, lineageStats, tableSchemas)
    val outputs = buildOutputs(config, lineageStats, tableSchemas)

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

  private def buildInputs(
      config: JobConfig,
      lineageStats: Map[Table, LineageStatistics] = Map.empty,
      tableSchemas: Map[Table, LineageSchema] = Map.empty
  ): Seq[OpenLineage.InputDataset] =
    config.inputs.map { table =>
      buildInput(table, lineageStats.get(table), tableSchemas.get(table))
    }

  private def buildInput(
      table: ReadTable,
      stats: Option[LineageStatistics] = None,
      schema: Option[LineageSchema] = None
  ): OpenLineage.InputDataset = {
    val facets = buildDatasetFacets(table, schema)

    val builder = openLineage
      .newInputDatasetBuilder()
      .namespace(namespace)
      .name(table.name)
      .facets(facets)
    stats.foreach { stats =>
      builder.inputFacets(
        openLineage
          .newInputDatasetInputFacetsBuilder()
          .dataQualityMetrics(
            openLineage
              .newDataQualityMetricsInputDatasetFacetBuilder()
              .rowCount(stats.rowCount)
              .bytes(stats.byteSize)
              .build()
          )
          .build()
      )
    }

    builder.build()
  }

  private def buildOutputs(
      config: JobConfig,
      lineageStats: Map[Table, LineageStatistics] = Map.empty,
      tableSchemas: Map[Table, LineageSchema] = Map.empty
  ): Seq[OpenLineage.OutputDataset] =
    config.outputs.map { table =>
      buildOutput(table, lineageStats.get(table), tableSchemas.get(table))
    }

  private def buildOutput(
      table: WriteTable,
      stats: Option[LineageStatistics] = None,
      schema: Option[LineageSchema] = None
  ): OpenLineage.OutputDataset = {
    val facets = buildDatasetFacets(table, schema)

    val builder = openLineage
      .newOutputDatasetBuilder()
      .namespace(namespace)
      .name(table.name)
      .facets(facets)

    stats.foreach { stats =>
      builder.outputFacets(
        openLineage.newOutputDatasetOutputFacets(
          openLineage
            .newOutputStatisticsOutputDatasetFacetBuilder()
            .rowCount(stats.rowCount)
            .size(stats.byteSize)
            .build()
        )
      )
    }
    builder.build()
  }

  private def dataSourceFor(
      table: Table
  ): OpenLineage.DatasourceDatasetFacet = {

    val dataSourceType = table.source.`type` match {
      case S3FileSystem    => "S3_FILE"
      case LocalFileSystem => "LOCAL_FILE"
      case JDBC            => "DB_TABLE"
    }

    openLineage
      .newDatasourceDatasetFacetBuilder()
      .name(table.name)
      .uri(table.source.locationURI)
      .put("type", dataSourceType)
      .build()
  }

  private def buildDatasetFacets(
      table: Table,
      schema: Option[LineageSchema]
  ): OpenLineage.DatasetFacets = {
    val facetsBuilder = openLineage
      .newDatasetFacetsBuilder()
      .dataSource(dataSourceFor(table))
    // TODO: Documentation
    schema.foreach { schema => facetsBuilder.schema(buildSchema(schema)) }

    facetsBuilder.build()
  }

  private def buildSchema(
      schema: LineageSchema
  ): OpenLineage.SchemaDatasetFacet = {
    openLineage
      .newSchemaDatasetFacetBuilder()
      .fields(
        schema.fieldsToType
          .map {
            case (key, dataType) =>
              openLineage
                .newSchemaDatasetFacetFieldsBuilder()
                .name(key)
                .`type`(dataType)
                .build()
          }
          .toList
          .asJava
      )
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

      logger.debug(s"Sending JSON: $runEventJson")
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
