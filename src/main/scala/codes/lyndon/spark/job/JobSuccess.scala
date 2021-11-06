package codes.lyndon.spark.job

case class JobSuccess(
    lineageStats: Map[Table, LineageStatistics] = Map.empty,
    lineageSchemas: Map[Table, LineageSchema] = Map.empty
)
