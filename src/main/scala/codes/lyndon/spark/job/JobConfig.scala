package codes.lyndon.spark.job

trait JobConfig {
  def jobName: String

  def inputs: Seq[ReadTable]

  def outputs: Seq[WriteTable]
}
