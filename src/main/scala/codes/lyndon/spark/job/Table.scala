package codes.lyndon.spark.job

trait Table {
  def name: String
  def source: DataSource
}
