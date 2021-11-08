package codes.lyndon.spark.job

trait WriteTable extends Table {
  def mode: String
  def partitionBy: Seq[String]
  def format: String
}