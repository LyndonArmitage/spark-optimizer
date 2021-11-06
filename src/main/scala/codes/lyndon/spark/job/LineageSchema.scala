package codes.lyndon.spark.job

import org.apache.spark.sql.DataFrame

case class LineageSchema(
    fieldsToType: Map[String, String]
)

object LineageSchema {

  def from(df: DataFrame): LineageSchema = {
    LineageSchema(df.dtypes.toMap)
  }

}