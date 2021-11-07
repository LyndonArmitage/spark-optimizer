package codes.lyndon.spark.job

import org.apache.spark.sql.DataFrame

case class LineageSchema(
    fieldsToType: Map[String, String]
)

object LineageSchema {

  def from(df: DataFrame): LineageSchema = {
    val fieldsToTypes = df.schema.fields.map { field =>
      (field.name, field.dataType.sql)
    }.toMap

    LineageSchema(fieldsToTypes)
  }

}