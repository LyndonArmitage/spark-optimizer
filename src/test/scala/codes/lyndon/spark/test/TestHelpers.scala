package codes.lyndon.spark.test

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters.seqAsJavaListConverter

object TestHelpers {

  implicit class RowsHelpers(rows: Seq[Row]) {

    def toDF(schema: StructType)(implicit spark: SparkSession): DataFrame = {
      spark.createDataFrame(
        rows.asJava,
        schema
      )
    }

  }

}
