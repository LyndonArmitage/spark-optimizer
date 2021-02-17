package codes.lyndon.spark

import org.apache.spark.sql.LyndonFunctions.dates_between
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object ExtraDataFrameFunctions {

  implicit class DataFrameUtils(df: DataFrame) {

    def densify_on_date(dateColumn: String): DataFrame = {
      import df.sparkSession.implicits._
      val dates = df
        .select(dateColumn)
        .agg(
          min(dateColumn).as("min"),
          max(dateColumn).as("max")
        )
        .withColumn("range", dates_between($"min", $"max"))
        .drop("min", "max")
        .withColumn(dateColumn, explode($"range"))
        .drop("range")

      // Must be outer to include any rows with null values in their date column
      df.join(dates, Seq(dateColumn), "outer")
        .sort(col(dateColumn).asc_nulls_first)
    }
  }

}
