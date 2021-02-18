package codes.lyndon.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.LyndonFunctions.dates_between
import org.apache.spark.sql.functions._

object ExtraDataFrameFunctions {

  implicit class DataFrameUtils(df: DataFrame) {

    def densify_on_date(
        dateColumn: String,
        ascending: Boolean = true,
        nullsFirst: Boolean = true
    ): DataFrame = {
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

      val sortCol = (ascending, nullsFirst) match {
        case (true, true)   => col(dateColumn).asc_nulls_first
        case (true, false)  => col(dateColumn).asc_nulls_last
        case (false, true)  => col(dateColumn).desc_nulls_first
        case (false, false) => col(dateColumn).desc_nulls_last
      }
      // Must be outer to include any rows with null values in their date column
      df.join(dates, Seq(dateColumn), "outer")
        .sort(sortCol)
    }
  }

}
