package codes.lyndon.spark

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.LyndonFunctions.dates_between
import org.apache.spark.sql.catalyst.expressions.{Ascending, NullOrdering, NullsFirst, SortDirection, SortOrder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DataTypes}

import java.sql.Date

object ExtraDataFrameFunctions {

  implicit class DataFrameUtils(df: DataFrame) {

    def densify_on_date(
        dateColumn: String,
        order: SortDirection = Ascending,
        nullOrdering: NullOrdering = NullsFirst,
        dropNullDates: Boolean = false
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

      val sortCol = new Column(
        new SortOrder(
          col(dateColumn).expr,
          order,
          nullOrdering,
          Seq.empty
        )
      )
      if (!dropNullDates) {
        // Must be outer to include any rows with null values in their date
        // column
        df.join(dates, Seq(dateColumn), "outer")
      } else {
        // If we are dropping nulls we can use the dates df on the left of the
        // join
        dates.join(df, Seq(dateColumn), "left")
      }.sort(sortCol)
    }

    def densify_on_date2(
        dateColumn: String,
        order: SortDirection = Ascending,
        nullOrdering: NullOrdering = NullsFirst,
        dropNullDates: Boolean = false
    ): DataFrame = {
      import df.sparkSession.implicits._
      val minMax = df
        .select(dateColumn)
        .agg(
          min(dateColumn).as("min"),
          max(dateColumn).as("max")
        )
        .withColumn("diff", datediff($"max", $"min"))
        .collect()

      val (minDate, maxDate, diff) = minMax.map {
        case Row(min: Date, max: Date, diff: Int) =>
          (min, max, diff)
      }.head

      val sortCol = new Column(
        new SortOrder(
          col(dateColumn).expr,
          order,
          nullOrdering,
          Seq.empty
        )
      )

      val dates = df.sparkSession
        .range(diff + 1)
        .withColumn("id", $"id".cast(DataTypes.IntegerType))
        .withColumn("start_date", lit(minDate))
        .withColumn(dateColumn, date_add($"start_date", $"id"))
        .drop("start_date", "id")
        .orderBy(sortCol)

      if (!dropNullDates) {
        // Must be outer to include any rows with null values in their date
        // column
        df.join(dates, Seq(dateColumn), "outer")
      } else {
        // If we are dropping nulls we can use the dates df on the left of the
        // join
        dates.join(df, Seq(dateColumn), "left")
      }.sort(sortCol)
    }

  }

}
