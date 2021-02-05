package codes.lyndon.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.SizeEstimator


import scala.util.Try

object DataFrameEstimator {

  implicit class DataFrameUtils(
      dataFrame: DataFrame
  ) {

    /**
      * Returns a sample of this DataFrame at the given size.
      *
     * This requires knowing the row count of the DataFrame
      * @param approxSize The size of the sample to return, this is approximate
      *                   because result may slightly differ
      * @param seed The seed to use for the sample operation
      * @param rowCount The count of rows in this DataFrame, by default this
      *                 is got using a count() operation
      * @return The sample DataFrame. If the approxSize is equal to or smaller
      *         than the whole DataFrame is returned
      */
    def sampleSized(
        approxSize: Int,
        seed: Long = 1L,
        rowCount: Long = dataFrame.count()
    ): DataFrame = {
      if (dataFrame.isEmpty || rowCount <= approxSize) return dataFrame
      val multiples = rowCount / approxSize
      val fraction  = 1.0 / multiples.toDouble
      dataFrame.sample(fraction, seed)
    }

    /**
      * Estimates the row size in bytes based on the DataFrame's schema alone.
      *
     * If the DataFrame is empty this will return 0.
      *
      * <b>Important Note:</b> This is <i>not</i> a very accurate way of getting
      * estimating a row size as the default estimated values will be very small
      * and inaccurate for non-primitive types like String and Array etc.
      * You should favour sampling the DataFrame row size with
      * [[sampleRowSize()]] to get a more accurate view
      * (including this estimate).
      *
     *
      * @return The row size estimate in bytes, 0 if the DataFrame was empty
      */
    def estimatedRowSize: Long = {
      if (dataFrame.isEmpty) return 0L
      val schema = dataFrame.schema
      schema.map(_.dataType.defaultSize).sum
    }

    /**
      * Samples the DataFrame and gets an estimate of each row's size in bytes in
      * the sample.
      *
      * Returned DataFrame contains the various stats on this sample.
      * @param sampleSize The sample size to get row sizes from, this will be
      *                   matched as close as possible see [[sampleRowSize()]]
      *                   for more information
      * @return A DataFrame containing the stats on the sampled row sizes in bytes
      */
    def sampleRowSize(
        sampleSize: Int = 10000
    ): DataFrame = {
      if (sampleSize <= 0 || dataFrame.isEmpty)
        return dataFrame.sparkSession.emptyDataFrame

      import dataFrame.sparkSession.implicits._

      val totalRowCount = dataFrame.count()
      val sample        = dataFrame.sampleSized(sampleSize, rowCount = totalRowCount)

      val schema = dataFrame.schema.zipWithIndex

      val rowSizes = sample
        .map { row =>
          schema.map {
            case (field, index) =>
              val dataType    = field.dataType
              val defaultSize = dataType.defaultSize.toLong
              val value       = if (!row.isNullAt(index)) row.get(index) else null

              dataType match {
                case ByteType    => defaultSize
                case BooleanType => defaultSize
                case ShortType   => defaultSize
                case IntegerType => defaultSize
                case LongType    => defaultSize
                case FloatType   => defaultSize
                case DoubleType  => defaultSize
                case _ =>
                  value match {
                    case x if x == null => defaultSize
                    case x: AnyRef =>
                      val size = Try(SizeEstimator.estimate(x))
                      if (size.isSuccess) size.get else defaultSize
                    case _ => defaultSize
                  }
              }

          }.sum
        }
        .toDF("size")

      rowSizes.agg(
        count("size").as("sample_row_count"),
        lit(totalRowCount).as("total_row_count"),
        lit(dataFrame.estimatedRowSize).as("estimate"),
        mean("size").as("mean"),
        max("size").as("max"),
        min("size").as("min"),
        stddev("size").as("stddev"),
        var_samp("size").as("var_samp"),
        var_pop("size").as("var_pop")
      )
    }

  }
}
