package codes.lyndon.spark

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.SizeEstimator

import scala.util.Try

/**
  * :: Experimental ::
  * Code for estimating and profiling DataFrame sizes for tuning jobs.
  */
@Experimental
object DataFrameEstimator {

  /**
    * Size hints for [[DataFrameUtils.estimatedRowSize()]]
    * @see [[DataFrameUtils.estimatedRowSize()]]
    */
  sealed trait SizeHint {
    def calculate(field: StructField): Option[Long]
  }

  object SizeHint {

    /**
      * Hint describing the given field as having a fixed size
      * @param size The fixed size
      */
    case class FixedSize(size: Long) extends SizeHint {
      override def calculate(field: StructField): Option[Long] = Some(size)
    }

    /**
      * Hint for fields that are variable in length.
      *
     * The result will be a calculation based on the estimated number of
      * entries.
      *
     * This supports the following types:
      * <ul>
      * <li>StringType</li>
      * <li>ArrayType</li>
      * <li>MapType</li>
      * </ul>
      * @param estimatedEntries The estimated number of entries for this field
      */
    case class LengthHint(estimatedEntries: Int) extends SizeHint {
      override def calculate(field: StructField): Option[Long] = {
        field.dataType match {
          case _: StringType =>
            Some(estimateString(estimatedEntries))
          case arr: ArrayType =>
            Some(arr.elementType.defaultSize * estimatedEntries)
          case map: MapType =>
            Some(
              (map.keyType.defaultSize + map.valueType.defaultSize)
                * estimatedEntries
            )
          case _ => None
        }
      }

      private[this] def estimateString(len: Int): Long = {
        val baseSize = 40
        val estimate = baseSize + roundUp(len, 8)
        math.max(
          estimate,
          baseSize
        )
      }

      private[this] def roundUp(n: Long, multiple: Long): Long =
        if (n >= 0)
          ((n + multiple - 1) / multiple) * multiple
        else
          (n / multiple) * multiple

    }

  }

  implicit class DataFrameUtils(dataFrame: DataFrame) {

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
      * <b>Important Note:</b> This is <i>not</i> a very accurate way of
      * estimating a row size as the default estimated values will be very small
      * and inaccurate for non-primitive types like String and Array etc.
      * You should favour sampling the DataFrame row size with
      * [[sampleRowSize()]] to get a more accurate view
      * (including this estimate).
      * @return The row size estimate in bytes, 0 if the DataFrame was empty
      *         @see [[estimatedRowSize()]]
      */
    def estimatedRowSize: Long = estimatedRowSize()

    /**
      * Estimates the row size in bytes based on the DataFrame's schema and user
      * given hints for column estimates (optional).
      *
     * If the DataFrame is empty this will return 0.
      *
      * <b>Important Note:</b> This is still <i>not</i> a very accurate way of
      * estimating a row size as the estimated values will be fixed but real
      *  data varies wildly.
      * You should favour sampling the DataFrame row size with
      * [[sampleRowSize()]] to get a more accurate view
      * (including a basic estimate).
      * @param hints The hints to use if any as a tuple of column name to hint.
      *
      *              If multiple hints are given for a single column the one that
      *              returns the maximum is used
      * @return The row size estimate in bytes, 0 if the DataFrame was empty
      */
    def estimatedRowSize(hints: (String, SizeHint)*): Long = {
      if (dataFrame.isEmpty) return 0L
      val schema = dataFrame.schema
      schema.estimateSize(hints: _*)
    }

    /**
      * Samples the DataFrame and gets an estimate of each row's size in bytes in
      * the sample.
      *
      * Returned DataFrame contains the various stats on this sample.
      * @param sampleSize The sample size to get row sizes from, this will be
      *                   matched as close as possible see [[sampleRowSize()]]
      *                   for more information
      * @param totalRowCount The count of rows in this DataFrame, by default
      *                      this is got using a count() operation but can be
      *                      supplied to avoid multiple count operations or given
      *                      as an estimate.
      * @return A DataFrame containing the stats on the sampled row sizes in bytes
      */
    def sampleRowSize(
        sampleSize: Int = 10000,
        totalRowCount: Long = dataFrame.count()
    ): DataFrame = {
      if (sampleSize <= 0 || dataFrame.isEmpty)
        return dataFrame.sparkSession.emptyDataFrame

      import dataFrame.sparkSession.implicits._

      val sample = dataFrame.sampleSized(sampleSize, rowCount = totalRowCount)

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

  implicit class StructTypeUtils(structType: StructType) {

    /**
      * Estimates the size of this struct in bytes based on the schema.
      * @return The size estimate in bytes
      */
    def estimateSize: Long = estimateSize()

    /**
      * Estimates the size of this struct in bytes based on the schema and user
      * given hints for column estimates (optional).
      *
     * @param hints The hints to use if any as a tuple of field name to hint.
      *
     *              If multiple hints are given for a single field the one that
      *              returns the maximum is used
      * @return The size estimate in bytes
      */
    def estimateSize(hints: (String, SizeHint)*): Long =
      structType.map { field =>
        val defaultSize = field.dataType match {
          case _: StringType => 56 // assume non-empty with around 16 chars
          case dataType      => dataType.defaultSize
        }
        val fieldHints = hints.filter(_._1 == field.name).map(_._2)
        if (fieldHints.nonEmpty) {
          val calculations = fieldHints.flatMap(_.calculate(field))
          if (calculations.nonEmpty) {
            calculations.max.ceil.toLong
          } else {
            defaultSize
          }
        } else {
          defaultSize
        }
      }.sum

  }
}
