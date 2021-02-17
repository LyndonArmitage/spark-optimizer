package org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.codegen.{
  CodegenContext,
  ExprCode
}
import org.apache.spark.sql.catalyst.util.{GenericArrayData, LyndonUtils}
import org.apache.spark.sql.types.{
  AbstractDataType,
  ArrayType,
  DataType,
  DateType
}

//noinspection ScalaFileName
case class DatesBetween(
    startDate: Expression,
    endDate: Expression
) extends BinaryExpression
    with ImplicitCastInputTypes {
  override def prettyName: String = "dates_between"

  override def left: Expression  = startDate
  override def right: Expression = endDate

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)

  override def dataType: DataType = ArrayType(DateType, containsNull = false)

  override protected def nullSafeEval(start: Any, end: Any): Any =
    LyndonUtils.getDatesBetween(start.asInstanceOf[Int], end.asInstanceOf[Int])

  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = {
    val dtu = LyndonUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(
      ctx,
      ev,
      (a, b) => s"$dtu.getDatesBetween($a,$b)"
    )
  }
}
