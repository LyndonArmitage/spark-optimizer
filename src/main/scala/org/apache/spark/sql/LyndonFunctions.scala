package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{DatesBetween, Expression}

object LyndonFunctions {

  private def withExpr(expr: Expression): Column = Column(expr)

  def dates_between(start: Column, end: Column): Column =
    withExpr {
      DatesBetween(start.expr, end.expr)
    }

}
