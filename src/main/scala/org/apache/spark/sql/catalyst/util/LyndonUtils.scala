package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.util.ArrayData.toArrayData

import java.time.{Duration, LocalDate}

object LyndonUtils {

  type SQLDate = Int

  private[this] def localDate(date: SQLDate): LocalDate =
    LocalDate.ofEpochDay(date)

  private[this] def localDateToDays(localDate: LocalDate): SQLDate =
    Math.toIntExact(localDate.toEpochDay)

  def getDatesBetween(start: SQLDate, end: SQLDate): ArrayData = {
    val startDate = localDate(start)
    val daysBetween = Duration
      .between(
        startDate.atStartOfDay(),
        localDate(end).atStartOfDay()
      )
      .toDays

    val arr = (0L to daysBetween).map { day =>
      localDateToDays(startDate.plusDays(day))
    }.toArray

    toArrayData(arr)
  }
}
