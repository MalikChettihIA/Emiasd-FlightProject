package com.flightdelay.data.utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

object TimeUtils {

  /**
   * Arrondit une heure HHMM à l'heure ronde la plus proche
   */
  def roundToNearestHour(time: String): String = {
    if (time == null || time.isEmpty) {
      return null
    }

    val timeInt = time.toInt
    val hours = timeInt / 100
    val minutes = timeInt % 100

    val roundedHour = if (minutes < 30) {
      // Arrondir vers le bas
      hours * 100
    } else {
      // Arrondir vers le haut
      if (hours == 23) {
        0  // 23h30+ devient 00h00
      } else {
        (hours + 1) * 100
      }
    }

    // Formater avec padding pour avoir toujours 4 chiffres (HHMM)
    f"$roundedHour%04d"
  }

  // Créer l'UDF
  val roundToNearestHourUDF: UserDefinedFunction = udf(roundToNearestHour _)
}