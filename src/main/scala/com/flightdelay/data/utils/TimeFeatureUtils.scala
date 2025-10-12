package com.flightdelay.data.utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object TimeFeatureUtils {

  /**
   * Arrondit une colonne d'heure au format HHMM à l'heure ronde la plus proche
   */
  def roundTimeToNearestHour(timeCol: Column): Column = {
    when(timeCol % lit(100) < lit(30),
      (floor(timeCol / lit(100)) * lit(100))

    ).otherwise(
      when(floor(timeCol / lit(100)) === lit(23),
        lit(0)
      ).otherwise(
        ((floor(timeCol / lit(100)) + lit(1)) * lit(100))
      )
    )
  }

  /**
   * Arrondit et retourne l'heure en format simple (0-23)
   */
  def roundTimeToNearestHourSimple(timeCol: Column): Column = {
    when(timeCol % lit(100) < lit(30),
      floor(timeCol / lit(100))
    ).otherwise(
      when(floor(timeCol / lit(100)) === lit(23),
        lit(0)
      ).otherwise(
        (floor(timeCol / lit(100)) + lit(1))
      )
    )
  }
}