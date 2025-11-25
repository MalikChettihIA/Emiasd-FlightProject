package com.flightdelay.data.preprocessing.flights

import com.flightdelay.config.AppConfiguration
import com.flightdelay.utils.DebugUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Feature engineering pour features cycliques (cosinus/sinus)
 * Transforme les variables temporelles en représentation cyclique
 * pour capturer leur nature périodique
 *
 * Les features cycliques permettent au modèle de comprendre que :
 * - L'heure 23h et 0h sont proches
 * - La semaine 52 et la semaine 1 sont proches
 *
 * Formule : sin(2π × x / période) et cos(2π × x / période)
 */
object FlightCyclicFeatures {

  /**
   * Point d'entrée unique : enrichit le DataFrame avec les features cycliques
   *
   * Colonnes ajoutées :
   *  - feature_hour_sin, feature_hour_cos : Heure de départ arrondie (0-23)
   *  - feature_week_of_year_sin, feature_week_of_year_cos : Semaine de l'année (1-52)
   *
   * @param df DataFrame d'entrée
   * @param spark Implicit SparkSession
   * @param configuration Implicit AppConfiguration
   * @return DataFrame enrichi avec les features cycliques
   */
  def createCyclicFeatures(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightCyclicFeatures.createCyclicFeatures()")
    debug("[Feature Engineering] FlightCyclicFeatures.createCyclicFeatures")

    var cur = df
    var addedFeatures = 0

    // 1. Heure de départ arrondie (0-23 heures)
    if (cur.columns.contains("feature_departure_hour_rounded")) {
      cur = cur
        .withColumn("feature_departure_hour_rounded_sin",
          sin(col("feature_departure_hour_rounded") * lit(2.0 * Math.PI / 24.0)))
        .withColumn("feature_departure_hour_rounded_cos",
          cos(col("feature_departure_hour_rounded") * lit(2.0 * Math.PI / 24.0)))

      debug("  - Created: feature_departure_hour_rounded_sin, feature_departure_hour_rounded_cos (from feature_departure_hour_rounded, period=24)")
      addedFeatures += 2
    } else {
      debug("  - Skipped: feature_departure_hour_rounded_sin/cos (feature_departure_hour_rounded_hour_rounded not found)")
    }

    // 2. Semaine de l'année (1-52)
    if (cur.columns.contains("feature_flight_week_of_year")) {
      cur = cur
        .withColumn("feature_flight_week_of_year_sin",
          sin((col("feature_flight_week_of_year") - lit(1)) * lit(2.0 * Math.PI / 52.0)))
        .withColumn("feature_flight_week_of_year_cos",
          cos((col("feature_flight_week_of_year") - lit(1)) * lit(2.0 * Math.PI / 52.0)))

      debug("  - Created: feature_flight_week_of_year_sin, feature_flight_week_of_year_cos (from feature_flight_week_of_year, period=52)")
      addedFeatures += 2
    } else {
      debug("  - Skipped: ffeature_flight_week_of_year_sin/cos (feature_flight_week_of_year not found)")
    }

    debug(s"  - Total cyclic features added: $addedFeatures")
    cur
  }

  /**
   * Version flexible : permet de choisir quelles features cycliques créer
   *
   * @param df DataFrame d'entrée
   * @param enableHour Activer les features cycliques pour l'heure
   * @param enableWeekOfYear Activer les features cycliques pour la semaine de l'année
   * @param spark Implicit SparkSession
   * @param configuration Implicit AppConfiguration
   * @return DataFrame enrichi avec les features cycliques sélectionnées
   */
  def createSelectiveCyclicFeatures(
    df: DataFrame,
    enableHour: Boolean = true,
    enableWeekOfYear: Boolean = true
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightCyclicFeatures.createSelectiveCyclicFeatures()")
    debug(s"  - enableHour: $enableHour, enableWeekOfYear: $enableWeekOfYear")

    var cur = df

    if (enableHour && cur.columns.contains("feature_departure_hour_rounded")) {
      cur = cur
        .withColumn("feature_hour_sin", sin(col("feature_departure_hour_rounded") * lit(2.0 * Math.PI / 24.0)))
        .withColumn("feature_hour_cos", cos(col("feature_departure_hour_rounded") * lit(2.0 * Math.PI / 24.0)))
      debug("  - Created: feature_hour_sin, feature_hour_cos")
    }

    if (enableWeekOfYear && cur.columns.contains("feature_flight_week_of_year")) {
      cur = cur
        .withColumn("feature_week_of_year_sin", sin((col("feature_flight_week_of_year") - lit(1)) * lit(2.0 * Math.PI / 52.0)))
        .withColumn("feature_week_of_year_cos", cos((col("feature_flight_week_of_year") - lit(1)) * lit(2.0 * Math.PI / 52.0)))
      debug("  - Created: feature_week_of_year_sin, feature_week_of_year_cos")
    }

    cur
  }
}
