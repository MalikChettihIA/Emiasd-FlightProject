package com.flightdelay.data.preprocessing.weather

import com.flightdelay.config.AppConfiguration
import com.flightdelay.utils.DebugUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Feature engineering pour les TENDANCES météo temporelles
 *
 * Problème identifié : Le modèle Random Forest n'améliore pas beaucoup avec plus d'heures météo
 * car il voit les valeurs brutes (température h1, h2, h3...) mais pas les TENDANCES.
 *
 * Ce module calcule :
 * - Pente/slope : évolution linéaire sur les N heures (température monte/descend?)
 * - Variance : volatilité/instabilité des conditions météo
 * - Direction : tendance globale (hausse, baisse, stable)
 * - Accélération : le changement s'accélère-t-il?
 */
object WeatherTrendFeatures {

  /**
   * Crée des features de tendance à partir des colonnes météo temporelles explodées
   *
   * Pour chaque variable météo (ex: Temperature, Pressure, Visibility), calcule:
   * - trend_slope : pente linéaire (régression simple)
   * - trend_variance : écart-type des valeurs
   * - trend_direction : -1 (baisse), 0 (stable), +1 (hausse)
   * - trend_acceleration : différence entre slope(h1-h3) et slope(h4-h6)
   *
   * @param df DataFrame avec colonnes météo explodées (ex: origin_weather_Temperature_h1, h2, h3...)
   * @param weatherVars Variables météo à analyser
   * @param depth Profondeur temporelle (nombre d'heures)
   * @param locations "origin", "destination", ou les deux
   */
  def createWeatherTrendFeatures(
    df: DataFrame,
    weatherVars: Seq[String] = Seq("HourlyPrecip", "RelativeHumidity", "Temperature", "pressure_change_abs"),
    depth: Int = 6,
    locations: Seq[String] = Seq("origin", "destination")
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.weather.WeatherTrendFeatures.createWeatherTrendFeatures()")
    debug(s"  - Variables: ${weatherVars.mkString(", ")}")
    debug(s"  - Depth: $depth hours")
    debug(s"  - Locations: ${locations.mkString(", ")}")

    var result = df
    var addedFeatures = 0

    locations.foreach { location =>
      weatherVars.foreach { varName =>

        // Vérifier si les colonnes existent
        val cols = (1 to depth).map(h => s"${location}_weather_${varName}_h$h")
        val existingCols = cols.filter(df.columns.contains)

        if (existingCols.length >= 3) {  // Au moins 3 points pour calculer une tendance

          // 1. SLOPE (pente linéaire) - mesure la vitesse de changement
          // Formule simplifiée : (dernière_valeur - première_valeur) / nombre_heures
          val firstCol = existingCols.head
          val lastCol = existingCols.last
          val slopeCol = s"${location}_weather_${varName}_trend_slope"

          result = result.withColumn(slopeCol,
            (coalesce(col(lastCol), lit(0.0)) - coalesce(col(firstCol), lit(0.0))) / lit(existingCols.length.toDouble)
          )

          // 2. VARIANCE (volatilité) - mesure l'instabilité
          // Calcule l'écart-type des valeurs temporelles
          val varianceCol = s"${location}_weather_${varName}_trend_variance"
          val valuesArray = existingCols.map(c => coalesce(col(c), lit(0.0)))

          // Calcul de la variance : sqrt(sum((x - mean)^2) / n)
          val mean = valuesArray.reduce(_ + _) / lit(valuesArray.length.toDouble)
          val squaredDiffs = valuesArray.map(v => pow(v - mean, 2.0))
          val variance = sqrt(squaredDiffs.reduce(_ + _) / lit(valuesArray.length.toDouble))

          result = result.withColumn(varianceCol, variance)

          // 3. DIRECTION (classification de la tendance)
          // -1: baisse significative, 0: stable, +1: hausse significative
          val directionCol = s"${location}_weather_${varName}_trend_direction"
          result = result.withColumn(directionCol,
            when(col(slopeCol) < lit(-0.1), lit(-1))  // Seuil ajustable selon la variable
              .when(col(slopeCol) > lit(0.1), lit(1))
              .otherwise(lit(0))
          )

          // 4. ACCELERATION (changement de changement)
          // Compare la pente récente vs pente ancienne
          if (existingCols.length >= 6) {
            val midPoint = existingCols.length / 2
            val recentCols = existingCols.takeRight(midPoint)
            val olderCols = existingCols.take(midPoint)

            val recentSlope = (coalesce(col(recentCols.last), lit(0.0)) - coalesce(col(recentCols.head), lit(0.0))) / lit(recentCols.length.toDouble)
            val olderSlope = (coalesce(col(olderCols.last), lit(0.0)) - coalesce(col(olderCols.head), lit(0.0))) / lit(olderCols.length.toDouble)

            val accelCol = s"${location}_weather_${varName}_trend_acceleration"
            result = result.withColumn(accelCol, recentSlope - olderSlope)

            addedFeatures += 4  // slope + variance + direction + acceleration
          } else {
            addedFeatures += 3  // slope + variance + direction seulement
          }

          debug(s"  ✓ Created trend features for: ${location}_weather_${varName} (from ${existingCols.length} hours)")

        } else {
          debug(s"  ✗ Skipped: ${location}_weather_${varName} (only ${existingCols.length} columns found, need >= 3)")
        }
      }
    }

    info(s"  - Total trend features added: $addedFeatures")
    result
  }

  /**
   * Crée des features de tendance spécifiques pour les conditions météo critiques
   *
   * Focus sur les patterns dangereux :
   * - Détérioration rapide de la visibilité
   * - Chute rapide de pression (tempête approchante)
   * - Augmentation rapide de l'humidité + baisse de température (risque de givrage)
   */
  def createCriticalWeatherTrendFeatures(
    df: DataFrame,
    depth: Int = 6
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Creating critical weather trend features...")
    var result = df

    // Pattern 1: Détérioration rapide de visibilité
    // Si visibilité baisse de >50% en quelques heures -> conditions dangereuses
    val visibilityCols = (1 to depth).map(h => s"origin_weather_Visibility_h$h").filter(df.columns.contains)
    if (visibilityCols.length >= 3) {
      result = result.withColumn("feature_visibility_deteriorating_fast",
        when(
          (coalesce(col(visibilityCols.head), lit(10.0)) - coalesce(col(visibilityCols.last), lit(10.0))) > lit(5.0),
          lit(1)
        ).otherwise(lit(0))
      )
      debug("  ✓ Created: feature_visibility_deteriorating_fast")
    }

    // Pattern 2: Conditions de givrage en développement
    // Température descend vers 0°C + humidité augmente
    val tempCols = (1 to depth).map(h => s"origin_weather_Temperature_h$h").filter(df.columns.contains)
    val humidCols = (1 to depth).map(h => s"origin_weather_RelativeHumidity_h$h").filter(df.columns.contains)

    if (tempCols.length >= 2 && humidCols.length >= 2) {
      val tempTrend = (coalesce(col(tempCols.last), lit(15.0)) - coalesce(col(tempCols.head), lit(15.0)))
      val humidTrend = (coalesce(col(humidCols.last), lit(50.0)) - coalesce(col(humidCols.head), lit(50.0)))

      result = result.withColumn("feature_icing_conditions_developing",
        when(
          (tempTrend < lit(-2.0))  // Température baisse rapidement
            .and(humidTrend > lit(10.0))  // Humidité augmente
            .and(col(tempCols.last) < lit(5.0)),  // Température finale proche de 0
          lit(1)
        ).otherwise(lit(0))
      )
      debug("  ✓ Created: feature_icing_conditions_developing")
    }

    result
  }
}
