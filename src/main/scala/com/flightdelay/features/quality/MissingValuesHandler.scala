package com.flightdelay.features.quality

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.flightdelay.utils.DebugUtils._

/**
 * Handler pour gérer les valeurs manquantes dans les données météo
 *
 * Stratégie:
 * 1. Détecte les valeurs NULL par heure d'observation (h1, h2, ..., h7)
 * 2. Crée des colonnes de flag binaires indiquant la présence/absence de données
 * 3. Remplace les NULL par des valeurs sentinelles configurables par feature
 *
 * Features supportées:
 * - Features météo de base (ex: origin_weather_Humidity_Delta_1hr_h7)
 * - Features agrégées (ex: origin_weather_press_change_abs_Max)
 */
object MissingValuesHandler {

  /**
   * Applique le traitement des valeurs manquantes sur le DataFrame
   *
   * @param df DataFrame avec potentiellement des valeurs NULL
   * @param experiment Configuration de l'expérimentation (contient les sentinelles)
   * @param spark Session Spark implicite
   * @param configuration Configuration de l'application
   * @return DataFrame avec sentinelles et flags de missing values
   */
  def handleMissingValues(
    df: DataFrame,
    experiment: ExperimentConfig
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("=" * 80)
    info("[MissingValuesHandler] Handling missing weather observations")
    info("=" * 80)

    val weatherOriginDepth = experiment.featureExtraction.weatherOriginDepthHours
    val weatherDestDepth = experiment.featureExtraction.weatherDestinationDepthHours

    // Étape 1: Gérer les features météo par heure (h1, h2, ..., h7)
    val dfWithHourlyFlags = handleHourlyMissingValues(
      df,
      weatherOriginDepth,
      weatherDestDepth,
      experiment
    )

    // Étape 2: Gérer les features agrégées (Sum, Avg, Max, Min, etc.)
    val dfWithAggregatedFlags = handleAggregatedMissingValues(
      dfWithHourlyFlags,
      weatherOriginDepth,
      weatherDestDepth,
      experiment
    )

    info("=" * 80)

    dfWithAggregatedFlags
  }

  /**
   * Gère les valeurs manquantes pour les features par heure (h1, h2, ..., h7)
   *
   * Pour chaque heure:
   * - Crée un flag: origin_weather_missing_h1, origin_weather_missing_h2, etc.
   * - Remplace les NULL par des sentinelles pour toutes les colonnes de cette heure
   */
  private def handleHourlyMissingValues(
    df: DataFrame,
    weatherOriginDepth: Int,
    weatherDestDepth: Int,
    experiment: ExperimentConfig
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("[MissingValuesHandler] Step 1: Handling hourly missing values")

    var result = df
    var flagsCreated = 0
    var sentinelsApplied = 0

    val maxDepth = math.max(weatherOriginDepth, weatherDestDepth)

    (1 to maxDepth).foreach { h =>

      // ========== ORIGINE ==========
      if (h <= weatherOriginDepth) {
        val hourCol = s"origin_weather_hour_h$h"
        val flagCol = s"origin_weather_missing_h$h"

        if (result.columns.contains(hourCol)) {

          // 1. Créer le flag de missing value
          result = result.withColumn(
            flagCol,
            when(col(hourCol).isNull, 1).otherwise(0)
          )
          flagsCreated += 1

          // 2. Identifier toutes les colonnes météo pour cette heure
          val weatherCols = result.columns.filter(c =>
            c.startsWith("origin_weather_") && c.endsWith(s"_h$h")
          )

          // 3. Remplacer les NULL par des sentinelles pour chaque colonne
          weatherCols.foreach { colName =>
            // Extraire le nom de la feature (ex: "Humidity_Delta_1hr" depuis "origin_weather_Humidity_Delta_1hr_h7")
            val featureName = extractFeatureName(colName, "origin_weather_", s"_h$h")

            // Récupérer la valeur sentinelle depuis la configuration
            val sentinel = getSentinelValue(featureName, result.schema(colName).dataType, experiment)

            result = result.withColumn(
              colName,
              coalesce(col(colName), sentinel)
            )
            sentinelsApplied += 1
          }

          debug(s"  [ORIGIN h$h] Created flag '$flagCol', applied sentinels to ${weatherCols.length} columns")
        }
      }

      // ========== DESTINATION ==========
      if (h <= weatherDestDepth) {
        val hourCol = s"destination_weather_hour_h$h"
        val flagCol = s"destination_weather_missing_h$h"

        if (result.columns.contains(hourCol)) {

          // 1. Créer le flag de missing value
          result = result.withColumn(
            flagCol,
            when(col(hourCol).isNull, 1).otherwise(0)
          )
          flagsCreated += 1

          // 2. Identifier toutes les colonnes météo pour cette heure
          val weatherCols = result.columns.filter(c =>
            c.startsWith("destination_weather_") && c.endsWith(s"_h$h")
          )

          // 3. Remplacer les NULL par des sentinelles
          weatherCols.foreach { colName =>
            val featureName = extractFeatureName(colName, "destination_weather_", s"_h$h")
            val sentinel = getSentinelValue(featureName, result.schema(colName).dataType, experiment)

            result = result.withColumn(
              colName,
              coalesce(col(colName), sentinel)
            )
            sentinelsApplied += 1
          }

          debug(s"  [DESTINATION h$h] Created flag '$flagCol', applied sentinels to ${weatherCols.length} columns")
        }
      }
    }

    info(f"  ✓ Created $flagsCreated%3d hourly missing flags")
    info(f"  ✓ Applied sentinels to $sentinelsApplied%3d hourly columns")

    result
  }

  /**
   * Gère les valeurs manquantes pour les features agrégées (Sum, Avg, Max, Min, etc.)
   *
   * Note: Les compteurs de missing (_missing_count) sont déjà créés par DataJoinerPostProcessor
   * Cette fonction applique SEULEMENT les sentinelles aux valeurs NULL des features agrégées
   *
   * Exemple: origin_weather_press_change_abs_Max
   * - Remplace NULL par sentinelle
   * - Flag déjà présent: origin_weather_press_change_abs_Max_missing_count
   */
  private def handleAggregatedMissingValues(
    df: DataFrame,
    weatherOriginDepth: Int,
    weatherDestDepth: Int,
    experiment: ExperimentConfig
  )(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("[MissingValuesHandler] Step 2: Handling aggregated missing values")

    var result = df
    var sentinelsApplied = 0

    // Récupérer les features agrégées depuis la configuration
    val aggregatedFeatures = experiment.featureExtraction.aggregatedSelectedFeatures.getOrElse(Map.empty)

    if (aggregatedFeatures.isEmpty) {
      info("  ℹ No aggregated features configured, skipping")
      return result
    }

    // Pour chaque feature agrégée configurée
    aggregatedFeatures.foreach { case (varName, aggConfig) =>
      val aggMethod = aggConfig.aggregation.toLowerCase.capitalize

      // ORIGINE
      if (weatherOriginDepth > 0) {
        val aggColName = s"origin_weather_${varName}_$aggMethod"

        if (result.columns.contains(aggColName)) {
          // Remplacer NULL par sentinelle (le compteur _missing_count existe déjà)
          val sentinel = getSentinelValue(varName, result.schema(aggColName).dataType, experiment)
          result = result.withColumn(
            aggColName,
            coalesce(col(aggColName), sentinel)
          )
          sentinelsApplied += 1

          debug(s"  [ORIGIN AGG] Applied sentinel to '$aggColName' (missing_count flag already exists)")
        }
      }

      // DESTINATION
      if (weatherDestDepth > 0) {
        val aggColName = s"destination_weather_${varName}_$aggMethod"

        if (result.columns.contains(aggColName)) {
          // Remplacer NULL par sentinelle (le compteur _missing_count existe déjà)
          val sentinel = getSentinelValue(varName, result.schema(aggColName).dataType, experiment)
          result = result.withColumn(
            aggColName,
            coalesce(col(aggColName), sentinel)
          )
          sentinelsApplied += 1

          debug(s"  [DESTINATION AGG] Applied sentinel to '$aggColName' (missing_count flag already exists)")
        }
      }
    }

    info(f"  ✓ Applied sentinels to $sentinelsApplied%3d aggregated columns")
    info("  ℹ Missing counts already tracked via _missing_count columns (created during aggregation)")

    result
  }

  /**
   * Extrait le nom de la feature depuis un nom de colonne complet
   *
   * Exemple:
   *   extractFeatureName("origin_weather_Humidity_Delta_1hr_h7", "origin_weather_", "_h7")
   *   => "Humidity_Delta_1hr"
   */
  private def extractFeatureName(colName: String, prefix: String, suffix: String): String = {
    colName.stripPrefix(prefix).stripSuffix(suffix)
  }

  /**
   * Récupère la valeur sentinelle pour une feature depuis la configuration
   *
   * Ordre de priorité:
   * 1. Valeur sentinelle explicite dans weatherSelectedFeatures[featureName].sentinelValue
   * 2. Valeur sentinelle explicite dans aggregatedSelectedFeatures[featureName].sentinelValue
   * 3. Valeur sentinelle par défaut selon le type de données
   *
   * @param featureName Nom de la feature (ex: "Humidity_Delta_1hr")
   * @param dataType Type de données Spark (DoubleType, IntegerType, StringType)
   * @param experiment Configuration de l'expérimentation
   * @return Column contenant la valeur sentinelle
   */
  private def getSentinelValue(
    featureName: String,
    dataType: DataType,
    experiment: ExperimentConfig
  ): org.apache.spark.sql.Column = {

    // 1. Chercher dans weatherSelectedFeatures
    val weatherSentinel = experiment.featureExtraction.weatherSelectedFeatures
      .flatMap(_.get(featureName))
      .flatMap(_.sentinelValue)

    // 2. Chercher dans aggregatedSelectedFeatures
    val aggregatedSentinel = experiment.featureExtraction.aggregatedSelectedFeatures
      .flatMap(_.get(featureName))
      .flatMap(_.sentinelValue)

    // 3. Valeur trouvée ou valeur par défaut
    val sentinelValue = weatherSentinel.orElse(aggregatedSentinel).getOrElse {
      // Valeurs par défaut selon le type
      dataType match {
        case DoubleType | FloatType => -999.0
        case IntegerType | LongType => -999
        case StringType => "MISSING"
        case _ => null
      }
    }

    // Convertir en Column
    sentinelValue match {
      case d: Double => lit(d)
      case i: Int => lit(i)
      case l: Long => lit(l)
      case s: String => lit(s)
      case _ => lit(null)
    }
  }
}
