package com.flightdelay.data.preprocessing.flights

import com.flightdelay.config.AppConfiguration
import com.flightdelay.data.preprocessing.DataPreprocessor
import com.flightdelay.utils.DebugUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * Classe spécialisée pour la génération des labels de classification
 * Implémente les stratégies D1, D2, D3, D4 de l'article TIST pour la création des datasets cibles
 * Responsable de la phase "Target data creation" de l'article TIST
 */
object FlightLabelGenerator extends DataPreprocessor {

  // Seuils de retard selon l'article TIST
  private val DELAY_THRESHOLD_15_MIN = 15.0
  private val DELAY_THRESHOLD_30_MIN = 30.0
  private val DELAY_THRESHOLD_45_MIN = 45.0
  private val DELAY_THRESHOLD_60_MIN = 60.0
  private val DELAY_THRESHOLD_90_MIN = 90.0

  /**
   * Génération complète des labels de classification selon l'article TIST
   * @param enrichedFlightData DataFrame contenant les données enrichies
   * @param spark Session Spark
   * @return DataFrame avec tous les labels de classification
   */
  override def preprocess(flightData: DataFrame, weatherData: DataFrame, wBANAirportTimezoneData: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightLabelGenerator.preprocess")

    debug("")
    debug("")
    debug("=" * 80)
    debug("[STEP 2][FlightLabelGenerator] Flight Label Generator - Start ...")
    debug("=" * 80)

    // Vérifier que les colonnes de retard nécessaires sont présentes
    validateRequiredColumns(flightData)

    // Étape 1: Gestion des valeurs manquantes pour les retards
    val withFilledDelays = handleDelayMissingValues(flightData)

    // Étape 2: Création des labels de base pour différents seuils
    val withBasicLabels = addBasicDelayLabels(withFilledDelays)

    withBasicLabels
  }

  /**
   * Valide que les colonnes nécessaires sont présentes
   */
  private def validateRequiredColumns(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightLabelGenerator.validateRequiredColumns")

    debug("")
    debug("Phase 1: Validate Required Columns")

    val requiredColumns = Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY")
    val missingColumns = requiredColumns.filterNot(df.columns.contains)

    if (missingColumns.nonEmpty) {
      debug(s"Colonnes manquantes pour la génération de labels: ${missingColumns.mkString(", ")}")
      throw new RuntimeException(s"Colonnes requises manquantes: ${missingColumns.mkString(", ")}")
    }

    debug("- Validation of required columns: OK")
  }

  /**
   * Gestion des valeurs manquantes pour les colonnes de retard
   */
  private def handleDelayMissingValues(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightLabelGenerator.handleDelayMissingValues")

    debug("")
    debug("Phase 2: Handling missing values for delays")

    val columnExpressions = Map(
      // Remplacer les valeurs nulles par 0 (interprétées comme "pas de retard de ce type")
      "label_arr_delay_filled" -> when(col("ARR_DELAY_NEW").isNull, 0.0).otherwise(col("ARR_DELAY_NEW")),
      "label_weather_delay_filled" -> when(col("WEATHER_DELAY").isNull, 0.0).otherwise(col("WEATHER_DELAY")),
      "label_nas_delay_filled" -> when(col("NAS_DELAY").isNull, 0.0).otherwise(col("NAS_DELAY")),

      // Indicateurs de valeurs manquantes (features pour ML)
      "label_arr_delay_was_missing" -> when(col("ARR_DELAY_NEW").isNull, 1).otherwise(0),
      "label_weather_delay_was_missing" -> when(col("WEATHER_DELAY").isNull, 1).otherwise(0),
      "label_nas_delay_was_missing" -> when(col("NAS_DELAY").isNull, 1).otherwise(0)
    )

    addCalculatedColumns(df, columnExpressions)
  }

  /**
   * Ajoute les labels de base pour différents seuils de retard
   */
  private def addBasicDelayLabels(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightLabelGenerator.addBasicDelayLabels")
    debug("")
    debug("Phase 2: Adding basic labels for different thresholds")

    val columnExpressions = Map[String, Column](
      // Labels binaires pour différents seuils (selon article TIST)
      "label_is_delayed_15min" -> when(col("label_arr_delay_filled") >= DELAY_THRESHOLD_15_MIN, 1).otherwise(0),
      "label_is_delayed_30min" -> when(col("label_arr_delay_filled") >= DELAY_THRESHOLD_30_MIN, 1).otherwise(0),
      "label_is_delayed_45min" -> when(col("label_arr_delay_filled") >= DELAY_THRESHOLD_45_MIN, 1).otherwise(0),
      "label_is_delayed_60min" -> when(col("label_arr_delay_filled") >= DELAY_THRESHOLD_60_MIN, 1).otherwise(0),
      "label_is_delayed_90min" -> when(col("label_arr_delay_filled") >= DELAY_THRESHOLD_90_MIN, 1).otherwise(0),

      // Labels pour les retards spécifiques
      "label_has_weather_delay" -> when(col("label_weather_delay_filled") > 0, 1).otherwise(0),
      "label_has_nas_delay" -> when(col("label_nas_delay_filled") > 0, 1).otherwise(0),
      "label_has_any_weather_nas_delay" -> when(
        col("label_weather_delay_filled") > 0 || col("label_nas_delay_filled") > 0, 1
      ).otherwise(0),

      // Retard total combiné weather + NAS
      "label_total_weather_nas_delay" -> (col("label_weather_delay_filled") + col("label_nas_delay_filled")),

      // Indicateur de vol à l'heure (aucun retard)
      "label_is_on_time" -> when(col("label_arr_delay_filled") <= 0, 1).otherwise(0),
      "label_is_early" -> when(col("label_arr_delay_filled") < 0, 1).otherwise(0)
    )

    addCalculatedColumns(df, columnExpressions)
  }

}