package com.flightdelay.data.preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Classe spécialisée pour le nettoyage de premier niveau des données de vols
 * Responsable du nettoyage, filtrage, validation et conversion de types
 * Implémente la phase "Data preprocessing" de l'article TIST
 */
object FlightDataCleaner extends DataPreprocessor {

  /**
   * Nettoyage complet des données de vols
   * @param rawFlightData DataFrame contenant les données de vols brutes
   * @param spark Session Spark
   * @return DataFrame nettoyé et validé
   */
  override def preprocess(rawFlightData: DataFrame)(implicit spark: SparkSession): DataFrame = {

    println("\n" + "=" * 80)
    println("[STEP 2][DataCleaner] Flight Data Cleaning - Start")
    println("=" * 80)

    val originalCount = rawFlightData.count()
    println(s"\nOriginal dataset: $originalCount records")

    // Étape 1: Nettoyage de base (doublons et valeurs nulles)
    val cleanedData = performBasicCleaning(rawFlightData)

    // Étape 2: Filtrage des vols annulés et détournés (selon TIST)
    val filteredData = filterInvalidFlights(cleanedData)

    // Étape 3: Conversion et validation des types de données
    val typedData = convertAndValidateDataTypes(filteredData)

    // Étape 4: Nettoyage des valeurs aberrantes
    val cleanedOutliers = cleanFlightOutliers(typedData)

    // Étape 5: Validation finale
    val finalData = performFinalValidation(cleanedOutliers)

    // Cleaning summary
    logCleaningSummary(rawFlightData, finalData)


    finalData
  }

  /**
   * Nettoyage de base : suppression des doublons et valeurs nulles critiques
   */
  private def performBasicCleaning(df: DataFrame): DataFrame = {
    println("\nPhase 1: Basic Cleaning")

    // Colonnes clés pour identifier les doublons
    val keyColumns = Seq(
      "FL_DATE", "OP_CARRIER_AIRLINE_ID", "OP_CARRIER_FL_NUM",
      "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME"
    )

    // Supprimer les doublons
    val deduplicated = removeDuplicates(df, keyColumns)

    // Colonnes critiques qui ne peuvent pas être nulles
    val criticalColumns = Seq(
      "FL_DATE", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME"
    )

    // Remove rows with critical null values
    val result = removeNullValues(deduplicated, criticalColumns)

    println(s"  - Current count: ${result.count()} records")
    result
  }

  /**
   * Filtrage des vols invalides selon l'article TIST
   */
  private def filterInvalidFlights(df: DataFrame): DataFrame = {
    println("\nPhase 2: Filter Invalid Flights")
    println("  - Filtering cancelled and diverted flights")

    // Replace NULL with 0 for CANCELLED and DIVERTED
    val dfWithDefaults = df
      .withColumn("CANCELLED", coalesce(col("CANCELLED"), lit(0)))
      .withColumn("DIVERTED", coalesce(col("DIVERTED"), lit(0)))

    // Filter cancelled and diverted flights (TIST article methodology)
    val filteredCancelledDiverted = dfWithDefaults
      .filter(col("CANCELLED") === 0 && col("DIVERTED") === 0)
      .drop("CANCELLED", "DIVERTED")

    // Filter invalid departure times
    println("  - Filtering invalid departure times")
    val validDepartureTimes = filteredCancelledDiverted.filter(
      col("CRS_DEP_TIME").isNotNull &&
        col("CRS_DEP_TIME") >= 0 &&
        col("CRS_DEP_TIME") <= 2359
    )

    // Filter invalid airport IDs
    println("  - Filtering invalid airports")
    val validAirports = validDepartureTimes.filter(
      col("ORIGIN_AIRPORT_ID") > 0 &&
        col("DEST_AIRPORT_ID") > 0 &&
        col("ORIGIN_AIRPORT_ID") =!= col("DEST_AIRPORT_ID")
    )

    println(s"  - Current count: ${validAirports.count()} records")
    validAirports
  }

  /**
   * Conversion et validation des types de données
   */
  private def convertAndValidateDataTypes(df: DataFrame): DataFrame = {
    println("\nPhase 3: Data Type Conversion")

    val typeMapping = Map(
      "FL_DATE" -> DateType,
      "OP_CARRIER_AIRLINE_ID" -> IntegerType,
      "OP_CARRIER_FL_NUM" -> IntegerType,
      "ORIGIN_AIRPORT_ID" -> IntegerType,
      "DEST_AIRPORT_ID" -> IntegerType,
      "CRS_DEP_TIME" -> IntegerType,
      "ARR_DELAY_NEW" -> DoubleType,
      "CRS_ELAPSED_TIME" -> DoubleType,
      "WEATHER_DELAY" -> DoubleType,
      "NAS_DELAY" -> DoubleType
    )

    val convertedData = convertDataTypes(df, typeMapping)

    // Validate date format
    println("  - Filtering invalid flight dates")
    val validDates = convertedData.filter(col("FL_DATE").isNotNull)

    println(s"  - Current count: ${validDates.count()} records")
    validDates
  }

  /**
   * Nettoyage des valeurs aberrantes spécifiques aux vols
   */
  private def cleanFlightOutliers(df: DataFrame): DataFrame = {
    println("\nPhase 4: Outlier Filtering")

    // Filter extreme delays (> 10 hours = 600 minutes)
    println("  - Filtering delays > 600 minutes")
    val reasonableDelays = df.filter(
      col("ARR_DELAY_NEW").isNull ||
        col("ARR_DELAY_NEW") <= 600
    )

    // Filter invalid flight times (between 10 minutes and 24 hours)
    println("  - Filtering flight times (10 min - 24 hours)")
    val validFlightTimes = reasonableDelays.filter(
      col("CRS_ELAPSED_TIME").isNull ||
        (col("CRS_ELAPSED_TIME") >= 10 && col("CRS_ELAPSED_TIME") <= 1440)
    )

    println(s"  - Current count: ${validFlightTimes.count()} records")
    validFlightTimes
  }

  /**
   * Validation finale des données nettoyées
   */
  private def performFinalValidation(df: DataFrame): DataFrame = {
    println("\nPhase 5: Final Validation")

    // Vérifier les colonnes essentielles
    val requiredColumns = Seq(
      "FL_DATE", "OP_CARRIER_AIRLINE_ID", "ORIGIN_AIRPORT_ID",
      "DEST_AIRPORT_ID", "CRS_DEP_TIME"
    )

    val missingColumns = requiredColumns.filterNot(df.columns.contains)
    if (missingColumns.nonEmpty) {
      println(s"  ✗ Missing columns: ${missingColumns.mkString(", ")}")
      throw new RuntimeException(s"Mandatory columns missing: ${missingColumns.mkString(", ")}")
    }

    val finalCount = df.count()
    println(s"  - Validation passed: $finalCount records")
    df
  }

  /**
   * Gestion des valeurs manquantes pour les colonnes de retard
   */
  def handleMissingDelayValues(df: DataFrame): DataFrame = {
    println("Handling missing delay values")

    val columnExpressions = Map(
      // Remplacer les valeurs nulles par 0 pour les retards
      "arr_delay_filled" -> when(col("ARR_DELAY_NEW").isNull, 0.0).otherwise(col("ARR_DELAY_NEW")),
      "weather_delay_filled" -> when(col("WEATHER_DELAY").isNull, 0.0).otherwise(col("WEATHER_DELAY")),
      "nas_delay_filled" -> when(col("NAS_DELAY").isNull, 0.0).otherwise(col("NAS_DELAY")),

      // Indicateurs de valeurs manquantes (utiles pour l'analyse)
      "arr_delay_was_null" -> when(col("ARR_DELAY_NEW").isNull, 1).otherwise(0),
      "weather_delay_was_null" -> when(col("WEATHER_DELAY").isNull, 1).otherwise(0),
      "nas_delay_was_null" -> when(col("NAS_DELAY").isNull, 1).otherwise(0)
    )

    addCalculatedColumns(df, columnExpressions)
  }

  /**
   * Résumé détaillé du processus de nettoyage
   */
  private def logCleaningSummary(originalDf: DataFrame, cleanedDf: DataFrame): Unit = {
    val originalCount = originalDf.count()
    val cleanedCount = cleanedDf.count()
    val reductionPercent = ((originalCount - cleanedCount).toDouble / originalCount * 100).round

    println("\n" + "=" * 50)
    println("Cleaning Summary")
    println("=" * 50)
    println(f"Original records:    $originalCount%,10d")
    println(f"Final records:       $cleanedCount%,10d")
    println(f"Removed records:     ${originalCount - cleanedCount}%,10d")
    println(f"Reduction:           $reductionPercent%3d%%")

    if (reductionPercent > 50) {
      println(f"\n⚠ WARNING: High reduction rate ($reductionPercent%%)")
    }
    println("=" * 50)
  }
}
