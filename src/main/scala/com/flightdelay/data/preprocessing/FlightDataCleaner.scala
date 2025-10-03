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

    println("")
    println("")
    println("----------------------------------------------------------------------------------------------------------")
    println("--> [FlightDataCleaner] Flight Data Cleaner - Start ...")
    println("----------------------------------------------------------------------------------------------------------")

    val originalCount = rawFlightData.count()
    println(s"Data Original Count: $originalCount")

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

    // Résumé du nettoyage
    logCleaningSummary(rawFlightData, finalData)

    println("--> [FlightDataCleaner] Flight Data Cleaner- End ...")
    println("----------------------------------------------------------------------------------------------------------")
    println("")
    println("")

    finalData
  }

  /**
   * Nettoyage de base : suppression des doublons et valeurs nulles critiques
   */
  private def performBasicCleaning(df: DataFrame): DataFrame = {
    println("")
    println("Phase 1: Basic Cleaning - Remove Duplicates")

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

    // Supprimer les lignes avec des valeurs null critiques
    val result = removeNullValues(deduplicated, criticalColumns)

    println(s"Current Count : ${result.count()}")
    result
  }

  /**
   * Filtrage des vols invalides selon l'article TIST
   */
  private def filterInvalidFlights(df: DataFrame): DataFrame = {
    println("")
    println("Phase 2: Filter Flights")
    println("- Filter Cancelled and Diverted Flights")
    // Selon l'article TIST, on exclut les vols annulés et détournés
    val exclusions = Map(
      "CANCELLED" -> Seq(1.0),
      "DIVERTED" -> Seq(1.0)
    )

    val filteredCancelledDiverted = removeSpecificValues(df, exclusions)

    // Filtrer les vols avec des heures de départ invalides
    println("- Filter Invalid departure time")
    val validDepartureTimes = filteredCancelledDiverted.filter(
      col("CRS_DEP_TIME").isNotNull &&
        col("CRS_DEP_TIME") >= 0 &&
        col("CRS_DEP_TIME") <= 2359
    )

    // Filtrer les IDs d'aéroports invalides (doivent être positifs)
    println("- Filter Invalid airports")
    val validAirports = validDepartureTimes.filter(
      col("ORIGIN_AIRPORT_ID") > 0 &&
        col("DEST_AIRPORT_ID") > 0 &&
        col("ORIGIN_AIRPORT_ID") =!= col("DEST_AIRPORT_ID")
    )

    println(s"Current Count : ${validAirports.count()}")
    validAirports
  }

  /**
   * Conversion et validation des types de données
   */
  private def convertAndValidateDataTypes(df: DataFrame): DataFrame = {
    println("")
    println("Phase 3: Types Conversion")

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

    // Validation du format de date (vérifier que la date n'est pas nulle)
    println("- Filter Invalid flight dates")
    val validDates = convertedData.filter(col("FL_DATE").isNotNull)

    println(s"Current Count : ${validDates.count()}")
    validDates
  }

  /**
   * Nettoyage des valeurs aberrantes spécifiques aux vols
   */
  private def cleanFlightOutliers(df: DataFrame): DataFrame = {
    println("")
    println("Phase 4: Filter Outliers")

    // Filtrer les retards extrêmes (> 10 heures = 600 minutes)
    println("- Filter delay > 10 hours = 600 minutes")
    val reasonableDelays = df.filter(
      col("ARR_DELAY_NEW").isNull ||
        col("ARR_DELAY_NEW") <= 600
    )

    // Filtrer les temps de vol invalides (entre 10 minutes et 24 heures)
    println("- Filter filght time > entre 10 minutes et 24 hours")
    val validFlightTimes = reasonableDelays.filter(
      col("CRS_ELAPSED_TIME").isNull ||
        (col("CRS_ELAPSED_TIME") >= 10 && col("CRS_ELAPSED_TIME") <= 1440)
    )


    // Filtrer les retards météo/NAS aberrants
    //val validWeatherDelays = validFlightTimes.filter(
    // (col("WEATHER_DELAY").isNull || col("WEATHER_DELAY") >= 0) &&
    //    (col("NAS_DELAY").isNull || col("NAS_DELAY") >= 0)
    //)

    println(s"Current Count : ${validFlightTimes.count()}")
    validFlightTimes
  }

  /**
   * Validation finale des données nettoyées
   */
  private def performFinalValidation(df: DataFrame): DataFrame = {
    println("")
    println("Phase 5: Final Validation")

    // Vérifier les colonnes essentielles
    val requiredColumns = Seq(
      "FL_DATE", "OP_CARRIER_AIRLINE_ID", "ORIGIN_AIRPORT_ID",
      "DEST_AIRPORT_ID", "CRS_DEP_TIME"
    )

    val missingColumns = requiredColumns.filterNot(df.columns.contains)
    if (missingColumns.nonEmpty) {
      println(s"Missing columns after cleaning: ${missingColumns.mkString(", ")}")
      throw new RuntimeException(s"Mandatory columns missing : ${missingColumns.mkString(", ")}")
    }

    // Vérifier qu'il reste suffisamment de données
    val finalCount = df.count()

    println(s"Final Validation succeeded: ${finalCount} flights")
    df
  }

  /**
   * Gestion des valeurs manquantes pour les colonnes de retard
   */
  def handleMissingDelayValues(df: DataFrame): DataFrame = {
    println("Gestion des valeurs manquantes pour les retards")

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

    println("")
    println("=== Flight Cleaning Summary ===")
    println(s"Original Count: $originalCount")
    println(s"Final Count: $cleanedCount")
    println(s"Cleaned: ${originalCount - cleanedCount}")
    println(s"Reduction Percentage: $reductionPercent%")

    if (reductionPercent > 50) {
      println(s"[WARN] Import Reduction: $reductionPercent%")
    }
    println("")
  }
}
