package com.flightdelay.data.preprocessing.flights

import com.flightdelay.config.AppConfiguration
import com.flightdelay.data.preprocessing.DataPreprocessor
import com.flightdelay.utils.DebugUtils._
import com.flightdelay.utils.MetricsUtils.withUiLabels
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

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
   * @param configuration Configuration de l'application
   * @return DataFrame nettoyé et validé
   */
  override def preprocess(rawFlightData: DataFrame, rawWeatherData: DataFrame, rawWBANAirportTimezoneData: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightDataCleaner.preprocess")

    debug("=" * 80)
    debug("[STEP 2][DataCleaner] Flight Data Cleaning - Start")
    debug("=" * 80)

    whenDebug{
      val originalCount = rawFlightData.count()
      debug(s"Original dataset: $originalCount records")
    }

    // Étape 1: Nettoyage de base (doublons et valeurs nulles)
    val cleanedData = performBasicCleaning(rawFlightData)

    // Étape 2: Filtrage des vols annulés et détournés (selon TIST)
    val filteredData = filterInvalidFlights(cleanedData)


    // Étape 2.5: Filtrage des vols basés sur les stations météo WBAN existantes
    val filteredByWeatherStations = filterFlightsByExistingWeatherStations(filteredData, rawWeatherData)

    // Étape 2.6: Filtrage des vols par les mois couverts par les données météo
    val filteredByCoveredMonths = filterFlightsByCoveredMonths(filteredByWeatherStations, rawWeatherData)

    // Étape 3: Conversion et validation des types de données
    val typedData = convertAndValidateDataTypes(filteredByCoveredMonths)

    // Étape 5: Validation finale
    val finalData = performFinalValidation(typedData)

    // Cleaning summary
    whenDebug {
      logCleaningSummary(rawFlightData, finalData)
    }

    finalData
  }

  /**
   * Nettoyage de base : suppression des doublons et valeurs nulles critiques
   */
  private def performBasicCleaning(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightDataCleaner.performBasicCleaning")
    debug("Phase 1: Basic Cleaning")

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

    debug(s"  - Current count: ${result.count()} records")
    result
  }

  /**
   * Filtrage des vols invalides selon l'article TIST
   */
  private def filterInvalidFlights(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightDataCleaner.filterInvalidFlights")

    debug("Phase 2: Filter Invalid Flights")
    debug("  - Filtering cancelled and diverted flights")

    // Replace NULL with 0 for CANCELLED and DIVERTED
    val dxColumns = Seq("DIVERTED","CANCELLED")
    val dfWithDefaults = df.na.fill(0, dxColumns)

    // Filter cancelled and diverted flights (TIST article methodology)
    val filteredCancelledDiverted = dfWithDefaults
      .filter(col("CANCELLED") === 0 && col("DIVERTED") === 0)
      .drop("CANCELLED", "DIVERTED")

    // Filter invalid departure times
    debug("  - Filtering invalid departure times")
    val validDepartureTimes = filteredCancelledDiverted.filter(
      col("CRS_DEP_TIME").isNotNull &&
        col("CRS_DEP_TIME") >= 0 &&
        col("CRS_DEP_TIME") <= 2359
    )

    // Filter invalid airport IDs
    debug("  - Filtering invalid airports")
    val validAirports = validDepartureTimes.filter(
      col("ORIGIN_AIRPORT_ID") > 0 &&
        col("DEST_AIRPORT_ID") > 0 &&
        col("ORIGIN_AIRPORT_ID") =!= col("DEST_AIRPORT_ID")
    )

    debug(s"  - Current count: ${validAirports.count()} records")
    validAirports
  }

  /**
   * Filtre les vols dont les stations météo WBAN (origine et destination) n'existent pas dans les données météo
   * Utilise des left_semi joins pour garder uniquement les vols avec des stations météo valides
   * @param df DataFrame des vols à filtrer
   * @param spark Session Spark
   * @param configuration Configuration de l'application
   * @return DataFrame filtré contenant uniquement les vols avec des stations météo existantes
   */
  private def filterFlightsByExistingWeatherStations(df: DataFrame, weatherDF: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightDataCleaner.filterFlightsByExistingWeatherStations")
    debug("Phase 2.5: Filter Flights by Existing Weather Stations")

    withUiLabels(
      groupId = "FlightDataCleaner.filterFlightsByExistingWeatherStations",
      desc = "Remove Flights If ORIGIN_WBAN, DEST_WBAN does not exist in Weather",
      tags = "prep,semi-join,wban"
    ) {

      debug("  - Extracting valid WBAN stations from weather data...")

      // 1) WBAN valides côté météo (distinct, non nuls, nettoyés)
      val weatherStations = weatherDF
        .select(trim(col("WBAN")).as("WBAN"))
        .where(col("WBAN").isNotNull && length(col("WBAN")) > 0)
        .distinct()
        .cache()

      whenDebug {
        val stationCount = weatherStations.count()
        debug(s"  - Found ${stationCount} unique weather stations")
      }

      // 2) Prépare les colonnes WBAN côté vols (nettoyage basique)
      val flightsWBAN = df
        .withColumn("ORIGIN_WBAN", trim(col("ORIGIN_WBAN")))
        .withColumn("DEST_WBAN", trim(col("DEST_WBAN")))

      // 3) Comptage avant filtrage



      // 4) Garde uniquement les vols dont ORIGIN_WBAN existe dans la météo
      debug("  - Filtering flights by ORIGIN_WBAN...")
      val originStations = weatherStations
        .select(col("WBAN").as("ORIGIN_WBAN"))

      val flightsHasOrigin = flightsWBAN
        .join(originStations, Seq("ORIGIN_WBAN"), "left_semi")

      // 5) Puis garde uniquement ceux dont DEST_WBAN existe aussi
      debug("  - Filtering flights by DEST_WBAN...")
      val destStations = weatherStations
        .select(col("WBAN").as("DEST_WBAN"))

      val flightDF_filtered = flightsHasOrigin
        .join(destStations, Seq("DEST_WBAN"), "left_semi")

      // 6) Comptage après filtrage et petit bilan
      whenDebug {
        val countBefore = flightsWBAN.count()
        debug(s"  - Flights before filtering: ${countBefore}")

        flightDF_filtered.cache()
        val countAfter = flightDF_filtered.count()
        val removedCount = countBefore - countAfter
        val removalPercent = if (countBefore > 0) (removedCount.toDouble / countBefore * 100).round else 0

        debug(s"  [WBAN filter] Summary:")
        debug(f"    - Flights before:  $countBefore%,10d")
        debug(f"    - Flights after:   $countAfter%,10d")
        debug(f"    - Removed:         $removedCount%,10d ($removalPercent%%)")
      }

      flightDF_filtered
    }
  }

  /**
   * Filtre les vols pour ne garder que ceux des mois couverts par les données météo
   * Utilise un left_semi join basé sur le mois UTC (format yyyy-MM)
   * @param df DataFrame des vols à filtrer
   * @param spark Session Spark
   * @param configuration Configuration de l'application
   * @return DataFrame filtré contenant uniquement les vols des mois avec données météo
   */
  private def filterFlightsByCoveredMonths(df: DataFrame, weatherDF: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    info("- Calling com.flightdelay.data.preprocessing.flights.FlightDataCleaner.filterFlightsByCoveredMonths")
    debug("Phase 2.6: Filter Flights by Covered Months")

    withUiLabels(
      groupId = "FlightDataCleaner.filterFlightsByCoveredMonths",
      desc = "Remove Flights from months not covered by Weather data",
      tags = "prep,semi-join,month-coverage"
    ) {

      debug("  - Extracting covered months from weather data...")

      // Ajouter la colonne month_utc aux vols
      val flightsCoveredMonths = df
        .withColumn("month_utc", date_format(col("UTC_FL_DATE"), "yyyy-MM"))

      // Extraire les mois distincts des données météo
      val weatherDFWithConvertedDates = weatherDF.withColumn("Date", to_date(col("Date"), "yyyyMMdd"))
      val weatherMonths = weatherDFWithConvertedDates
        .withColumn("month_utc", date_format(col("Date"), "yyyy-MM"))
        .select("month_utc")
        .distinct()
        .cache()

      whenDebug {
        val monthCount = weatherMonths.count()
        debug(s"  - Found ${monthCount} distinct months in weather data")
      }

      // Filtrer les vols pour ne garder que ceux des mois couverts
      debug("  - Filtering flights by covered months...")
      val flightDF_mCovered = flightsCoveredMonths
        .join(weatherMonths, Seq("month_utc"), "left_semi")
        .drop("month_utc")  // Supprimer la colonne temporaire
        .cache()


      // Comptage après filtrage et statistiques
      whenDebug{
        val countBefore = flightsCoveredMonths.count()
        debug(s"  - Flights before filtering: ${countBefore}")

        val countAfter = flightDF_mCovered.count()
        val removedCount = countBefore - countAfter
        val coveragePercent = if (countBefore > 0) (countAfter.toDouble * 100.0 / countBefore) else 0.0

        debug(s"  [Month Coverage filter] Summary:")
        debug(f"    - Flights before:     $countBefore%,10d")
        debug(f"    - Flights after:      $countAfter%,10d")
        debug(f"    - Removed:            $removedCount%,10d")
        debug(f"    - Coverage:           $coveragePercent%.2f%%")
      }
      // Nettoyage du cache
      weatherMonths.unpersist()
      flightDF_mCovered
    }
  }

  /**
   * Conversion et validation des types de données
   */
  private def convertAndValidateDataTypes(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    info("- Calling com.flightdelay.data.preprocessing.flights.FlightDataCleaner.convertAndValidateDataTypes")
    debug("Phase 3: Data Type Conversion")

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
    debug("  - Filtering invalid flight dates")
    val validDates = convertedData.filter(col("FL_DATE").isNotNull)

    debug(s"  - Current count: ${validDates.count()} records")
    validDates
  }

  /**
   * Validation finale des données nettoyées
   */
  private def performFinalValidation(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    info("- Calling com.flightdelay.data.preprocessing.flights.FlightDataCleaner.performFinalValidation")
    debug("Phase 5: Final Validation")

    // Vérifier les colonnes essentielles
    val requiredColumns = Seq(
      "FL_DATE", "OP_CARRIER_AIRLINE_ID", "ORIGIN_AIRPORT_ID",
      "DEST_AIRPORT_ID", "CRS_DEP_TIME"
    )

    val missingColumns = requiredColumns.filterNot(df.columns.contains)
    if (missingColumns.nonEmpty) {
      debug(s"  ✗ Missing columns: ${missingColumns.mkString(", ")}")
      throw new RuntimeException(s"Mandatory columns missing: ${missingColumns.mkString(", ")}")
    }

    whenDebug{
      val finalCount = df.count()
      println(s"  - Validation passed: $finalCount records")
    }

    df
  }

  /**
   * Résumé détaillé du processus de nettoyage
   */
  private def logCleaningSummary(originalDf: DataFrame, cleanedDf: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    val originalCount = originalDf.count()
    val cleanedCount = cleanedDf.count()
    val reductionPercent = ((originalCount - cleanedCount).toDouble / originalCount * 100).round

    debug("=" * 50)
    debug("Cleaning Summary")
    debug("=" * 50)
    debug(f"Original records:    $originalCount%,10d")
    debug(f"Final records:       $cleanedCount%,10d")
    debug(f"Removed records:     ${originalCount - cleanedCount}%,10d")
    debug(f"Reduction:           $reductionPercent%3d%%")

    if (reductionPercent > 50) {
      debug(f"WARNING: High reduction rate ($reductionPercent%%)")
    }
    debug("=" * 50)
  }
}
