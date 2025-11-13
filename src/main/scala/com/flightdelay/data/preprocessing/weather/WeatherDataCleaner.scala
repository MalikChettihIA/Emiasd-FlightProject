package com.flightdelay.data.preprocessing.weather

import com.flightdelay.config.AppConfiguration
import com.flightdelay.data.preprocessing.DataPreprocessor
import com.flightdelay.utils.MetricsUtils.withUiLabels
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Classe spécialisée pour le nettoyage des données météo
 * Responsable du nettoyage, filtrage, normalisation temporelle et validation
 */
object WeatherDataCleaner extends DataPreprocessor {

  override def preprocess(rawWeatherData: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration = null): DataFrame = {
    println("\n" + "=" * 80)
    println("[STEP 2][DataCleaner] Weather Data Cleaning - Start")
    println("=" * 80)

    val cleanedData    = performBasicCleaning(rawWeatherData)
    val filteredByWBAN = filterWeatherByFlightWBANs(cleanedData)
    val normalizedTime = normalizeWeatherTime(filteredByWBAN)
    val typedData      = convertAndValidateDataTypes(normalizedTime)
    val finalData      = performFinalValidation(typedData)

    finalData
  }

  /** Phase 1: suppression des doublons et valeurs nulles critiques */
  private def performBasicCleaning(df: DataFrame): DataFrame = {
    println("\nPhase 1: Basic Cleaning")
    val keyColumns      = Seq("WBAN", "Date", "Time")
    val deduplicated    = removeDuplicates(df, keyColumns)
    val criticalColumns = Seq("WBAN", "Date", "Time")
    val result          = removeNullValues(deduplicated, criticalColumns)
    println(s"  - Current count: ${result.count()} records")
    result
  }

  /**
   * Filtre les données météo pour ne garder que les stations WBAN utilisées par les vols
   * Utilise un left_semi join basé sur les WBAN d'origine et de destination des vols
   * @param df DataFrame des données météo à filtrer
   * @param spark Session Spark
   * @param configuration Configuration de l'application
   * @return DataFrame filtré contenant uniquement les stations WBAN référencées par les vols
   */
  private def filterWeatherByFlightWBANs(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    println("\nPhase 1.5: Filter Weather by Flight WBANs")

    withUiLabels(
      groupId = "Filter-Weather-By-Flight-WBANs",
      desc = "Remove Weather stations not referenced by any flights",
      tags = "prep,semi-join,wban"
    ) {

      println("  - Loading flight data from parquet...")

      // Charger les données de vols brutes depuis le parquet
      val rawFlightPath = s"${configuration.common.output.basePath}/common/data/processed_flights.parquet"
      val flightDF = spark.read.parquet(rawFlightPath)

      println("  - Extracting WBAN stations used by flights...")

      // 1) Extraire les WBAN d'origine
      val originWBANs = flightDF
        .select(trim(col("ORIGIN_WBAN")).as("WBAN"))
        .where(col("WBAN").isNotNull && length(col("WBAN")) > 0)

      // 2) Extraire les WBAN de destination
      val destWBANs = flightDF
        .select(trim(col("DEST_WBAN")).as("WBAN"))
        .where(col("WBAN").isNotNull && length(col("WBAN")) > 0)

      // 3) Union et distinct pour obtenir tous les WBAN référencés
      val flightWBANs = originWBANs
        .unionByName(destWBANs)
        .distinct()
        .cache()

      val flightWBANCount = flightWBANs.count()
      println(s"  - Found ${flightWBANCount} unique WBAN stations referenced by flights")

      // 4) Comptage avant filtrage
      val countBefore = df.count()
      println(s"  - Weather records before filtering: ${countBefore}")

      // 5) Filtrer les données météo pour ne garder que les WBAN utilisés
      println("  - Filtering weather data by flight WBANs...")
      val weatherDF_pruned = df
        .withColumn("WBAN", trim(col("WBAN")))
        .where(col("WBAN").isNotNull && length(col("WBAN")) > 0)
        .join(flightWBANs, Seq("WBAN"), "left_semi")
        .cache()

      // 6) Comptage après filtrage et statistiques
      val countAfter = weatherDF_pruned.count()
      val removedCount = countBefore - countAfter
      val retentionPercent = if (countBefore > 0) (countAfter.toDouble * 100.0 / countBefore) else 0.0

      println(s"\n  [Weather WBAN filter] Summary:")
      println(f"    - Weather records before:  $countBefore%,10d")
      println(f"    - Weather records after:   $countAfter%,10d")
      println(f"    - Removed:                 $removedCount%,10d")
      println(f"    - Retention:               $retentionPercent%.2f%%")

      // Nettoyage du cache
      flightWBANs.unpersist()

      weatherDF_pruned
    }
  }

  /** Phase 2: normalisation temporelle HH:mm -> enregistrement le plus proche de HH:00 */
  import org.apache.spark.sql.expressions.Window
  private def normalizeWeatherTime(df: DataFrame): DataFrame = {
    println("\nPhase 2: Weather Time Normalization")
    println("  - Keeping only the closest record to HH:00 for each hour")
    println("  - Normalizing selected times to HH:00")

    val dfWithHour = df.withColumn("hour", (col("Time").cast("int") / 100).cast("int"))
    val dfWithDistance = dfWithHour.withColumn("distance_to_hour", abs(col("Time").cast("int") % 100))
    val window = Window.partitionBy("WBAN", "Date", "hour").orderBy(col("distance_to_hour"))

    val result = dfWithDistance
      .withColumn("rank", row_number().over(window))
      .filter(col("rank") === 1)
      .withColumn("Time", format_string("%04d", col("hour") * 100))
      .drop("hour", "distance_to_hour", "rank")

    result
  }

  /** Phase 3: conversions de types + nettoyage des codes "NULL" → null et cast en Int */
  private def convertAndValidateDataTypes(df: DataFrame): DataFrame = {
    println("\nPhase 3: Data Type Conversion")

    val typeMapping = Map(
      "WBAN" -> StringType,
      "Time" -> StringType,
      "StationType" -> IntegerType,
      "Visibility" -> DoubleType,
      "DryBulbFarenheit" -> DoubleType,
      "DryBulbCelsius" -> DoubleType,
      "WetBulbFarenheit" -> DoubleType,
      "WetBulbCelsius" -> DoubleType,
      "DewPointFarenheit" -> DoubleType,
      "DewPointCelsius" -> DoubleType,
      "RelativeHumidity" -> DoubleType,
      "WindSpeed" -> DoubleType,
      "WindDirection" -> DoubleType,
      "StationPressure" -> DoubleType,
      "Altimeter" -> DoubleType
    )

    // 3.1 — cast simple des colonnes numériques usuelles
    val convertedData = convertDataTypes(df, typeMapping)

    // 3.2 — Nettoyage précipitations horaires: 'T' (trace) -> 0.0
    println("  - Cleaning HourlyPrecip: Converting 'T' (trace) to 0.0")
    val withCleanedPrecip = convertedData.withColumn(
      "HourlyPrecip",
      when(trim(col("HourlyPrecip")) === "T", lit("0.0"))
        .otherwise(col("HourlyPrecip"))
        .cast(DoubleType)
    )

    // 3.3 — Pression au niveau de la mer: 'M' (missing) -> null puis cast double
    println("  - Cleaning SeaLevelPressure: Converting 'M' (missing) to null")
    val withCleanedPressure = withCleanedPrecip.withColumn(
      "SeaLevelPressure",
      when(trim(col("SeaLevelPressure")) === "M", lit(null).cast(StringType))
        .otherwise(col("SeaLevelPressure"))
        .cast(DoubleType)
    )

    // 3.4 — Codes entiers stockés en string -> null-safe cast en Int
    //       on traite ici les colonnes sujettes à l’erreur VectorAssembler
    val codeIntCols = Seq(
      "PressureTendency",
      "ValueForWindCharacter"
    ).filter(withCleanedPressure.columns.contains)

    val withCodesAsInt = codeIntCols.foldLeft(withCleanedPressure){ (acc, c) =>
      println(s"  - Normalizing code column '$c': 'NULL'/empty -> null, cast to Int")
      acc.withColumn(
        c,
        when(trim(col(c)).isin("", "NULL"), lit(null).cast(StringType))
          .otherwise(regexp_replace(col(c), "[^0-9-]", "")) // garde chiffres/signe
          .cast(IntegerType)
      )
    }

    // 3.5 — Date: yyyyMMdd -> Date
    println("  - Converting Date from YYYYMMDD to Date type")
    val withDateConverted = withCodesAsInt.withColumn("Date", to_date(col("Date"), "yyyyMMdd"))

    withDateConverted
  }

  /** Phase 4: validation finale */
  private def performFinalValidation(df: DataFrame): DataFrame = {
    println("\nPhase 4: Final Validation")
    val requiredColumns = Seq("WBAN", "Date", "Time")
    val missingColumns  = requiredColumns.filterNot(df.columns.contains)
    if (missingColumns.nonEmpty)
      throw new RuntimeException(s"Mandatory columns missing: ${missingColumns.mkString(", ")}")
    df
  }

}