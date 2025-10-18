package com.flightdelay.data.preprocessing.weather

import com.flightdelay.data.preprocessing.DataPreprocessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Classe spécialisée pour le nettoyage des données météo
 * Responsable du nettoyage, filtrage, normalisation temporelle et validation
 */
object WeatherDataCleaner extends DataPreprocessor {

  /**
   * Nettoyage complet des données météo
   * @param rawWeatherData DataFrame contenant les données météo brutes
   * @param spark Session Spark
   * @return DataFrame nettoyé et validé
   */
  override def preprocess(rawWeatherData: DataFrame)(implicit spark: SparkSession): DataFrame = {

    println("\n" + "=" * 80)
    println("[STEP 2][DataCleaner] Weather Data Cleaning - Start")
    println("=" * 80)

    val originalCount = rawWeatherData.count()
    println(s"\nOriginal dataset: $originalCount records")

    // Étape 1: Nettoyage de base (doublons et valeurs nulles)
    val cleanedData = performBasicCleaning(rawWeatherData)

    // Étape 2: Normalisation temporelle (xx:15 -> xx:00)
    val normalizedTime = normalizeWeatherTime(cleanedData)

    // Étape 3: Conversion et validation des types de données
    val typedData = convertAndValidateDataTypes(normalizedTime)

    // Étape 4: Validation finale
    val finalData = performFinalValidation(typedData)

    // Cleaning summary
    logCleaningSummary(rawWeatherData, finalData)

    finalData
  }

  /**
   * Nettoyage de base : suppression des doublons et valeurs nulles critiques
   */
  private def performBasicCleaning(df: DataFrame): DataFrame = {
    println("\nPhase 1: Basic Cleaning")

    // Colonnes clés pour identifier les doublons
    val keyColumns = Seq("WBAN", "Date", "Time")

    // Supprimer les doublons
    val deduplicated = removeDuplicates(df, keyColumns)

    // Colonnes critiques qui ne peuvent pas être nulles
    val criticalColumns = Seq("WBAN", "Date", "Time")

    // Remove rows with critical null values
    val result = removeNullValues(deduplicated, criticalColumns)

    println(s"  - Current count: ${result.count()} records")
    result
  }

  /**
   * Normalisation temporelle des données météo
   * Filtre pour ne garder que les enregistrements plus proche de à xx:00
   * et transforme xxmm en xx00 pour alignement avec les heures de vol
   */
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._

  private def normalizeWeatherTime(df: DataFrame): DataFrame = {
    println("\nPhase 2: Weather Time Normalization")
    println("  - Keeping only the closest record to HH:00 for each hour")
    println("  - Normalizing selected times to HH:00")

    val countBefore = df.count()

    // Étape 1 : Extraire l'heure (partie HH de Time)
    val dfWithHour = df.withColumn(
      "hour",
      (col("Time").cast("int") / 100).cast("int")
    )

    // Étape 2 : Calculer la distance en minutes par rapport à HH:00
    val dfWithDistance = dfWithHour.withColumn(
      "distance_to_hour",
      abs(col("Time").cast("int") % 100)  // Minutes après l'heure
    )

    // Étape 3 : Créer une fenêtre pour chaque (WBAN, Date, Hour)
    val window = Window
      .partitionBy("WBAN", "Date", "hour")
      .orderBy("distance_to_hour")

    // Étape 4 : Attribuer un rang (1 = le plus proche de HH:00)
    val dfWithRank = dfWithDistance
      .withColumn("rank", row_number().over(window))

    // Étape 5 : Garder seulement le plus proche et normaliser à HH:00
    val result = dfWithRank
      .filter(col("rank") === 1)
      .withColumn("Time", format_string("%04d", col("hour") * 100))
      .drop("hour", "distance_to_hour", "rank")

    val countAfter = result.count()
    val filteredOut = countBefore - countAfter

    println(s"  - Records before filtering: $countBefore")
    println(s"  - Records after filtering: $countAfter")
    println(s"  - Filtered out: $filteredOut (${(filteredOut * 100.0 / countBefore).round}%)")

    result
  }

  /**
   * Conversion et validation des types de données
   */
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

    val convertedData = convertDataTypes(df, typeMapping)

    // Convert Date from YYYYMMDD string format to proper Date type
    println("  - Converting Date from YYYYMMDD to Date type")
    val withDateConverted = convertedData
      .withColumn("Date", to_date(col("Date"), "yyyyMMdd"))

    println(s"  - Current count: ${withDateConverted.count()} records")
    withDateConverted
  }

  /**
   * Validation finale des données nettoyées
   */
  private def performFinalValidation(df: DataFrame): DataFrame = {
    println("\nPhase 4: Final Validation")

    // Vérifier les colonnes essentielles
    val requiredColumns = Seq("WBAN", "Date", "Time")

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
