package com.flightdelay.data.preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp
import java.text.SimpleDateFormat

/**
 * Preprocessor spécialisé pour les données de vols
 * Implémente le preprocessing selon l'article TIST sur la prédiction des retards de vols
 *
 */
object FlightDataPreprocessor extends DataPreprocessor {

  // Configuration des seuils de retard selon l'article TIST
  private val DELAY_THRESHOLD_15_MIN = 15.0
  private val DELAY_THRESHOLD_60_MIN = 60.0

  // Codes de retard liés à la météorologie et au NAS selon l'article
  private val WEATHER_RELATED_CODES = Seq("WeatherDelay", "NASDelay")

  /**
   * Preprocessing principal des données de vols
   * @param spark Session Spark
   * @param rawFlightData DataFrame contenant les données de vols brutes
   * @return DataFrame préprocessé et enrichi
   */
  override def preprocess(rawFlightData: DataFrame)(implicit spark: SparkSession): DataFrame = {
    println("--> Starting Flight data preprocessing ...")

    val originalCount = rawFlightData.count()
    println(s"Original Flight count: $originalCount")

    // Étape 1: Nettoyage de base
    println("--> Cleaning Duplicates and Null Values ...")
    val cleaned = basicCleaning(rawFlightData)
    val cleanedCount = cleaned.count()
    println("--> Cleaning Duplicates and Null Values : "+ (originalCount-cleanedCount) + " cleaned.")

    // Étape 2: Filtrage des vols annulés et détournés (selon l'article TIST)
    //val filtered = filterCancelledAndDiverted(cleaned)

    // Étape 3: Conversion et validation des types de données
    //val typedData = convertFlightDataTypes(filtered)

    // Étape 4: Enrichissement avec les colonnes calculées
    //val enriched = addFlightFeatures(typedData)

    // Étape 5: Création des labels de classification selon différents seuils
    //val labeled = addDelayClassificationLabels(enriched)

    // Étape 6: Filtrage des retards liés à la météo (selon l'article)
    //val weatherRelated = filterWeatherRelatedDelays(labeled)

    // Étape 7: Nettoyage final et validation
    //val finalData = finalCleaning(weatherRelated)

    // Résumé du preprocessing
    val finalData = cleaned
    //logPreprocessingSummary(rawFlightData, finalData)
    //validateFlightData(finalData)

    logger.info("--> Flight data preprocessed ...")
    finalData
  }

  /**
   * Nettoyage de base des données de vols
   */
  private def basicCleaning(df: DataFrame): DataFrame = {
    logger.info("Nettoyage de base des données de vols")

    // Supprimer les doublons basés sur des colonnes clés
    val keyColumns = Seq("FL_DATE", "OP_CARRIER_AIRLINE_ID", "OP_CARRIER_FL_NUM",
      "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME")
    val deduplicated = removeDuplicates(df, keyColumns)

    // Supprimer les lignes avec des valeurs null critiques
    val criticalColumns = Seq("FL_DATE", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME")
    removeNullValues(deduplicated, criticalColumns)
  }

  /**
   * Filtre les vols annulés et détournés selon l'article TIST
   */
  private def filterCancelledAndDiverted(df: DataFrame): DataFrame = {
    logger.info("Filtrage des vols annulés et détournés")

    val exclusions = Map(
      "CANCELLED" -> Seq(1.0),
      "DIVERTED" -> Seq(1.0)
    )

    removeSpecificValues(df, exclusions)
  }

  /**
   * Conversion et validation des types de données spécifiques aux vols
   */
  private def convertFlightDataTypes(df: DataFrame): DataFrame = {
    logger.info("Conversion des types de données de vols")

    val typeMapping = Map(
      "FL_DATE" -> StringType,
      "OP_CARRIER_AIRLINE_ID" -> IntegerType,
      "OP_CARRIER_FL_NUM" -> StringType,
      "ORIGIN_AIRPORT_ID" -> IntegerType,
      "DEST_AIRPORT_ID" -> IntegerType,
      "CRS_DEP_TIME" -> StringType,
      "ARR_DELAY_NEW" -> DoubleType,
      "CANCELLED" -> DoubleType,
      "DIVERTED" -> DoubleType,
      "CRS_ELAPSED_TIME" -> DoubleType,
      "WEATHER_DELAY" -> DoubleType,
      "NAS_DELAY" -> DoubleType
    )

    convertDataTypes(df, typeMapping)
  }

  /**
   * Ajoute des caractéristiques dérivées pour les vols
   */
  private def addFlightFeatures(df: DataFrame): DataFrame = {
    logger.info("Ajout des caractéristiques de vol")

    val columnExpressions = Map(
      // Conversion de la date en timestamp
      "flight_timestamp" -> to_timestamp(col("FL_DATE"), "yyyy-MM-dd"),

      // Extraction des composants temporels
      "flight_year" -> year(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),
      "flight_month" -> month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),
      "flight_day_of_week" -> dayofweek(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),
      "flight_day_of_year" -> dayofyear(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),

      // Conversion de l'heure de départ
      "departure_hour" -> (col("CRS_DEP_TIME").cast(IntegerType) / 100).cast(IntegerType),
      "departure_minute" -> (col("CRS_DEP_TIME").cast(IntegerType) % 100).cast(IntegerType),

      // Indicateurs de période (selon l'article, certaines périodes sont plus sujettes aux retards)
      "is_weekend" -> when(dayofweek(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(1, 7), 1).otherwise(0),
      "is_holiday_season" -> when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(11, 12, 1), 1).otherwise(0),
      "is_summer" -> when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(6, 7, 8), 1).otherwise(0),

      // Indicateurs de période de la journée (rush hours)
      "is_morning_rush" -> when(col("departure_hour").between(6, 9), 1).otherwise(0),
      "is_evening_rush" -> when(col("departure_hour").between(17, 20), 1).otherwise(0),
      "is_night_flight" -> when(col("departure_hour").between(22, 6), 1).otherwise(0),

      // Distance calculée (proxy basé sur les temps de vol)
      "estimated_distance_category" -> when(col("CRS_ELAPSED_TIME") <= 90, "short")
        .when(col("CRS_ELAPSED_TIME") <= 240, "medium")
        .otherwise("long"),

      // Gestion des valeurs manquantes pour les retards
      "arr_delay_filled" -> when(col("ARR_DELAY_NEW").isNull, 0.0).otherwise(col("ARR_DELAY_NEW")),
      "weather_delay_filled" -> when(col("WEATHER_DELAY").isNull, 0.0).otherwise(col("WEATHER_DELAY")),
      "nas_delay_filled" -> when(col("NAS_DELAY").isNull, 0.0).otherwise(col("NAS_DELAY"))
    )

    addCalculatedColumns(df, columnExpressions)
  }

  /**
   * Ajoute les labels de classification selon différents seuils de retard
   * Implémente la stratégie D1, D2, D3, D4 de l'article TIST
   */
  private def addDelayClassificationLabels(df: DataFrame): DataFrame = {
    logger.info("Ajout des labels de classification des retards")

    val columnExpressions = Map(
      // Label binaire basique (retard >= 15 minutes)
      "is_delayed_15min" -> when(col("arr_delay_filled") >= DELAY_THRESHOLD_15_MIN, 1).otherwise(0),
      "is_delayed_60min" -> when(col("arr_delay_filled") >= DELAY_THRESHOLD_60_MIN, 1).otherwise(0),

      // Labels selon l'article TIST pour différents datasets
      // D1: Retards dus uniquement à la météo extrême ou NAS
      "label_d1" -> when(
        (col("weather_delay_filled") > 0 || col("nas_delay_filled") > 0) &&
          col("arr_delay_filled") >= DELAY_THRESHOLD_15_MIN, 1
      ).otherwise(0),

      // D2: Météo extrême + NAS >= seuil
      "label_d2" -> when(
        col("weather_delay_filled") > 0 ||
          col("nas_delay_filled") >= DELAY_THRESHOLD_15_MIN, 1
      ).otherwise(0),

      // D3: Météo extrême + NAS (même si < seuil)
      "label_d3" -> when(
        col("weather_delay_filled") > 0 || col("nas_delay_filled") > 0, 1
      ).otherwise(0),

      // Catégories de retard pour analyse plus fine
      "delay_category" -> when(col("arr_delay_filled") <= 0, "on_time")
        .when(col("arr_delay_filled") <= 15, "minor_delay")
        .when(col("arr_delay_filled") <= 60, "moderate_delay")
        .when(col("arr_delay_filled") <= 180, "major_delay")
        .otherwise("extreme_delay"),

      // Indicateur de retard lié à la météo
      "has_weather_delay" -> when(col("weather_delay_filled") > 0, 1).otherwise(0),
      "has_nas_delay" -> when(col("nas_delay_filled") > 0, 1).otherwise(0),

      // Score de sévérité du retard (normalisé)
      "delay_severity_score" -> when(col("arr_delay_filled") <= 0, 0.0)
        .otherwise(least(col("arr_delay_filled") / 300.0, lit(1.0)))
    )

    addCalculatedColumns(df, columnExpressions)
  }

  /**
   * Filtre les vols avec retards liés à la météorologie selon l'article TIST
   */
  private def filterWeatherRelatedDelays(df: DataFrame): DataFrame = {
    logger.info("Filtrage des retards liés à la météo")

    // Selon l'article, on se concentre sur les retards ayant une composante météo ou NAS
    val filtered = df.filter(
      col("has_weather_delay") === 1 ||
        col("has_nas_delay") === 1 ||
        col("arr_delay_filled") <= 0  // Inclure aussi les vols à l'heure pour l'équilibrage
    )

    logger.info(s"Vols après filtrage météo: ${filtered.count()}")
    filtered
  }

  /**
   * Nettoyage final et optimisations
   */
  private def finalCleaning(df: DataFrame): DataFrame = {
    logger.info("Nettoyage final des données de vols")

    // Supprimer les outliers extrêmes de retard (> 10 heures = 600 minutes)
    val withoutExtremeOutliers = df.filter(col("arr_delay_filled") <= 600)

    // Supprimer les temps de vol invalides
    val validFlightTimes = withoutExtremeOutliers.filter(
      col("CRS_ELAPSED_TIME").isNotNull &&
        col("CRS_ELAPSED_TIME") > 0 &&
        col("CRS_ELAPSED_TIME") <= 1440  // Max 24 heures
    )

    // Validation finale des heures de départ
    val validDepartureTimes = validFlightTimes.filter(
      col("departure_hour").between(0, 23) &&
        col("departure_minute").between(0, 59)
    )

    validDepartureTimes
  }

  /**
   * Validation spécifique des données de vols
   */
  private def validateFlightData(df: DataFrame): Boolean = {
    logger.info("Validation des données de vols après preprocessing")

    val requiredColumns = Seq(
      "FL_DATE", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID",
      "is_delayed_15min", "is_delayed_60min", "label_d1", "label_d2", "label_d3"
    )

    if (!validateDataQuality(df, requiredColumns)) {
      return false
    }

    // Vérifications spécifiques aux vols
    val totalFlights = df.count()
    val delayedFlights15 = df.filter(col("is_delayed_15min") === 1).count()
    val delayedFlights60 = df.filter(col("is_delayed_60min") === 1).count()

    val delayRate15 = (delayedFlights15.toDouble / totalFlights * 100).round
    val delayRate60 = (delayedFlights60.toDouble / totalFlights * 100).round

    logger.info(s"Taux de retard >= 15min: $delayRate15%")
    logger.info(s"Taux de retard >= 60min: $delayRate60%")

    // Vérifier que les taux sont dans des plages réalistes
    if (delayRate15 < 5 || delayRate15 > 50) {
      logger.warn(s"Taux de retard 15min suspect: $delayRate15%")
    }

    true
  }

  /**
   * Créer des datasets balancés pour l'entraînement selon l'article TIST
   */
  def createBalancedDatasets(df: DataFrame, spark: SparkSession): Map[String, DataFrame] = {
    logger.info("Création des datasets balancés pour l'entraînement")

    import spark.implicits._

    Map(
      "D1_15min" -> balanceDataset(df, "label_d1"),
      "D2_15min" -> balanceDataset(df, "label_d2"),
      "D3_15min" -> balanceDataset(df, "label_d3"),
      "basic_15min" -> balanceDataset(df, "is_delayed_15min"),
      "basic_60min" -> balanceDataset(df, "is_delayed_60min")
    )
  }

  /**
   * Balance un dataset pour avoir le même nombre de vols en retard et à l'heure
   */
  private def balanceDataset(df: DataFrame, labelColumn: String): DataFrame = {
    val delayed = df.filter(col(labelColumn) === 1)
    val onTime = df.filter(col(labelColumn) === 0)

    val delayedCount = delayed.count()
    val onTimeCount = onTime.count()

    logger.info(s"Pour $labelColumn - Retardés: $delayedCount, À l'heure: $onTimeCount")

    if (delayedCount < onTimeCount) {
      // Sous-échantillonner les vols à l'heure
      val sampledOnTime = onTime.sample(withReplacement = false, delayedCount.toDouble / onTimeCount)
      delayed.union(sampledOnTime)
    } else {
      // Sous-échantillonner les vols retardés
      val sampledDelayed = delayed.sample(withReplacement = false, onTimeCount.toDouble / delayedCount)
      sampledDelayed.union(onTime)
    }
  }

  /**
   * Preprocessing pour la jointure avec les données météo
   * Prépare les données de vols pour être jointes avec les observations météo
   */
  def prepareForWeatherJoin(df: DataFrame): DataFrame = {
    logger.info("Préparation des données de vols pour jointure météo")

    // Ajouter des colonnes pour faciliter la jointure temporelle avec les données météo
    df.withColumn("departure_date", to_date(col("FL_DATE"), "yyyy-MM-dd"))
      .withColumn("departure_hour_rounded", col("departure_hour"))
      .withColumn("join_key_origin", concat(col("ORIGIN_AIRPORT_ID"), lit("_"), col("departure_date")))
      .withColumn("join_key_dest", concat(col("DEST_AIRPORT_ID"), lit("_"), col("departure_date")))
  }
}
