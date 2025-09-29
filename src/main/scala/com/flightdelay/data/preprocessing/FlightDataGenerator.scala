package com.flightdelay.data.preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Classe spécialisée pour l'ajout de nouvelles colonnes et features engineering
 * Responsable de l'enrichissement des données avec des caractéristiques dérivées
 * Implémente la phase "Data transformation" de l'article TIST
 */
object FlightDataGenerator extends DataPreprocessor {

  /**
   * Enrichissement principal des données de vols avec des features calculées
   * @param cleanedFlightData DataFrame contenant les données de vols nettoyées
   * @param spark Session Spark
   * @return DataFrame enrichi avec des nouvelles colonnes
   */
  override def preprocess(cleanedFlightData: DataFrame)(implicit spark: SparkSession): DataFrame = {
    println("")
    println("")
    println("----------------------------------------------------------------------------------------------------------")
    println("--> [FlightDataGenerator] Flight Data Generator - Start ...")
    println("----------------------------------------------------------------------------------------------------------")

    val originalColumns = cleanedFlightData.columns.length
    println(s"Original Column Counts: $originalColumns")

    // Étape 1: Ajout des caractéristiques temporelles
    val withTemporalFeatures = addTemporalFeatures(cleanedFlightData)

    // Étape 2: Ajout des caractéristiques de vol
    val withFlightFeatures = addFlightCharacteristics(withTemporalFeatures)

    // Étape 3: Ajout des indicateurs de période
    val withPeriodIndicators = addPeriodIndicators(withFlightFeatures)

    // Étape 4: Ajout des caractéristiques géographiques
    val withGeographicFeatures = addGeographicFeatures(withPeriodIndicators)

    // Étape 5: Ajout des features agrégées
    val withAggregatedFeatures = addAggregatedFeatures(withGeographicFeatures)

    // Étape 6: Normalisation de certaines colonnes
    val normalizedData = normalizeSelectedFeatures(withAggregatedFeatures)

    // Résumé de l'enrichissement
    logEnrichmentSummary(cleanedFlightData, normalizedData)

    println("")
    println("--> [FlightDataGenerator] Flight Data Generator- End ...")
    println("----------------------------------------------------------------------------------------------------------")
    println("")
    println("")

    normalizedData

  }

  /**
   * Ajoute les caractéristiques temporelles dérivées de la date et heure
   */
  def addTemporalFeatures(df: DataFrame): DataFrame = {
    println("")
    println("Phase 1: Add Temporal Features")
    println("- Add flight_timestamp")
    println("- Add flight_year")
    println("- Add flight_month")
    println("- Add flight_quarter") // NOUVEAU
    println("- Add flight_day_of_month")
    println("- Add flight_day_of_week")
    println("- Add flight_day_of_year")
    println("- Add flight_week_of_year")
    println("- Add departure_hour")
    println("- Add departure_minute")
    println("- Add departure_hour_decimal")
    println("- Add departure_quarter_day")
    println("- Add departure_quarter_name")
    println("- Add departure_time_period")
    println("- Add minutes_since_midnight")

    val columnExpressions = Map[String, Column](
      // Conversion de la date en timestamp
      "flight_timestamp" -> to_timestamp(col("FL_DATE"), "yyyy-MM-dd"),

      // Extraction des composants temporels
      "flight_year" -> year(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),
      "flight_month" -> month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),

      // Trimestre de l'année (Q1, Q2, Q3, Q4)
      "flight_quarter" -> when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(1, 2, 3), 1)
        .when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(4, 5, 6), 2)
        .when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(7, 8, 9), 3)
        .otherwise(4),

      // Version textuelle du trimestre
      "flight_quarter_name" -> when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(1, 2, 3), "Q1")
        .when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(4, 5, 6), "Q2")
        .when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(7, 8, 9), "Q3")
        .otherwise("Q4"),

      "flight_day_of_month" -> dayofmonth(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),
      "flight_day_of_week" -> dayofweek(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),
      "flight_day_of_year" -> dayofyear(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),
      "flight_week_of_year" -> weekofyear(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),

      // Conversion de l'heure de départ (format HHMM vers heures et minutes)
      "departure_hour" -> (col("CRS_DEP_TIME") / lit(100)).cast(IntegerType),
      "departure_minute" -> (col("CRS_DEP_TIME") % lit(100)).cast(IntegerType),

      // Heure de départ en format décimal (ex: 14h30 = 14.5)
      "departure_hour_decimal" -> ((col("CRS_DEP_TIME") / lit(100)).cast(DoubleType) +
        ((col("CRS_DEP_TIME") % lit(100)).cast(DoubleType) / lit(60.0))),

      // Quart de la journée (0-3 pour les 4 quarters de 6h)
      "departure_quarter_day" -> when((col("CRS_DEP_TIME") / lit(100)) < lit(6), 0)
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(12), 1)
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(18), 2)
        .otherwise(3),

      // Nom du quarter pour lisibilité
      "departure_quarter_name" -> when((col("CRS_DEP_TIME") / lit(100)) < lit(6), "Night")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(12), "Morning")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(18), "Afternoon")
        .otherwise("Evening"),

      // Période de temps plus granulaire (8 périodes de 3h)
      "departure_time_period" -> when((col("CRS_DEP_TIME") / lit(100)) < lit(3), "Late_Night")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(6), "Early_Morning")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(9), "Morning")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(12), "Late_Morning")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(15), "Early_Afternoon")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(18), "Late_Afternoon")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(21), "Evening")
        .otherwise("Night"),

      // Minutes depuis minuit
      "minutes_since_midnight" -> ((col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100)))
    )

    val result = addCalculatedColumns(df, columnExpressions)
    println(s"Temporal features added: ${columnExpressions.size}")
    result.printSchema
    result
  }

  /**
   * Ajoute les caractéristiques spécifiques au vol
   */
  def addFlightCharacteristics(df: DataFrame): DataFrame = {
    println("")
    println("Phase 2: Add Flight Characteristics")

    println("- Add flight_unique_id ")
    println("- Add distance_category (short, medium, long, very_long) ")
    println("- Add distance_score ")
    println("- Add is_likely_domestic ")
    println("- Add carrier_hash ")
    println("- Add route_id ")
    println("- Add is_roundtrip_candidate ")

    val columnExpressions = Map(
      // Identifiant unique du vol
      "flight_unique_id" -> concat(
        col("FL_DATE"), lit("_"),
        col("OP_CARRIER_AIRLINE_ID"), lit("_"),
        col("OP_CARRIER_FL_NUM"), lit("_"),
        col("ORIGIN_AIRPORT_ID"), lit("_"),
        col("DEST_AIRPORT_ID")
      ),

      // Catégorie de distance basée sur le temps de vol estimé
      "distance_category" -> when(col("CRS_ELAPSED_TIME") <= 90, "short")
        .when(col("CRS_ELAPSED_TIME") <= 240, "medium")
        .when(col("CRS_ELAPSED_TIME") <= 360, "long")
        .otherwise("very_long"),

      // Score de distance normalisé (0-1)
      "distance_score" -> least(col("CRS_ELAPSED_TIME") / 600.0, lit(1.0)),

      // Indicateur de vol domestique vs international (proxy basé sur la durée)
      "is_likely_domestic" -> when(col("CRS_ELAPSED_TIME") <= 360, 1).otherwise(0),

      // Catégorie de compagnie (transformée en hash pour anonymisation)
      "carrier_hash" -> hash(col("OP_CARRIER_AIRLINE_ID")),

      // Route (combinaison origine-destination)
      "route_id" -> concat(
        least(col("ORIGIN_AIRPORT_ID"), col("DEST_AIRPORT_ID")),
        lit("_"),
        greatest(col("ORIGIN_AIRPORT_ID"), col("DEST_AIRPORT_ID"))
      ),

      // Indicateur de vol aller-retour dans la même journée (proxy)
      "is_roundtrip_candidate" -> when(col("CRS_ELAPSED_TIME") <= 180, 1).otherwise(0)
    )

    val result = addCalculatedColumns(df, columnExpressions)
    println(s"Added Flight features: ${columnExpressions.size}")
    result
  }

  /**
   * Ajoute les indicateurs de période (rush hours, week-end, saisons, etc.)
   */
  def addPeriodIndicators(df: DataFrame): DataFrame = {

    println("")
    println("Phase 3: Add Period <indicator")

    println("- Add is_weekend, is_friday, is_monday")
    println("- Add is_summer, is_winter, is_spring, is_fall ")
    println("- Add is_holiday_season (approximative)")
    println("- Add is_early_morning ")
    println("- Add is_morning_rush ")
    println("- Add is_business_hours ")
    println("- Add is_evening_rush ")
    println("- Add is_night_flight ")
    println("- Add is_month_start ")
    println("- Add is_month_end ")
    println("- Add is_extended_weekend ")

    val columnExpressions = Map(
      // Indicateurs de fin de semaine
      "is_weekend" -> when(col("flight_day_of_week").isin(1, 7), 1).otherwise(0),
      "is_friday" -> when(col("flight_day_of_week") === 6, 1).otherwise(0),
      "is_monday" -> when(col("flight_day_of_week") === 2, 1).otherwise(0),

      // Indicateurs saisonniers
      "is_summer" -> when(col("flight_month").isin(6, 7, 8), 1).otherwise(0),
      "is_winter" -> when(col("flight_month").isin(12, 1, 2), 1).otherwise(0),
      "is_spring" -> when(col("flight_month").isin(3, 4, 5), 1).otherwise(0),
      "is_fall" -> when(col("flight_month").isin(9, 10, 11), 1).otherwise(0),

      // Période de vacances (approximative)
      "is_holiday_season" -> when(
        col("flight_month").isin(11, 12, 1) || // Thanksgiving, Noël, Nouvel An
          (col("flight_month") === 7) || // Juillet (vacances d'été)
          (col("flight_month") === 3 && col("flight_day_of_month").between(15, 31)), // Spring break
        1
      ).otherwise(0),

      // Indicateurs de périodes de pointe (rush hours)
      "is_early_morning" -> when(col("departure_hour").between(5, 7), 1).otherwise(0),
      "is_morning_rush" -> when(col("departure_hour").between(6, 9), 1).otherwise(0),
      "is_business_hours" -> when(col("departure_hour").between(9, 17), 1).otherwise(0),
      "is_evening_rush" -> when(col("departure_hour").between(17, 20), 1).otherwise(0),
      "is_night_flight" -> when(
        col("departure_hour") >= 22 || col("departure_hour") <= 5, 1
      ).otherwise(0),

      // Indicateurs de début/fin de mois (voyages d'affaires)
      "is_month_start" -> when(col("flight_day_of_month") <= 5, 1).otherwise(0),
      "is_month_end" -> when(col("flight_day_of_month") >= 26, 1).otherwise(0),

      // Indicateur de semaine de travail vs weekend prolongé
      "is_extended_weekend" -> when(
        (col("flight_day_of_week") === 6 && col("departure_hour") <= 12) || // Vendredi matin
          (col("flight_day_of_week") === 1 && col("departure_hour") >= 18), 1  // Dimanche soir
      ).otherwise(0)
    )

    val result = addCalculatedColumns(df, columnExpressions)
    println(s"Added Flight features: ${columnExpressions.size}")
    result
  }

  /**
   * Ajoute les caractéristiques géographiques et de réseau
   */
  def addGeographicFeatures(df: DataFrame): DataFrame = {
    println("")
    println("Phase 4: Add Geographical Features")

    println("- Add origin_is_major_hub (10397, 11298, 12266, 13930, 14107, 14771, 15016  // Principaux hubs US)")
    println("- Add dest_is_major_hub  (10397, 11298, 12266, 13930, 14107, 14771, 15016  // Principaux hubs US)")
    println("- Add is_hub_to_hub")
    println("- Add flight_quarter") // NOUVEAU
    println("- Add origin_complexity_score")
    println("- Add dest_complexity_score")
    println("- Add timezone_diff_proxy")
    println("- Add flight_week_of_year")
    println("- Add is_eastbound")
    println("- Add is_westbound")


    val columnExpressions1 = Map(
      // Indicateurs de hub (aéroports avec beaucoup de trafic)
      "origin_is_major_hub" -> when(
        col("ORIGIN_AIRPORT_ID").isin(
          10397, 11298, 12266, 13930, 14107, 14771, 15016  // Principaux hubs US
        ), 1
      ).otherwise(0),

      "dest_is_major_hub" -> when(
        col("DEST_AIRPORT_ID").isin(
          10397, 11298, 12266, 13930, 14107, 14771, 15016
        ), 1
      ).otherwise(0),

      // Score de complexité de l'aéroport (proxy basé sur l'ID)
      "origin_complexity_score" -> (col("ORIGIN_AIRPORT_ID") % 100) / 100.0,
      "dest_complexity_score" -> (col("DEST_AIRPORT_ID") % 100) / 100.0,

      // Différence de fuseaux horaires (approximative basée sur l'ID d'aéroport)
      "timezone_diff_proxy" -> abs(
        (col("ORIGIN_AIRPORT_ID") % 10) - (col("DEST_AIRPORT_ID") % 10)
      ),

      // Direction générale du vol (Est-Ouest)
      "is_eastbound" -> when(col("DEST_AIRPORT_ID") > col("ORIGIN_AIRPORT_ID"), 1).otherwise(0),
      "is_westbound" -> when(col("ORIGIN_AIRPORT_ID") > col("DEST_AIRPORT_ID"), 1).otherwise(0)
    )

    val columnExpressions2 = Map(
      // Indicateur de vol hub-to-hub
      "is_hub_to_hub" -> when(
        col("origin_is_major_hub") === 1 && col("dest_is_major_hub") === 1, 1
      ).otherwise(0)
    )

    val result1 = addCalculatedColumns(df, columnExpressions1)
    val result2 = addCalculatedColumns(result1, columnExpressions2)
    println(s"Added Flight features: ${columnExpressions1.size + columnExpressions2.size}")
    result2
  }

  /**
   * Ajoute des features agrégées calculées par session Spark
   */
  def addAggregatedFeatures(df: DataFrame): DataFrame = {
    println("")
    println("Phase 5 : Add Aggregated Features")

    println("- Add flights_on_route")
    println("- Add carrier_flight_count")
    println("- Add origin_airport_traffic")
    println("- Add route_popularity_score")
    println("- Add carrier_size_category")



    import org.apache.spark.sql.expressions.Window

    // Window pour calculer des statistiques par route
    val routeWindow = Window.partitionBy("route_id")
    val carrierWindow = Window.partitionBy("OP_CARRIER_AIRLINE_ID")
    val airportOriginWindow = Window.partitionBy("ORIGIN_AIRPORT_ID")

    val result = df
      .withColumn("flights_on_route", count("*").over(routeWindow))
      .withColumn("carrier_flight_count", count("*").over(carrierWindow))
      .withColumn("origin_airport_traffic", count("*").over(airportOriginWindow))
      .withColumn(
        "route_popularity_score",
        when(col("flights_on_route") >= 100, "high")
          .when(col("flights_on_route") >= 20, "medium")
          .otherwise("low")
      )
      .withColumn(
        "carrier_size_category",
        when(col("carrier_flight_count") >= 1000, "major")
          .when(col("carrier_flight_count") >= 100, "medium")
          .otherwise("small")
      )

    println("Added Flight features: 5")
    result
  }

  /**
   * Normalise certaines features numériques pour améliorer les performances ML
   */
  def normalizeSelectedFeatures(df: DataFrame): DataFrame = {
    println("")
    println("Phase 6 : Features Normalization")

    println("- Normalize CRS_ELAPSED_TIME")
    println("- Normalize departure_hour_decimal")
    println("- Normalize distance_score")
    println("- Normalize origin_complexity_score")
    println("- Normalize dest_complexity_score")

    val columnsToNormalize = Seq(
      "CRS_ELAPSED_TIME",
      "departure_hour_decimal",
      "distance_score",
      "origin_complexity_score",
      "dest_complexity_score"
    )

    // Appliquer la normalisation Z-score uniquement aux colonnes numériques
    val availableColumns = columnsToNormalize.filter(df.columns.contains)
    val result = normalizeColumns(df, availableColumns)

    println(s"Normalized Columns: ${availableColumns.size}")
    result
  }

  /**
   * Ajoute des features spécialisées pour la prédiction de retards météo
   */
  def addWeatherRelatedFeatures(df: DataFrame): DataFrame = {
    println("Ajout des features liées à la météo")

    val columnExpressions = Map(
      // Indicateurs de saisons à risque météorologique
      "is_storm_season" -> when(col("flight_month").isin(5, 6, 7, 8, 9), 1).otherwise(0),
      "is_snow_season" -> when(col("flight_month").isin(11, 12, 1, 2, 3), 1).otherwise(0),

      // Périodes de la journée sensibles aux conditions météo
      "is_weather_sensitive_hour" -> when(
        col("departure_hour").between(6, 10) || // Brouillard matinal
          col("departure_hour").between(15, 19), 1  // Orages après-midi
      ).otherwise(0),

      // Score de risque météorologique combiné
      "weather_risk_score" -> (
        col("is_storm_season") * 0.3 +
          col("is_snow_season") * 0.3 +
          col("is_weather_sensitive_hour") * 0.2 +
          col("is_winter") * 0.2
        )
    )

    addCalculatedColumns(df, columnExpressions)
  }

  /**
   * Crée des features d'interaction entre différentes variables
   */
  def addInteractionFeatures(df: DataFrame): DataFrame = {
    println("Ajout des features d'interaction")

    val columnExpressions = Map(
      // Interaction saison x période de la journée
      "summer_evening_flight" -> col("is_summer") * col("is_evening_rush"),
      "winter_morning_flight" -> col("is_winter") * col("is_early_morning"),

      // Interaction week-end x distance
      "weekend_long_flight" -> col("is_weekend") *
        when(col("distance_category") === "long", 1).otherwise(0),

      // Interaction hub x rush hour
      "hub_rush_complexity" -> (col("origin_is_major_hub") + col("dest_is_major_hub")) *
        (col("is_morning_rush") + col("is_evening_rush")),

      // Score composite de complexité du vol
      "flight_complexity_score" -> (
        col("distance_score") * 0.3 +
          (col("origin_is_major_hub") + col("dest_is_major_hub")) * 0.2 +
          col("is_hub_to_hub") * 0.2 +
          (col("is_morning_rush") + col("is_evening_rush")) * 0.3
        )
    )

    addCalculatedColumns(df, columnExpressions)
  }

  /**
   * Résumé détaillé du processus d'enrichissement
   */
  private def logEnrichmentSummary(originalDf: DataFrame, enrichedDf: DataFrame): Unit = {
    val originalColumns = originalDf.columns.length
    val enrichedColumns = enrichedDf.columns.length
    val addedColumns = enrichedColumns - originalColumns

    println("")
    println("")
    println("=== Enrichment Summary ===")
    println(s"Original Columns: $originalColumns")
    println(s"Columns after enrichment: $enrichedColumns")
    println(s"Enriched Columns: $addedColumns")
    println(s"Dataset size: ${enrichedDf.count()}")
    println("")

    // Lister les nouvelles colonnes par catégorie
    val newColumns = enrichedDf.columns.filterNot(originalDf.columns.contains)
    println(s"New Features Created : \n${newColumns.mkString(",\n")}")
  }
}