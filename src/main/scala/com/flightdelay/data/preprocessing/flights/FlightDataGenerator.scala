package com.flightdelay.data.preprocessing.flights

import com.flightdelay.data.preprocessing.DataPreprocessor
import com.flightdelay.data.utils.TimeFeatureUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

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
    println("=" * 80)
    println("[STEP 2][FlightDataGenerator] Flight Data Generator - Start ...")
    println("=" * 80)

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

    // Résumé de l'enrichissement
    logEnrichmentSummary(cleanedFlightData, withAggregatedFeatures)

    withAggregatedFeatures

  }

  /**
   * Ajoute les caractéristiques temporelles dérivées de la date et heure
   */
  def addTemporalFeatures(df: DataFrame): DataFrame = {
    println("")
    println("Phase 1: Add Temporal Features")

    val columnExpressions = Map[String, Column](
      // Conversion de la date en timestamp
      "feature_flight_timestamp" -> to_timestamp(col("FL_DATE"), "yyyy-MM-dd"),

      // Extraction des composants temporels
      "feature_flight_year" -> year(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),
      "feature_flight_month" -> month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),

      // Trimestre de l'année (Q1, Q2, Q3, Q4)
      "feature_flight_quarter" -> when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(1, 2, 3), 1)
        .when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(4, 5, 6), 2)
        .when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(7, 8, 9), 3)
        .otherwise(4),

      // Version textuelle du trimestre
      "feature_flight_quarter_name" -> when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(1, 2, 3), "Q1")
        .when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(4, 5, 6), "Q2")
        .when(month(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")).isin(7, 8, 9), "Q3")
        .otherwise("Q4"),

      "feature_flight_day_of_month" -> dayofmonth(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),
      "feature_flight_day_of_week" -> dayofweek(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),
      "feature_flight_day_of_year" -> dayofyear(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),
      "feature_flight_week_of_year" -> weekofyear(to_timestamp(col("FL_DATE"), "yyyy-MM-dd")),

      // Conversion de l'heure de départ (format HHMM vers heures et minutes)
      "feature_departure_hour" -> (col("CRS_DEP_TIME") / lit(100)).cast(IntegerType),
      "feature_utc_departure_hour" -> (col("UTC_CRS_DEP_TIME") / lit(100)).cast(IntegerType),
      "feature_departure_minute" -> (col("CRS_DEP_TIME") % lit(100)).cast(IntegerType),

      // Heure de départ en format décimal (ex: 14h30 = 14.5)
      "feature_departure_hour_decimal" -> ((col("CRS_DEP_TIME") / lit(100)).cast(DoubleType) +
        ((col("CRS_DEP_TIME") % lit(100)).cast(DoubleType) / lit(60.0))),
      "feature_utc_departure_hour_decimal" -> ((col("UTC_CRS_DEP_TIME") / lit(100)).cast(DoubleType) +
        ((col("UTC_CRS_DEP_TIME") % lit(100)).cast(DoubleType) / lit(60.0))),

      // Quart de la journée (0-3 pour les 4 quarters de 6h)
      "feature_departure_quarter_day" -> when((col("CRS_DEP_TIME") / lit(100)) < lit(6), 0)
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(12), 1)
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(18), 2)
        .otherwise(3),

      // Nom du quarter pour lisibilité
      "feature_departure_quarter_name" -> when((col("CRS_DEP_TIME") / lit(100)) < lit(6), "Night")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(12), "Morning")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(18), "Afternoon")
        .otherwise("Evening"),

      // Période de temps plus granulaire (8 périodes de 3h)
      "feature_departure_time_period" -> when((col("CRS_DEP_TIME") / lit(100)) < lit(3), "Late_Night")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(6), "Early_Morning")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(9), "Morning")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(12), "Late_Morning")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(15), "Early_Afternoon")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(18), "Late_Afternoon")
        .when((col("CRS_DEP_TIME") / lit(100)) < lit(21), "Evening")
        .otherwise("Night"),

      // Minutes depuis minuit (départ)
      "feature_minutes_since_midnight" -> ((col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100))),
      // Arrondi de CRS_DEP_TIME
      "feature_departure_hour_rounded" -> TimeFeatureUtils.roundTimeToNearestHour(col("CRS_DEP_TIME")),
      "feature_utc_departure_hour_rounded" -> TimeFeatureUtils.roundTimeToNearestHour(col("UTC_CRS_DEP_TIME")),

      // ===== ARRIVAL TIME FEATURES =====
      // Calcul de l'heure d'arrivée prévue (CRS_DEP_TIME + CRS_ELAPSED_TIME)
      // 1. Convertir CRS_DEP_TIME en minutes depuis minuit
      "feature_departure_minutes_total" -> ((col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100))),

      // 2. Ajouter la durée du vol et gérer le passage à minuit (modulo 1440 minutes = 24h)
      "feature_arrival_minutes_total" -> (
        ((col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100)) + col("CRS_ELAPSED_TIME")) % lit(1440)
      ),

      // 3. Convertir en format HHMM
      "feature_arrival_time" -> (
        ((((col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100)) + col("CRS_ELAPSED_TIME")) % lit(1440)) / lit(60)).cast(IntegerType) * lit(100) +
        ((((col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100)) + col("CRS_ELAPSED_TIME")) % lit(1440)) % lit(60)).cast(IntegerType)
      ),

      // Extraction des composants de l'heure d'arrivée
      "feature_arrival_hour" -> (
        (((col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100)) + col("CRS_ELAPSED_TIME")) % lit(1440)) / lit(60)
      ).cast(IntegerType),

      "feature_arrival_minute" -> (
        (((col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100)) + col("CRS_ELAPSED_TIME")) % lit(1440)) % lit(60)
      ).cast(IntegerType),

      // Heure d'arrivée en format décimal (ex: 14h30 = 14.5)
      "feature_arrival_hour_decimal" -> (
        (((col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100)) + col("CRS_ELAPSED_TIME")) % lit(1440)) / lit(60.0)
      ),

      // ===== MIDNIGHT CROSSING DETECTION =====
      // Détection si le vol traverse minuit (arrive le jour suivant)
      "feature_crosses_midnight" -> when(
        ((col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100)) + col("CRS_ELAPSED_TIME")) >= lit(1440),
        1
      ).otherwise(0),

      // Nombre de jours de voyage (0 = même jour, 1 = lendemain, etc.)
      "feature_flight_days_span" -> floor(
        ((col("CRS_DEP_TIME") / lit(100)) * lit(60) + (col("CRS_DEP_TIME") % lit(100)) + col("CRS_ELAPSED_TIME")) / lit(1440)
      ).cast(IntegerType)

    )

    // Ajouter les colonnes calculées
    val resultWithBasicFeatures = addCalculatedColumns(df, columnExpressions)

    // Ajouter les features qui dépendent des colonnes créées ci-dessus
    val columnExpressions2 = Map(
      // Arrondi de l'heure d'arrivée (format String avec padding 4 chiffres)
      "feature_arrival_hour_rounded" -> format_string("%04d",
        TimeFeatureUtils.roundTimeToNearestHour(col("feature_arrival_time"))
      ),

      // Date d'arrivée (en tenant compte du passage à minuit)
      // Ajoute feature_flight_days_span jours à FL_DATE
      "feature_arrival_date" -> date_format(
        date_add(col("UTC_FL_DATE"), col("feature_flight_days_span")),
        "yyyy-MM-dd"
      )
    )

    val result = addCalculatedColumns(resultWithBasicFeatures, columnExpressions2)

    println(s"Temporal features added: ${columnExpressions.size + columnExpressions2.size}")
    result
  }

  /**
   * Ajoute les caractéristiques spécifiques au vol
   */
  def addFlightCharacteristics(df: DataFrame): DataFrame = {
    println("")
    println("Phase 2: Add Flight Characteristics")

    /**println("- Add feature_flight_unique_id ")
    println("- Add feature_distance_category (short, medium, long, very_long) ")
    println("- Add feature_distance_score ")
    println("- Add feature_is_likely_domestic ")
    println("- Add feature_carrier_hash ")
    println("- Add feature_route_id ")
    println("- Add feature_is_roundtrip_candidate ")**/

    val columnExpressions = Map(
      // Identifiant unique du vol
      "feature_flight_unique_id" -> concat(
        col("UTC_FL_DATE"), lit("_"),
        col("OP_CARRIER_AIRLINE_ID"), lit("_"),
        col("OP_CARRIER_FL_NUM"), lit("_"),
        col("ORIGIN_AIRPORT_ID"), lit("_"),
        col("DEST_AIRPORT_ID")
      ),

      // Catégorie de distance basée sur le temps de vol estimé
      "feature_distance_category" -> when(col("CRS_ELAPSED_TIME") <= 90, "short")
        .when(col("CRS_ELAPSED_TIME") <= 240, "medium")
        .when(col("CRS_ELAPSED_TIME") <= 360, "long")
        .otherwise("very_long"),

      // Score de distance normalisé (0-1)
      "feature_distance_score" -> least(col("CRS_ELAPSED_TIME") / 600.0, lit(1.0)),

      // Indicateur de vol domestique vs international (proxy basé sur la durée)
      "feature_is_likely_domestic" -> when(col("CRS_ELAPSED_TIME") <= 360, 1).otherwise(0),

      // Catégorie de compagnie (transformée en hash pour anonymisation)
      "feature_carrier_hash" -> hash(col("OP_CARRIER_AIRLINE_ID")),

      // Route (combinaison origine-destination)
      "feature_route_id" -> concat(
        least(col("ORIGIN_AIRPORT_ID"), col("DEST_AIRPORT_ID")),
        lit("_"),
        greatest(col("ORIGIN_AIRPORT_ID"), col("DEST_AIRPORT_ID"))
      ),

      // Indicateur de vol aller-retour dans la même journée (proxy)
      "feature_is_roundtrip_candidate" -> when(col("CRS_ELAPSED_TIME") <= 180, 1).otherwise(0)
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

   /**println("- Add feature_is_weekend, feature_is_friday, feature_is_monday")
    println("- Add feature_is_summer, feature_is_winter, feature_is_spring, feature_is_fall ")
    println("- Add feature_is_holiday_season (approximative)")
    println("- Add feature_is_early_morning ")
    println("- Add feature_is_morning_rush ")
    println("- Add feature_is_business_hours ")
    println("- Add feature_is_evening_rush ")
    println("- Add feature_is_night_flight ")
    println("- Add feature_is_month_start ")
    println("- Add feature_is_month_end ")
    println("- Add feature_is_extended_weekend ")**/

    val columnExpressions = Map(
      // Indicateurs de fin de semaine
      "feature_is_weekend" -> when(col("feature_flight_day_of_week").isin(1, 7), 1).otherwise(0),
      "feature_is_friday" -> when(col("feature_flight_day_of_week") === 6, 1).otherwise(0),
      "feature_is_monday" -> when(col("feature_flight_day_of_week") === 2, 1).otherwise(0),

      // Indicateurs saisonniers
      "feature_is_summer" -> when(col("feature_flight_month").isin(6, 7, 8), 1).otherwise(0),
      "feature_is_winter" -> when(col("feature_flight_month").isin(12, 1, 2), 1).otherwise(0),
      "feature_is_spring" -> when(col("feature_flight_month").isin(3, 4, 5), 1).otherwise(0),
      "feature_is_fall" -> when(col("feature_flight_month").isin(9, 10, 11), 1).otherwise(0),

      // Période de vacances (approximative)
      "feature_is_holiday_season" -> when(
        col("feature_flight_month").isin(11, 12, 1) || // Thanksgiving, Noël, Nouvel An
          (col("feature_flight_month") === 7) || // Juillet (vacances d'été)
          (col("feature_flight_month") === 3 && col("feature_flight_day_of_month").between(15, 31)), // Spring break
        1
      ).otherwise(0),

      // Indicateurs de périodes de pointe (rush hours)
      "feature_is_early_morning" -> when(col("feature_departure_hour").between(5, 7), 1).otherwise(0),
      "feature_is_morning_rush" -> when(col("feature_departure_hour").between(6, 9), 1).otherwise(0),
      "feature_is_business_hours" -> when(col("feature_departure_hour").between(9, 17), 1).otherwise(0),
      "feature_is_evening_rush" -> when(col("feature_departure_hour").between(17, 20), 1).otherwise(0),
      "feature_is_night_flight" -> when(
        col("feature_departure_hour") >= 22 || col("feature_departure_hour") <= 5, 1
      ).otherwise(0),

      // Indicateurs de début/fin de mois (voyages d'affaires)
      "feature_is_month_start" -> when(col("feature_flight_day_of_month") <= 5, 1).otherwise(0),
      "feature_is_month_end" -> when(col("feature_flight_day_of_month") >= 26, 1).otherwise(0),

      // Indicateur de semaine de travail vs weekend prolongé
      "feature_is_extended_weekend" -> when(
        (col("feature_flight_day_of_week") === 6 && col("feature_departure_hour") <= 12) || // Vendredi matin
          (col("feature_flight_day_of_week") === 1 && col("feature_departure_hour") >= 18), 1  // Dimanche soir
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

    /**println("- Add feature_origin_is_major_hub (10397, 11298, 12266, 13930, 14107, 14771, 15016  // Principaux hubs US)")
    println("- Add feature_dest_is_major_hub  (10397, 11298, 12266, 13930, 14107, 14771, 15016  // Principaux hubs US)")
    println("- Add feature_is_hub_to_hub")
    println("- Add feature_flight_quarter") // NOUVEAU
    println("- Add feature_origin_complexity_score")
    println("- Add feature_dest_complexity_score")
    println("- Add feature_timezone_diff_proxy")
    println("- Add feature_flight_week_of_year")
    println("- Add feature_is_eastbound")
    println("- Add feature_is_westbound")**/


    val columnExpressions1 = Map(
      // Indicateurs de hub (aéroports avec beaucoup de trafic)
      "feature_origin_is_major_hub" -> when(
        col("ORIGIN_AIRPORT_ID").isin(
          10397, 11298, 12266, 13930, 14107, 14771, 15016  // Principaux hubs US
        ), 1
      ).otherwise(0),

      "feature_dest_is_major_hub" -> when(
        col("DEST_AIRPORT_ID").isin(
          10397, 11298, 12266, 13930, 14107, 14771, 15016
        ), 1
      ).otherwise(0),

      // Score de complexité de l'aéroport (proxy basé sur l'ID)
      "feature_origin_complexity_score" -> (col("ORIGIN_AIRPORT_ID") % 100) / 100.0,
      "feature_dest_complexity_score" -> (col("DEST_AIRPORT_ID") % 100) / 100.0,

      // Différence de fuseaux horaires (approximative basée sur l'ID d'aéroport)
      "feature_timezone_diff_proxy" -> abs(
        (col("ORIGIN_AIRPORT_ID") % 10) - (col("DEST_AIRPORT_ID") % 10)
      ),

      // Direction générale du vol (Est-Ouest)
      "feature_is_eastbound" -> when(col("DEST_AIRPORT_ID") > col("ORIGIN_AIRPORT_ID"), 1).otherwise(0),
      "feature_is_westbound" -> when(col("ORIGIN_AIRPORT_ID") > col("DEST_AIRPORT_ID"), 1).otherwise(0)
    )

    val columnExpressions2 = Map(
      // Indicateur de vol hub-to-hub
      "feature_is_hub_to_hub" -> when(
        col("feature_origin_is_major_hub") === 1 && col("feature_dest_is_major_hub") === 1, 1
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

    /**println("- Add feature_flights_on_route")
    println("- Add feature_carrier_flight_count")
    println("- Add feature_origin_airport_traffic")
    println("- Add feature_route_popularity_score")
    println("- Add feature_carrier_size_category")**/

    import org.apache.spark.sql.expressions.Window

    // Window pour calculer des statistiques par route
    val routeWindow = Window.partitionBy("feature_route_id")
    val carrierWindow = Window.partitionBy("OP_CARRIER_AIRLINE_ID")
    val airportOriginWindow = Window.partitionBy("ORIGIN_AIRPORT_ID")

    val result = df
      .withColumn("feature_flights_on_route", count("*").over(routeWindow))
      .withColumn("feature_carrier_flight_count", count("*").over(carrierWindow))
      .withColumn("feature_origin_airport_traffic", count("*").over(airportOriginWindow))
      .withColumn(
        "feature_route_popularity_score",
        when(col("feature_flights_on_route") >= 100, "high")
          .when(col("feature_flights_on_route") >= 20, "medium")
          .otherwise("low")
      )
      .withColumn(
        "feature_carrier_size_category",
        when(col("feature_carrier_flight_count") >= 1000, "major")
          .when(col("feature_carrier_flight_count") >= 100, "medium")
          .otherwise("small")
      )

    println("Added Flight features: 5")
    result
  }

  /**
   * Résumé détaillé du processus d'enrichissement
   */
  private def logEnrichmentSummary(originalDf: DataFrame, enrichedDf: DataFrame): Unit = {
    val originalColumns = originalDf.columns.length
    val enrichedColumns = enrichedDf.columns.length
    val addedColumns = enrichedColumns - originalColumns

    println("")
    println("=== Enrichment Summary ===")
    println("")
    println(s"Original Columns: $originalColumns")
    println(s"Columns after enrichment: $enrichedColumns")
    println(s"Enriched Columns: $addedColumns")
    println(s"Dataset size: ${enrichedDf.count()}")
    println("")

  }
}