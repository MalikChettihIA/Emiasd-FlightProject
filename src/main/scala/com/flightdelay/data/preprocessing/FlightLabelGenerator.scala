package com.flightdelay.data.preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
  override def preprocess(enrichedFlightData: DataFrame)(implicit spark: SparkSession): DataFrame = {
    println("")
    println("")
    println("----------------------------------------------------------------------------------------------------------")
    println("--> [FlightLabelGenerator] Flight Label Generator - Start ...")
    println("----------------------------------------------------------------------------------------------------------")

    // Vérifier que les colonnes de retard nécessaires sont présentes
    validateRequiredColumns(enrichedFlightData)

    // Étape 1: Gestion des valeurs manquantes pour les retards
    val withFilledDelays = handleDelayMissingValues(enrichedFlightData)

    // Étape 2: Création des labels de base pour différents seuils
    val withBasicLabels = addBasicDelayLabels(withFilledDelays)

    // Étape 3: Création des labels selon les stratégies TIST (D1, D2, D3, D4)
    //val withTISTLabels = addTISTStrategyLabels(withBasicLabels)

    // Étape 4: Ajout des labels de sévérité et catégories
    //val withSeverityLabels = addSeverityLabels(withTISTLabels)

    // Étape 5: Ajout des labels composites et d'interaction
    //val withCompositeLabels = addCompositeLabels(withSeverityLabels)

    // Étape 6: Validation et statistiques finales
    //val finalData = validateAndLogLabelStatistics(withCompositeLabels)

    println("")
    println("--> [FlightDataGenerator] Flight Data Generator- End ...")
    println("----------------------------------------------------------------------------------------------------------")
    println("")
    println("")

    withBasicLabels
  }

  /**
   * Valide que les colonnes nécessaires sont présentes
   */
  private def validateRequiredColumns(df: DataFrame): Unit = {

    println("")
    println("Phase 1: Validate Required Columns")

    val requiredColumns = Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY")
    val missingColumns = requiredColumns.filterNot(df.columns.contains)

    if (missingColumns.nonEmpty) {
      println(s"Colonnes manquantes pour la génération de labels: ${missingColumns.mkString(", ")}")
      throw new RuntimeException(s"Colonnes requises manquantes: ${missingColumns.mkString(", ")}")
    }

    println("- Validation of required columns: OK")
  }

  /**
   * Gestion des valeurs manquantes pour les colonnes de retard
   */
  private def handleDelayMissingValues(df: DataFrame): DataFrame = {
    println("")
    println("Phase 2: Handling missing values for delays")

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
  private def addBasicDelayLabels(df: DataFrame): DataFrame = {
    println("")
    println("Phase 2: Adding basic labels for different thresholds")
    println("- Add label_is_delayed_15min")
    println("- Add label_is_delayed_30min")
    println("- Add label_is_delayed_45min")
    println("- Add label_is_delayed_60min")
    println("- Add label_is_delayed_90min")
    println("- Add label_has_weather_delay")
    println("- Add label_has_nas_delay")
    println("- Add label_has_any_weather_nas_delay")
    println("- Add label_total_weather_nas_delay")
    println("- Add label_is_on_time")
    println("- Add label_is_early")


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

  /**
   * Ajoute les labels selon les stratégies TIST (D1, D2, D3, D4)
   * Implémente exactement les définitions de l'article
   */
  private def addTISTStrategyLabels(df: DataFrame): DataFrame = {
    println("")
    println("Phase 3: Adding labels according to TIST strategies (D1, D2, D3, D4)")
    println("- Add label_d1_15min")
    println("- Add label_d1_60min")
    println("- Add label_d2_15min")
    println("- Add label_d2_60min")
    println("- Add label_d3_15min")
    println("- Add label_d3_60min")
    println("- Add label_d4_15min")
    println("- Add label_d4_60min")
    println("- Add label_d1_30min")
    println("- Add label_d2_30min")
    println("- Add label_d3_30min")
    println("- Add label_d4_30min")

    println("Ajout des labels selon les stratégies TIST (D1, D2, D3, D4)")

    val columnExpressions = Map(
      // D1: Solo Extreme Weather U Solo NAS U Solo (Extreme Weather and NAS)
      // Retards dus uniquement à la météo extrême ou NAS (≥15min)
      "label_d1_15min" -> when(
        (col("weather_delay_filled") > 0 || col("nas_delay_filled") > 0) &&
          col("arr_delay_filled") >= DELAY_THRESHOLD_15_MIN, 1
      ).otherwise(0),

      "label_d1_60min" -> when(
        (col("weather_delay_filled") > 0 || col("nas_delay_filled") > 0) &&
          col("arr_delay_filled") >= DELAY_THRESHOLD_60_MIN, 1
      ).otherwise(0),

      // D2: Extreme Weather U NAS≥Th
      // Météo extrême + NAS si NAS >= seuil
      "label_d2_15min" -> when(
        col("weather_delay_filled") > 0 ||
          col("nas_delay_filled") >= DELAY_THRESHOLD_15_MIN, 1
      ).otherwise(0),

      "label_d2_60min" -> when(
        col("weather_delay_filled") > 0 ||
          col("nas_delay_filled") >= DELAY_THRESHOLD_60_MIN, 1
      ).otherwise(0),

      // D3: Extreme Weather U NAS
      // Météo extrême + NAS (même si < seuil)
      "label_d3_15min" -> when(
        col("weather_delay_filled") > 0 || col("nas_delay_filled") > 0, 1
      ).otherwise(0),

      "label_d3_60min" -> when(
        col("weather_delay_filled") > 0 || col("nas_delay_filled") > 0, 1
      ).otherwise(0),

      // D4: All (tous les retards ≥ seuil, quelle que soit la cause)
      "label_d4_15min" -> when(col("arr_delay_filled") >= DELAY_THRESHOLD_15_MIN, 1).otherwise(0),
      "label_d4_60min" -> when(col("arr_delay_filled") >= DELAY_THRESHOLD_60_MIN, 1).otherwise(0),

      // Labels pour tous les seuils intermédiaires (30min, 45min, 90min)
      "label_d1_30min" -> when(
        (col("weather_delay_filled") > 0 || col("nas_delay_filled") > 0) &&
          col("arr_delay_filled") >= DELAY_THRESHOLD_30_MIN, 1
      ).otherwise(0),

      "label_d2_30min" -> when(
        col("weather_delay_filled") > 0 ||
          col("nas_delay_filled") >= DELAY_THRESHOLD_30_MIN, 1
      ).otherwise(0),

      "label_d3_30min" -> when(
        col("weather_delay_filled") > 0 || col("nas_delay_filled") > 0, 1
      ).otherwise(0),

      "label_d4_30min" -> when(col("arr_delay_filled") >= DELAY_THRESHOLD_30_MIN, 1).otherwise(0)
    )

    addCalculatedColumns(df, columnExpressions)
  }

  /**
   * Ajoute les labels de sévérité et catégories de retard
   */
  private def addSeverityLabels(df: DataFrame): DataFrame = {
    println("")
    println("Phase 4: Adding severity labels and categories")
    println("- Add delay_category")
    println("- Add delay_severity_score")
    println("- Add weather_delay_category")
    println("- Add nas_delay_category")
    println("- Add weather_dominance_score")
    println("- Add dominant_delay_type")

    val columnExpressions = Map(
      // Catégories de retard selon la sévérité
      "delay_category" -> when(col("arr_delay_filled") <= 0, "on_time")
        .when(col("arr_delay_filled") <= 15, "minor_delay")
        .when(col("arr_delay_filled") <= 60, "moderate_delay")
        .when(col("arr_delay_filled") <= 180, "major_delay")
        .otherwise("extreme_delay"),

      // Score de sévérité normalisé (0-1)
      "delay_severity_score" -> when(col("arr_delay_filled") <= 0, 0.0)
        .otherwise(least(col("arr_delay_filled") / 300.0, lit(1.0))),

      // Catégories spécifiques selon type de retard
      "weather_delay_category" -> when(col("weather_delay_filled") <= 0, "no_weather_delay")
        .when(col("weather_delay_filled") <= 30, "minor_weather_delay")
        .when(col("weather_delay_filled") <= 120, "moderate_weather_delay")
        .otherwise("severe_weather_delay"),

      "nas_delay_category" -> when(col("nas_delay_filled") <= 0, "no_nas_delay")
        .when(col("nas_delay_filled") <= 30, "minor_nas_delay")
        .when(col("nas_delay_filled") <= 120, "moderate_nas_delay")
        .otherwise("severe_nas_delay"),

      // Score de dominance météo vs NAS
      "weather_dominance_score" -> when(
        col("weather_delay_filled") + col("nas_delay_filled") > 0,
        col("weather_delay_filled") / (col("weather_delay_filled") + col("nas_delay_filled"))
      ).otherwise(0.0),

      // Type de retard dominant
      "dominant_delay_type" -> when(col("weather_delay_filled") > col("nas_delay_filled"), "weather")
        .when(col("nas_delay_filled") > col("weather_delay_filled"), "nas")
        .when(col("weather_delay_filled") > 0 && col("nas_delay_filled") > 0, "both")
        .otherwise("other")
    )

    addCalculatedColumns(df, columnExpressions)
  }

  /**
   * Ajoute des labels composites et d'interaction
   */
  private def addCompositeLabels(df: DataFrame): DataFrame = {
    println("")
    println("Phase 5: Adding composite and interaction labels")
    println("- Add tist_strategy_15min")
    println("- Add weather_risk_label")
    println("- Add balanced_15min_candidate")
    println("- Add predictable_delay_15min")
    println("- Add temporal_split_group")


    val columnExpressions = Map(
      // Labels composites pour ML multi-classe
      "tist_strategy_15min" -> when(col("label_d1_15min") === 1, "D1")
        .when(col("label_d2_15min") === 1 && col("label_d1_15min") === 0, "D2")
        .when(col("label_d3_15min") === 1 && col("label_d2_15min") === 0, "D3")
        .when(col("label_d4_15min") === 1 && col("label_d3_15min") === 0, "D4")
        .otherwise("on_time"),

      // Score composite de risque météorologique
      "weather_risk_label" -> when(
        col("has_weather_delay") === 1 && col("delay_severity_score") >= 0.5, "high_weather_risk"
      ).when(
        col("has_weather_delay") === 1 && col("delay_severity_score") >= 0.2, "medium_weather_risk"
      ).when(
        col("has_weather_delay") === 1, "low_weather_risk"
      ).otherwise("no_weather_risk"),

      // Labels pour équilibrage des datasets
      "balanced_15min_candidate" -> when(
        col("label_d3_15min") === 1 ||
          (col("is_on_time") === 1 && rand() < 0.3), 1  // Sous-échantillonnage des vols à l'heure
      ).otherwise(0),

      // Label pour prédiction en temps réel (basé sur les features disponibles à l'avance)
      "predictable_delay_15min" -> when(
        col("has_weather_delay") === 1 || col("has_nas_delay") === 1,
        col("is_delayed_15min")
      ).otherwise(0),

      // Labels pour validation croisée temporelle
      "temporal_split_group" -> when(col("flight_month") <= 6, "first_half")
        .otherwise("second_half")
    )

    addCalculatedColumns(df, columnExpressions)
  }

  /**
   * Valide les labels générés et affiche des statistiques détaillées
   */
  private def validateAndLogLabelStatistics(df: DataFrame): DataFrame = {
    println("")
    println("Phase 6: Validate And Log Label Statistics")

    val totalFlights = df.count()

    // Statistiques des labels de base
    val delayed15Stats = df.filter(col("is_delayed_15min") === 1).count()
    val delayed60Stats = df.filter(col("is_delayed_60min") === 1).count()
    val weatherDelayStats = df.filter(col("has_weather_delay") === 1).count()
    val nasDelayStats = df.filter(col("has_nas_delay") === 1).count()

    println(s"- Total of Flights: $totalFlights")
    println(s"- Delay ≥15min: $delayed15Stats (${(delayed15Stats.toDouble/totalFlights*100).round}%)")
    println(s"- Delay ≥60min: $delayed60Stats (${(delayed60Stats.toDouble/totalFlights*100).round}%)")
    println(s"- Delay météo: $weatherDelayStats (${(weatherDelayStats.toDouble/totalFlights*100).round}%)")
    println(s"- Delay NAS: $nasDelayStats (${(nasDelayStats.toDouble/totalFlights*100).round}%)")

    // Statistiques des stratégies TIST
    logTISTStrategyStatistics(df, totalFlights)

    // Validation de cohérence
    validateLabelConsistency(df)

    df
  }

  /**
   * Affiche les statistiques détaillées des stratégies TIST
   */
  private def logTISTStrategyStatistics(df: DataFrame, totalFlights: Long): Unit = {
    println("")
    println("Phase 7: Log TIST Strategy Statistics")

    val strategies = Seq("d1", "d2", "d3", "d4")
    val thresholds = Seq("15min", "60min")

    for (strategy <- strategies; threshold <- thresholds) {
      val colName = s"label_${strategy}_${threshold}"
      if (df.columns.contains(colName)) {
        val count = df.filter(col(colName) === 1).count()
        val percentage = (count.toDouble / totalFlights * 100).round
        println(s"Strategy ${strategy.toUpperCase} ($threshold): $count vols ($percentage%)")
      }
    }

    // Analyse de la distribution par catégorie
    println("")
    println("Phase 8: Log Distribution by delay category")

    val categoryStats = df.groupBy("delay_category").count().collect()
    categoryStats.foreach { row =>
      val category = row.getString(0)
      val count = row.getLong(1)
      val percentage = (count.toDouble / totalFlights * 100).round
      println(s"$category: $count vols ($percentage%)")
    }
  }

  /**
   * Valide la cohérence logique des labels générés
   */
  private def validateLabelConsistency(df: DataFrame): Unit = {
    println("")
    println("Phase 9: Validate Label Consistency")

    // Vérification 1: D1 ⊆ D2 ⊆ D3
    val d1Count = df.filter(col("label_d1_15min") === 1).count()
    val d2Count = df.filter(col("label_d2_15min") === 1).count()
    val d3Count = df.filter(col("label_d3_15min") === 1).count()

    if (d1Count > d2Count || d2Count > d3Count) {
      println("Inconsistency in the hierarchy D1 ⊆ D2 ⊆ D3")
    }

    println("Consistency validation completed")
  }

  /**
   * Crée des datasets équilibrés selon les stratégies TIST
   */
  def createBalancedTISTDatasets(df: DataFrame)(implicit spark: SparkSession): Map[String, DataFrame] = {
    println("Création des datasets équilibrés selon TIST")

    val datasets = Map(
      "D1_15min" -> createBalancedDataset(df, "label_d1_15min"),
      "D1_60min" -> createBalancedDataset(df, "label_d1_60min"),
      "D2_15min" -> createBalancedDataset(df, "label_d2_15min"),
      "D2_60min" -> createBalancedDataset(df, "label_d2_60min"),
      "D3_15min" -> createBalancedDataset(df, "label_d3_15min"),
      "D3_60min" -> createBalancedDataset(df, "label_d3_60min"),
      "D4_15min" -> createBalancedDataset(df, "label_d4_15min"),
      "D4_60min" -> createBalancedDataset(df, "label_d4_60min")
    )

    datasets
  }

  /**
   * Équilibre un dataset pour avoir un ratio 50/50 entre classes
   */
  private def createBalancedDataset(df: DataFrame, labelColumn: String): DataFrame = {
    val positive = df.filter(col(labelColumn) === 1)
    val negative = df.filter(col(labelColumn) === 0)

    val positiveCount = positive.count()
    val negativeCount = negative.count()

    if (positiveCount < negativeCount) {
      // Sous-échantillonner la classe majoritaire (negative)
      val sampledNegative = negative.sample(withReplacement = false, positiveCount.toDouble / negativeCount)
      positive.union(sampledNegative)
    } else if (negativeCount < positiveCount) {
      // Sous-échantillonner la classe majoritaire (positive)
      val sampledPositive = positive.sample(withReplacement = false, negativeCount.toDouble / positiveCount)
      sampledPositive.union(negative)
    } else {
      // Les classes sont déjà équilibrées
      df.filter(col(labelColumn).isin(0, 1))
    }
  }
}