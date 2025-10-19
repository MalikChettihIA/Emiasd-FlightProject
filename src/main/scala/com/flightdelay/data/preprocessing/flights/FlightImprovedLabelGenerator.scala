package com.flightdelay.data.preprocessing.flights

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Improved Flight Label Generator
 *
 * Basé sur le papier TIST-Flight-Delay-final.pdf
 *
 * Amélioration clés:
 * 1. Labels composites (WEATHER + NAS) pour capturer les effets indirects
 * 2. Multiple thresholds pour analyse comparative
 * 3. Features explicatives (contribution météo, type dominant)
 * 4. Gestion des valeurs manquantes avec stratégie intelligente
 */
object FlightImprovedLabelGenerator {

  // Seuils de retard standards (en minutes)
  private val DELAY_THRESHOLDS = Seq(15, 30, 45, 60, 90)

  /**
   * Génère les labels améliorés selon l'approche du papier TIST
   *
   * @param flightData DataFrame avec colonnes de retard (ARR_DELAY_NEW, WEATHER_DELAY, NAS_DELAY, etc.)
   * @return DataFrame enrichi avec labels multiples et features explicatives
   */
  def generateImprovedLabels(flightData: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    println("")
    println("=" * 80)
    println("[ImprovedLabelGenerator] Generating Enhanced Labels - Start")
    println("=" * 80)

    // Étape 1 : Validation des colonnes requises
    println("\n[Step 1] Validating required columns...")
    validateRequiredColumns(flightData)
    println("  ✓ All required columns present")

    // Étape 2 : Remplissage des valeurs manquantes (stratégie intelligente)
    println("\n[Step 2] Handling missing values with intelligent strategy...")
    val withFilledDelays = fillMissingDelays(flightData)

    // Étape 3 : Labels principaux (approche papier TIST)
    println("\n[Step 3] Creating primary labels (TIST approach)...")
    val withPrimaryLabels = createPrimaryLabels(withFilledDelays)

    // Étape 4 : Labels par seuil (pour analyse comparative)
    println("\n[Step 4] Creating threshold-based labels...")
    val withThresholdLabels = createThresholdLabels(withPrimaryLabels)

    // Étape 5 : Features explicatives
    println("\n[Step 5] Creating explanatory features...")
    val withExplanatoryFeatures = createExplanatoryFeatures(withThresholdLabels)

    // Étape 6 : Statistiques finales
    println("\n[Step 6] Computing label statistics...")
    printLabelStatistics(withExplanatoryFeatures)

    println("\n[ImprovedLabelGenerator] Completed successfully")
    println("=" * 80)

    withExplanatoryFeatures
  }

  /**
   * Valide la présence des colonnes requises
   */
  private def validateRequiredColumns(df: DataFrame): Unit = {
    val requiredColumns = Seq(
      "ARR_DELAY_NEW",
      "WEATHER_DELAY",
      "NAS_DELAY"
    )

    val missingColumns = requiredColumns.filterNot(df.columns.contains)

    if (missingColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"Missing required columns: ${missingColumns.mkString(", ")}"
      )
    }
  }

  /**
   * Stratégie intelligente de remplissage des valeurs manquantes
   *
   * Règles :
   * 1. Si ARR_DELAY_NEW est NULL → vol probablement annulé → on exclut
   * 2. Si WEATHER_DELAY/NAS_DELAY NULL mais ARR_DELAY > 0 → on met 0 (pas de retard météo)
   * 3. On garde trace des valeurs manquantes pour analyse
   */
  private def fillMissingDelays(df: DataFrame): DataFrame = {

    println("  Strategy:")
    println("    - If ARR_DELAY_NEW is NULL → filter out (likely cancelled)")
    println("    - If specific delay NULL but ARR_DELAY > 0 → assume 0 (no such delay)")
    println("    - Track missing values for transparency")

    df
      // Filtrer les vols sans ARR_DELAY (probablement annulés)
      .filter(col("ARR_DELAY_NEW").isNotNull)

      // Remplir les retards spécifiques par 0 (assumption : pas de retard de ce type)
      .withColumn("label_weather_delay_filled",
        coalesce(col("WEATHER_DELAY"), lit(0.0))
      )
      .withColumn("label_nas_delay_filled",
        coalesce(col("NAS_DELAY"), lit(0.0))
      )
      .withColumn("label_arr_delay_filled",
        coalesce(col("ARR_DELAY_NEW"), lit(0.0))
      )

      // Flags pour tracer les valeurs manquantes
      .withColumn("label_weather_delay_was_missing",
        when(col("WEATHER_DELAY").isNull, 1).otherwise(0)
      )
      .withColumn("label_nas_delay_was_missing",
        when(col("NAS_DELAY").isNull, 1).otherwise(0)
      )
      .withColumn("label_arr_delay_was_missing",
        when(col("ARR_DELAY_NEW").isNull, 1).otherwise(0)
      )
  }

  /**
   * Crée les labels principaux selon l'approche du papier TIST
   *
   * Labels créés :
   * 1. label_weather_only : Retard météo pur (WEATHER_DELAY seul)
   * 2. label_weather_nas : Retard météo + NAS (approche recommandée papier)
   * 3. label_total : Retard total toutes causes (ARR_DELAY_NEW)
   * 4. label_weather_related : Retard potentiellement lié météo (heuristique)
   */
  private def createPrimaryLabels(df: DataFrame): DataFrame = {

    println("  Creating 4 primary delay metrics:")
    println("    1. Weather-only (WEATHER_DELAY)")
    println("    2. Weather+NAS (recommended by TIST paper)")
    println("    3. Total delay (ARR_DELAY_NEW)")
    println("    4. Weather-related (heuristic)")

    df
      // 1. Retard météo pur
      .withColumn("label_weather_only_delay",
        col("label_weather_delay_filled")
      )

      // 2. Retard météo + NAS (APPROCHE PAPIER - RECOMMANDÉE)
      .withColumn("label_weather_nas_delay",
        col("label_weather_delay_filled") + col("label_nas_delay_filled")
      )

      // 3. Retard total
      .withColumn("label_total_delay",
        col("label_arr_delay_filled")
      )

      // 4. Retard "weather-related" (heuristique intelligente)
      // Un retard est considéré "weather-related" si:
      //   - Il y a du WEATHER_DELAY direct, OU
      //   - NAS_DELAY est élevé ET il y a des conditions météo défavorables
      .withColumn("label_weather_related_delay",
        when(
          col("label_weather_delay_filled") > 0,
          // Si météo directe, prendre météo + partie du NAS (effet indirect)
          col("label_weather_delay_filled") + (col("label_nas_delay_filled") * 0.5)
        ).otherwise(
          // Sinon, juste le retard total si NAS élevé (potentiellement météo)
          when(col("label_nas_delay_filled") > 30,
            col("label_nas_delay_filled") * 0.3
          ).otherwise(0.0)
        )
      )
  }

  /**
   * Crée les labels binaires pour différents seuils
   *
   * Pour chaque seuil (15, 30, 45, 60, 90 min) et chaque type de retard,
   * crée un label binaire (0/1)
   */
  private def createThresholdLabels(df: DataFrame): DataFrame = {

    println(s"  Creating binary labels for thresholds: ${DELAY_THRESHOLDS.mkString(", ")} minutes")

    // Types de retard à considérer
    val delayTypes = Map(
      "weather_only" -> "Weather-only delays",
      "weather_nas" -> "Weather+NAS delays (TIST)",
      "total" -> "Total delays",
      "weather_related" -> "Weather-related delays (heuristic)"
    )

    var result = df

    delayTypes.foreach { case (delayType, description) =>
      println(s"    - $description")

      DELAY_THRESHOLDS.foreach { threshold =>
        val sourceCol = s"label_${delayType}_delay"
        val targetCol = s"label_is_${delayType}_delayed_${threshold}min"

        result = result.withColumn(targetCol,
          when(col(sourceCol) >= threshold, 1).otherwise(0)
        )
      }
    }

    // Label par défaut : weather_nas avec seuil 15 min (recommandation papier)
    result = result.withColumn("label",
      col("label_is_weather_nas_delayed_15min")
    )

    println(s"  ✓ Created ${delayTypes.size * DELAY_THRESHOLDS.size} threshold labels")
    println(s"  ✓ Default label set to: label_is_weather_nas_delayed_15min")

    result
  }

  /**
   * Crée des features explicatives pour comprendre la nature des retards
   */
  private def createExplanatoryFeatures(df: DataFrame): DataFrame = {

    println("  Creating explanatory features:")

    df
      // 1. Contribution météo au retard total (%)
      .withColumn("feature_weather_contribution_pct",
        when(col("label_total_delay") > 0,
          (col("label_weather_only_delay") / col("label_total_delay")) * 100
        ).otherwise(0.0)
      )

      // 2. Contribution NAS au retard total (%)
      .withColumn("feature_nas_contribution_pct",
        when(col("label_total_delay") > 0,
          (col("label_nas_delay_filled") / col("label_total_delay")) * 100
        ).otherwise(0.0)
      )

      // 3. Score de cascading (mesure l'effet domino)
      .withColumn("feature_cascading_score",
        when(col("label_weather_only_delay") > 0,
          // Si météo présente, mesurer l'amplification via NAS
          col("label_nas_delay_filled") / (col("label_weather_only_delay") + 1)
        ).otherwise(0.0)
      )

      // 4. Type de retard dominant
      .withColumn("feature_dominant_delay_type",
        when(col("label_weather_only_delay") >= col("label_nas_delay_filled"),
          lit("weather")
        ).when(col("label_nas_delay_filled") > 0,
          lit("nas")
        ).otherwise(
          lit("other")
        )
      )

      // 5. Indicateur de retard mixte (plusieurs causes)
      .withColumn("feature_is_mixed_delay",
        when(
          (col("label_weather_only_delay") > 0) &&
            (col("label_nas_delay_filled") > 0),
          1
        ).otherwise(0)
      )

      // 6. Severity score (gravité du retard)
      .withColumn("feature_delay_severity",
        when(col("label_total_delay") >= 90, lit("critical"))
          .when(col("label_total_delay") >= 60, lit("severe"))
          .when(col("label_total_delay") >= 30, lit("moderate"))
          .when(col("label_total_delay") >= 15, lit("minor"))
          .otherwise(lit("none"))
      )

    println("    ✓ weather_contribution_pct")
    println("    ✓ nas_contribution_pct")
    println("    ✓ cascading_score")
    println("    ✓ dominant_delay_type")
    println("    ✓ is_mixed_delay")
    println("    ✓ delay_severity")

    df
  }

  /**
   * Affiche les statistiques des labels créés
   */
  private def printLabelStatistics(df: DataFrame): Unit = {

    println("\n" + "=" * 80)
    println("LABEL STATISTICS")
    println("=" * 80)

    // Statistiques globales
    val totalFlights = df.count()
    println(s"\nTotal flights: ${totalFlights}")

    // Statistiques par type de retard (seuil 15 min)
    val delayTypes = Seq(
      ("weather_only", "Weather-only"),
      ("weather_nas", "Weather+NAS (TIST)"),
      ("total", "Total"),
      ("weather_related", "Weather-related")
    )

    println("\nDelay distribution (15-minute threshold):")
    println("-" * 80)
    println(f"${"Type"}%-25s ${"Delayed Flights"}%15s ${"Percentage"}%12s")
    println("-" * 80)

    delayTypes.foreach { case (delayType, label) =>
      val delayedCount = df.filter(col(s"label_is_${delayType}_delayed_15min") === 1).count()
      val percentage = (delayedCount.toDouble / totalFlights) * 100

      println(f"${label}%-25s ${delayedCount}%,15d ${percentage}%11.2f%%")
    }

    // Statistiques de contribution météo
    println("\nWeather contribution analysis:")
    println("-" * 80)

    val avgWeatherContrib = df.agg(avg("feature_weather_contribution_pct")).first().getDouble(0)
    val mixedDelayCount = df.filter(col("feature_is_mixed_delay") === 1).count()
    val mixedDelayPct = (mixedDelayCount.toDouble / totalFlights) * 100

    println(f"Average weather contribution: ${avgWeatherContrib}%.2f%%")
    println(f"Mixed delays (weather + NAS): ${mixedDelayCount}%,d (${mixedDelayPct}%.2f%%)")

    // Distribution par severity
    println("\nDelay severity distribution:")
    println("-" * 80)

    val severityDist = df.groupBy("feature_delay_severity")
      .count()
      .orderBy(desc("count"))
      .collect()

    severityDist.foreach { row =>
      val severity = row.getString(0)
      val count = row.getLong(1)
      val pct = (count.toDouble / totalFlights) * 100
      println(f"${severity}%-15s: ${count}%,10d (${pct}%5.2f%%)")
    }

    println("=" * 80)
  }

  /**
   * Méthode utilitaire pour obtenir le nom du label recommandé
   */
  def getRecommendedLabel(threshold: Int = 15): String = {
    s"label_is_weather_nas_delayed_${threshold}min"
  }

  /**
   * Méthode utilitaire pour obtenir tous les noms de labels pour un seuil donné
   */
  def getAllLabelsForThreshold(threshold: Int): Seq[String] = {
    Seq(
      s"label_is_weather_only_delayed_${threshold}min",
      s"label_is_weather_nas_delayed_${threshold}min",
      s"label_is_total_delayed_${threshold}min",
      s"label_is_weather_related_delayed_${threshold}min"
    )
  }
}