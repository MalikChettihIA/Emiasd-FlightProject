package com.flightdelay.data.preprocessing.weather

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object WeatherInteractionFeatures {

  /**
   * Calcule un indice de sévérité météorologique combiné
   * Prend en compte à la fois la couverture nuageuse et la visibilité
   */
  val calculateWeatherSeverityIndex = udf(
    (cloudRisk: Double, visibilityRisk: Double, ceiling: Int, visibility: Double) => {
      // Combinaison pondérée
      val baseScore = (cloudRisk * 0.4) + (visibilityRisk * 0.6)

      // Pénalité pour plafond très bas
      val ceilingPenalty = if (ceiling < 500) 2.0
      else if (ceiling < 1000) 1.0
      else 0.0

      // Pénalité pour visibilité très basse
      val visibilityPenalty = if (visibility < 0.5) 2.0
      else if (visibility < 1.0) 1.0
      else 0.0

      math.min(baseScore + ceilingPenalty + visibilityPenalty, 10.0)
    }
  )

  /**
   * Détermine si les conditions sont VFR (Visual Flight Rules)
   */
  val isVFRConditions = udf((visibility: Double, ceiling: Int) => {
    visibility >= 5.0 && ceiling >= 3000
  })

  /**
   * Détermine si les conditions sont IFR (Instrument Flight Rules)
   */
  val isIFRConditions = udf((visibility: Double, ceiling: Int) => {
    visibility < 3.0 || ceiling < 1000
  })

  /**
   * Détermine si CAT II/III est requis
   */
  val requiresCATII = udf((visibility: Double, ceiling: Int) => {
    visibility < 1.0 || ceiling < 200
  })

  /**
   * Calcule le niveau de risque opérationnel (0-4)
   * 0=None, 1=Low, 2=Moderate, 3=High, 4=Critical
   */
  val calculateOperationsRiskLevel = udf(
    (visibility: Double, ceiling: Int, hasObscured: Boolean) => {
      if (hasObscured || visibility < 0.25 || ceiling < 100) 4 // Critical
      else if (visibility < 0.5 || ceiling < 200) 3 // High
      else if (visibility < 1.0 || ceiling < 500) 3 // High
      else if (visibility < 3.0 || ceiling < 1000) 2 // Moderate
      else if (visibility < 5.0 || ceiling < 3000) 1 // Low
      else 0 // None
    }
  )

  /**
   * Applique toutes les features d'interaction
   */
  def createInteractionFeatures(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._

    df.withColumn("feature_weather_severity_index",
        calculateWeatherSeverityIndex(
          col("feature_cloud_risk_score"),
          col("feature_visibility_risk_score"),
          col("feature_ceiling"),
          col("feature_visibility_miles")
        ))
      .withColumn("feature_is_vfr_conditions",
        isVFRConditions(col("feature_visibility_miles"), col("feature_ceiling")))
      .withColumn("feature_is_ifr_conditions",
        isIFRConditions(col("feature_visibility_miles"), col("feature_ceiling")))
      .withColumn("feature_requires_cat_ii",
        requiresCATII(col("feature_visibility_miles"), col("feature_ceiling")))
      .withColumn("feature_operations_risk_level",
        calculateOperationsRiskLevel(
          col("feature_visibility_miles"),
          col("feature_ceiling"),
          col("feature_has_obscured")
        ))
  }
}

