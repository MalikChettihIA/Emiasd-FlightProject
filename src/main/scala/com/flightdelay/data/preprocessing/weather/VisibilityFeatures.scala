package com.flightdelay.data.preprocessing.weather

import org.apache.spark.sql.DataFrame


object VisibilityFeatures {

  import org.apache.spark.sql.functions._
  import scala.util.Try

  /**
   * Nettoie et convertit la visibilité en miles
   * Hypothèse : les valeurs sont en dixièmes de miles
   */
  val cleanVisibility = udf((visibility: String) => {
    if (visibility == null || visibility.trim.isEmpty || visibility == "M") {
      10.0 // Valeur par défaut pour données manquantes (bonne visibilité)
    } else {
      Try(visibility.toDouble).toOption match {
        case Some(v) => {
          val miles = v / 10.0 // Conversion depuis dixièmes de miles
          math.min(miles, 10.0) // Plafonner à 10 miles max
        }
        case None => 10.0
      }
    }
  })

  /**
   * Catégorise la visibilité selon les standards aviation
   */
  val categorizeVisibility = udf((visibilityMiles: Double) => {
    visibilityMiles match {
      case v if v < 0.5  => "LIFR"      // Low IFR
      case v if v < 1.0  => "IFR_LOW"   // IFR bas
      case v if v < 3.0  => "IFR"       // Instrument Flight Rules
      case v if v < 5.0  => "MVFR"      // Marginal VFR
      case v if v < 10.0 => "VFR"       // Visual Flight Rules
      case _             => "VFR_HIGH"  // > 10 miles, excellente
    }
  })

  /**
   * Calcule un score de risque basé sur la visibilité (0-5)
   */
  val calculateVisibilityRiskScore = udf((visibilityMiles: Double) => {
    visibilityMiles match {
      case v if v < 0.5  => 5.0  // Très dangereux
      case v if v < 1.0  => 4.0  // Dangereux
      case v if v < 3.0  => 3.0  // Modéré (IFR)
      case v if v < 5.0  => 2.0  // Léger (MVFR)
      case v if v < 10.0 => 1.0  // Faible (VFR)
      case _             => 0.0  // Aucun risque
    }
  })

  /**
   * Détecte les conditions de faible visibilité (< 3 miles)
   */
  val isLowVisibility = udf((visibilityMiles: Double) => {
    visibilityMiles < 3.0
  })

  /**
   * Détecte les conditions de très faible visibilité (< 1 mile)
   */
  val isVeryLowVisibility = udf((visibilityMiles: Double) => {
    visibilityMiles < 1.0
  })

  /**
   * Applique toutes les transformations pour Visibility
   */
  def createVisibilityFeatures(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._

    df.withColumn("visibility_miles", cleanVisibility(col("Visibility")))
      .withColumn("visibility_km", col("visibility_miles") * 1.609)
      .withColumn("visibility_category", categorizeVisibility(col("visibility_miles")))
      .withColumn("visibility_risk_score", calculateVisibilityRiskScore(col("visibility_miles")))
      .withColumn("is_low_visibility", isLowVisibility(col("visibility_miles")))
      .withColumn("is_very_low_visibility", isVeryLowVisibility(col("visibility_miles")))
      .withColumn("visibility_normalized",
        when(col("visibility_miles") > 0, col("visibility_miles") / 10.0)
          .otherwise(0.0))
      .withColumn("visibility_inverse",
        when(col("visibility_miles") > 0, lit(1.0) / col("visibility_miles"))
          .otherwise(10.0))
  }
}

weatherWithFeatureDF = VisibilityFeatures.createVisibilityFeatures(weatherWithFeatureDF)

weatherWithFeatureDF.select(
  "Visibility",
  "visibility_miles",
  "visibility_km",
  "visibility_category",
  "visibility_risk_score",
  "is_very_low_visibility",
  "visibility_normalized",
  "visibility_inverse").show(20)
