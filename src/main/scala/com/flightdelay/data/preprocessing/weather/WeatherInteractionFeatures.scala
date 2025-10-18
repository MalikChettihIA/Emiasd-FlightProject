package com.flightdelay.data.preprocessing.weather

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object WeatherInteractionFeatures {

  /**
   * Applique toutes les features d'interaction
   * OPTIMISÉ : Utilise uniquement des expressions Spark natives (pas d'UDF)
   */
  def createInteractionFeatures(df: DataFrame): DataFrame = {

    df
      // 1. Calculer l'indice de sévérité météorologique combiné
      // Combinaison pondérée + pénalités pour conditions critiques
      .withColumn("_temp_base_score",
        (col("feature_cloud_risk_score") * 0.4) + (col("feature_visibility_risk_score") * 0.6)
      )

      .withColumn("_temp_ceiling_penalty",
        when(col("feature_ceiling") < 500, lit(2.0))
          .when(col("feature_ceiling") < 1000, lit(1.0))
          .otherwise(lit(0.0))
      )

      .withColumn("_temp_visibility_penalty",
        when(col("feature_visibility_miles") < 0.5, lit(2.0))
          .when(col("feature_visibility_miles") < 1.0, lit(1.0))
          .otherwise(lit(0.0))
      )

      .withColumn("feature_weather_severity_index",
        least(
          col("_temp_base_score") + col("_temp_ceiling_penalty") + col("_temp_visibility_penalty"),
          lit(10.0)
        )
      )

      // 2. Déterminer si les conditions sont VFR (Visual Flight Rules)
      .withColumn("feature_is_vfr_conditions",
        ((col("feature_visibility_miles") >= 5.0) &&
          (col("feature_ceiling") >= 3000)).cast(IntegerType)
      )

      // 3. Déterminer si les conditions sont IFR (Instrument Flight Rules)
      .withColumn("feature_is_ifr_conditions",
        ((col("feature_visibility_miles") < 3.0) ||
          (col("feature_ceiling") < 1000)).cast(IntegerType)
      )

      // 4. Déterminer si CAT II/III est requis
      .withColumn("feature_requires_cat_ii",
        ((col("feature_visibility_miles") < 1.0) ||
          (col("feature_ceiling") < 200)).cast(IntegerType)
      )

      // 5. Calculer le niveau de risque opérationnel (0-4)
      // 0=None, 1=Low, 2=Moderate, 3=High, 4=Critical
      .withColumn("feature_operations_risk_level",
        when(col("feature_has_obscured") === true ||
          col("feature_visibility_miles") < 0.25 ||
          col("feature_ceiling") < 100, lit(4))  // Critical
          .when(col("feature_visibility_miles") < 0.5 ||
            col("feature_ceiling") < 200, lit(3))  // High
          .when(col("feature_visibility_miles") < 1.0 ||
            col("feature_ceiling") < 500, lit(3))  // High
          .when(col("feature_visibility_miles") < 3.0 ||
            col("feature_ceiling") < 1000, lit(2)) // Moderate
          .when(col("feature_visibility_miles") < 5.0 ||
            col("feature_ceiling") < 3000, lit(1)) // Low
          .otherwise(lit(0))                       // None
      )

      // Nettoyer les colonnes temporaires
      .drop("_temp_base_score", "_temp_ceiling_penalty", "_temp_visibility_penalty")
  }
}
