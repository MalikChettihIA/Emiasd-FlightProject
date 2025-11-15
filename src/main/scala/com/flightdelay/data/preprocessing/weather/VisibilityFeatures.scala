package com.flightdelay.data.preprocessing.weather

import com.flightdelay.config.AppConfiguration
import com.flightdelay.utils.DebugUtils.debug
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.flightdelay.utils.DebugUtils._

object VisibilityFeatures {

  /**
   * Applique toutes les transformations pour Visibility
   * OPTIMISÉ : Utilise uniquement des expressions Spark natives (pas d'UDF)
   */
  def createVisibilityFeatures(df: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.data.preprocessing.weather.VisibilityFeatures.createVisibilityFeatures()")
    df
      // 1. Nettoyer et convertir la visibilité en miles
      // Hypothèse : les valeurs sont en dixièmes de miles
      .withColumn("feature_visibility_miles",
        when(col("Visibility").isNull ||
          trim(col("Visibility")) === "" ||
          col("Visibility") === "M", lit(10.0))  // Valeur par défaut pour données manquantes
          .otherwise(
            // Convertir la string en double, gérer les erreurs avec coalesce
            // Si conversion échoue, retourner 10.0
            coalesce(
              least(
                col("Visibility").cast(DoubleType) / 10.0,  // Conversion depuis dixièmes de miles
                lit(10.0)  // Plafonner à 10 miles max
              ),
              lit(10.0)  // Fallback si cast échoue
            )
          )
      )

      // 2. Convertir en kilomètres
      .withColumn("feature_visibility_km",
        col("feature_visibility_miles") * 1.609
      )

      // 3. Catégoriser la visibilité selon les standards aviation
      .withColumn("feature_visibility_category",
        when(col("feature_visibility_miles") < 0.5, lit("LIFR"))       // Low IFR
          .when(col("feature_visibility_miles") < 1.0, lit("IFR_LOW")) // IFR bas
          .when(col("feature_visibility_miles") < 3.0, lit("IFR"))     // Instrument Flight Rules
          .when(col("feature_visibility_miles") < 5.0, lit("MVFR"))    // Marginal VFR
          .when(col("feature_visibility_miles") < 10.0, lit("VFR"))    // Visual Flight Rules
          .otherwise(lit("VFR_HIGH"))                                  // > 10 miles, excellente
      )

      // 4. Calculer le score de risque basé sur la visibilité (0-5)
      .withColumn("feature_visibility_risk_score",
        when(col("feature_visibility_miles") < 0.5, lit(5.0))   // Très dangereux
          .when(col("feature_visibility_miles") < 1.0, lit(4.0)) // Dangereux
          .when(col("feature_visibility_miles") < 3.0, lit(3.0)) // Modéré (IFR)
          .when(col("feature_visibility_miles") < 5.0, lit(2.0)) // Léger (MVFR)
          .when(col("feature_visibility_miles") < 10.0, lit(1.0)) // Faible (VFR)
          .otherwise(lit(0.0))                                    // Aucun risque
      )

      // 5. Détecter les conditions de faible visibilité (< 3 miles)
      .withColumn("feature_is_low_visibility",
        (col("feature_visibility_miles") < 3.0).cast(IntegerType)
      )

      // 6. Détecter les conditions de très faible visibilité (< 1 mile)
      .withColumn("feature_is_very_low_visibility",
        (col("feature_visibility_miles") < 1.0).cast(IntegerType)
      )

      // 7. Normaliser la visibilité (0-1)
      .withColumn("feature_visibility_normalized",
        when(col("feature_visibility_miles") > 0, col("feature_visibility_miles") / 10.0)
          .otherwise(0.0)
      )

      // 8. Calculer l'inverse de la visibilité (pour modèles linéaires)
      .withColumn("feature_visibility_inverse",
        when(col("feature_visibility_miles") > 0, lit(1.0) / col("feature_visibility_miles"))
          .otherwise(10.0)
      )
  }
}
