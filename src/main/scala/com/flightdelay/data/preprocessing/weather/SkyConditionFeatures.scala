package com.flightdelay.data.preprocessing.weather

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SkyConditionFeatures {

  /**
   * Applique toutes les transformations pour SkyCondition
   * OPTIMISÉ : Utilise uniquement des expressions Spark natives (pas d'UDF)
   */
  def createSkyConditionFeatures(df: DataFrame): DataFrame = {

    df
      // 1. Extraire le trigramme le plus critique
      // Ordre de priorité : VV > OVC > BKN > SCT > FEW > CLR
      .withColumn("feature_most_critical_sky",
        when(col("SkyCondition").isNull || trim(col("SkyCondition")) === "", lit("UNKNOWN"))
          .when(col("SkyCondition").contains("VV"), lit("VV"))
          .when(col("SkyCondition").contains("OVC"), lit("OVC"))
          .when(col("SkyCondition").contains("BKN"), lit("BKN"))
          .when(col("SkyCondition").contains("SCT"), lit("SCT"))
          .when(col("SkyCondition").contains("FEW"), lit("FEW"))
          .when(col("SkyCondition").contains("CLR") || col("SkyCondition").contains("SKC"), lit("CLR"))
          .otherwise(lit("UNKNOWN"))
      )

      // 2. Compter le nombre de couches nuageuses
      .withColumn("feature_num_cloud_layers",
        when(col("SkyCondition").isNull || trim(col("SkyCondition")) === "", lit(0))
          .otherwise(
            size(split(trim(col("SkyCondition")), "\\s+"))
          )
      )

      // 3. Calculer le score de risque basé sur la couverture nuageuse (0-5)
      .withColumn("feature_cloud_risk_score",
        when(col("SkyCondition").isNull || trim(col("SkyCondition")) === "", lit(1.0))
          .when(col("SkyCondition").contains("VV"), lit(5.0))      // Obscuration totale
          .when(col("SkyCondition").contains("OVC"), lit(4.0))     // Ciel couvert
          .when(col("SkyCondition").contains("BKN"), lit(3.0))     // Fragmenté
          .when(col("SkyCondition").contains("SCT"), lit(2.0))     // Épars
          .when(col("SkyCondition").contains("FEW"), lit(1.0))     // Quelques nuages
          .when(col("SkyCondition").contains("CLR") || col("SkyCondition").contains("SKC"), lit(0.0)) // Clair
          .otherwise(lit(2.0))                                     // Inconnu = risque moyen
      )

      // 4. Détection des conditions spécifiques
      .withColumn("feature_has_overcast", when(col("SkyCondition").contains("OVC"), 1).otherwise(0))
      .withColumn("feature_has_broken", when(col("SkyCondition").contains("BKN"), 1).otherwise(0))
      .withColumn("feature_has_obscured", when(col("SkyCondition").contains("VV"), 1).otherwise(0))
      .withColumn("feature_is_clear",
        (col("SkyCondition").contains("CLR") || col("SkyCondition").contains("SKC")).cast(IntegerType))

      // 5. Extraire les altitudes des couches nuageuses (en pieds)
      // Approche : Extraire jusqu'à 4 couches (max typique), puis calculer min/max
      .withColumn("_temp_sky_clean",
        when(col("SkyCondition").isNull, lit(""))
          .otherwise(col("SkyCondition"))
      )

      // Extraire les hauteurs de toutes les couches avec regexp_extract_all
      // Pattern: Capture les nombres de 3 chiffres après FEW/SCT/BKN/OVC
      .withColumn("_temp_all_heights",
        regexp_extract_all(
          col("_temp_sky_clean"),
          lit("(FEW|SCT|BKN|OVC)(\\d{3})"),  // Capture le code et les 3 chiffres
          lit(2)  // Groupe 2 = les chiffres
        )
      )

      // Convertir les strings en ints et multiplier par 100 pour obtenir les pieds
      .withColumn("_temp_heights_array",
        transform(
          col("_temp_all_heights"),
          x => (x.cast(IntegerType) * 100)
        )
      )

      // Calculer l'altitude de la couche la plus basse
      .withColumn("feature_lowest_cloud_height",
        when(size(col("_temp_heights_array")) > 0,
          array_min(col("_temp_heights_array")))
          .otherwise(lit(99999))
      )

      // 6. Calculer le plafond (couche BKN, OVC ou VV la plus basse)
      .withColumn("_temp_ceiling_heights",
        when(col("SkyCondition").contains("VV"), array(lit(0)))  // VV = plafond 0
          .otherwise(
            regexp_extract_all(
              col("_temp_sky_clean"),
              lit("(BKN|OVC)(\\d{3})"),  // Seulement BKN et OVC
              lit(2)
            )
          )
      )

      .withColumn("_temp_ceiling_array",
        when(col("SkyCondition").contains("VV"), array(lit(0)))
          .otherwise(
            transform(
              col("_temp_ceiling_heights"),
              x => (x.cast(IntegerType) * 100)
            )
          )
      )

      .withColumn("feature_ceiling",
        when(size(col("_temp_ceiling_array")) > 0,
          array_min(col("_temp_ceiling_array")))
          .otherwise(lit(99999))
      )

      // 7. Déterminer si le plafond est bas (< 1000 pieds)
      .withColumn("feature_has_low_ceiling",
        (col("feature_ceiling") < 1000).cast(IntegerType)
      )

      // Nettoyer les colonnes temporaires
      .drop("_temp_sky_clean", "_temp_all_heights", "_temp_heights_array",
        "_temp_ceiling_heights", "_temp_ceiling_array")
  }
}
