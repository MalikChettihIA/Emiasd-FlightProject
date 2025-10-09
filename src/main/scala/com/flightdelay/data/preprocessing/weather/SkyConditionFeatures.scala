package com.flightdelay.data.preprocessing.weather

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.util.Try

object SkyConditionFeatures {

  /**
   * Extrait le trigramme le plus critique d'une observation SkyCondition
   * Ordre de priorité : VV > OVC > BKN > SCT > FEW > CLR
   */
  val getMostCriticalSky = udf((skyCondition: String) => {
    if (skyCondition == null || skyCondition.trim.isEmpty) {
      "UNKNOWN"
    } else {
      val codes = skyCondition.split(" ").map(_.trim).filter(_.nonEmpty)

      // Priorité du pire au meilleur
      if (codes.exists(_.startsWith("VV"))) "VV"
      else if (codes.exists(_.startsWith("OVC"))) "OVC"
      else if (codes.exists(_.startsWith("BKN"))) "BKN"
      else if (codes.exists(_.startsWith("SCT"))) "SCT"
      else if (codes.exists(_.startsWith("FEW"))) "FEW"
      else if (codes.exists(_.startsWith("CLR")) || codes.exists(_.startsWith("SKC"))) "CLR"
      else "UNKNOWN"
    }
  })

  /**
   * Extrait l'altitude de la couche nuageuse la plus basse (en pieds)
   */
  val getLowestCloudHeight = udf((skyCondition: String) => {
    if (skyCondition == null || skyCondition.trim.isEmpty) {
      99999 // Valeur sentinelle pour pas de nuages
    } else {
      val codes = skyCondition.split(" ").filter(_.length > 3)

      val heights = codes.flatMap { code =>
        Try(code.substring(3).toInt * 100).toOption
      }

      if (heights.nonEmpty) heights.min else 99999
    }
  })

  /**
   * Calcule l'altitude du plafond (couche BKN ou OVC la plus basse)
   */
  val getCeiling = udf((skyCondition: String) => {
    if (skyCondition == null || skyCondition.trim.isEmpty) {
      99999
    } else {
      val codes = skyCondition.split(" ").filter(_.nonEmpty)

      val ceilingCodes = codes.filter(c =>
        c.startsWith("BKN") || c.startsWith("OVC") || c.startsWith("VV")
      )

      val ceilings = ceilingCodes.flatMap { code =>
        if (code.startsWith("VV")) Some(0) // Visibilité verticale = plafond 0
        else if (code.length > 3) Try(code.substring(3).toInt * 100).toOption
        else None
      }

      if (ceilings.nonEmpty) ceilings.min else 99999
    }
  })

  /**
   * Compte le nombre de couches nuageuses
   */
  val countCloudLayers = udf((skyCondition: String) => {
    if (skyCondition == null || skyCondition.trim.isEmpty) {
      0
    } else {
      skyCondition.split(" ").filter(_.nonEmpty).length
    }
  })

  /**
   * Calcule un score de risque basé sur la couverture nuageuse (0-5)
   */
  val calculateCloudRiskScore = udf((skyCondition: String) => {
    if (skyCondition == null || skyCondition.trim.isEmpty) {
      1.0 // Risque faible par défaut
    } else {
      val codes = skyCondition.split(" ")

      if (codes.exists(_.startsWith("VV"))) 5.0      // Obscuration totale
      else if (codes.exists(_.startsWith("OVC"))) 4.0 // Ciel couvert
      else if (codes.exists(_.startsWith("BKN"))) 3.0 // Fragmenté
      else if (codes.exists(_.startsWith("SCT"))) 2.0 // Épars
      else if (codes.exists(_.startsWith("FEW"))) 1.0 // Quelques nuages
      else if (codes.exists(_.startsWith("CLR"))) 0.0 // Clair
      else 2.0 // Inconnu = risque moyen
    }
  })

  /**
   * Détecte si le plafond est bas (< 1000 pieds)
   */
  val hasLowCeiling = udf((ceiling: Int) => {
    ceiling < 1000
  })

  /**
   * Applique toutes les transformations pour SkyCondition
   */
  def createSkyConditionFeatures(df: DataFrame): DataFrame = {
    df.withColumn("most_critical_sky", getMostCriticalSky(col("SkyCondition")))
      .withColumn("lowest_cloud_height", getLowestCloudHeight(col("SkyCondition")))
      .withColumn("ceiling", getCeiling(col("SkyCondition")))
      .withColumn("num_cloud_layers", countCloudLayers(col("SkyCondition")))
      .withColumn("cloud_risk_score", calculateCloudRiskScore(col("SkyCondition")))
      .withColumn("has_overcast", col("SkyCondition").contains("OVC"))
      .withColumn("has_broken", col("SkyCondition").contains("BKN"))
      .withColumn("has_obscured", col("SkyCondition").contains("VV"))
      .withColumn("is_clear", col("SkyCondition").contains("CLR") || col("SkyCondition").contains("SKC"))
      .withColumn("has_low_ceiling", hasLowCeiling(col("ceiling")))
  }
}


