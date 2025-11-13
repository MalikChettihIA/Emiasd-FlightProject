package com.flightdelay.data.preprocessing.weather

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Feature engineering pression atmosphérique (point d'entrée unique).
 * - Tendance (codes WMO) -> signe : -1 (baisse), 0 (stable), +1 (hausse), null (inconnu)
 * - Variation absolue de pression -> risque binaire + bucket interprétable
 * - Interaction pression × visibilité -> score + binning par quantiles
 */
object WeatherPressionFeatures {

  // ---- Colonnes en dur ----
  private val TendencyCol        = "PressureTendency"
  private val ChangeCol          = "PressureChange"
  private val VisibilityRiskCol  = "feature_visibility_risk_score"
  private val StrongChangeThresh = 20.0   // hPa (ou l’unité de votre colonne)

  /**
   * Point d’entrée unique : enrichit le DataFrame avec les features pression & interactions.
   *
   * Colonnes ajoutées :
   *  - press_trend_sign: Int (-1/0/+1, null si inconnu)
   *  - press_change_raw: Double (PressureChange casté)
   *  - press_change_abs: Double (|ΔP|)
   *  - feature_pressure_variation_risk: Int (1 si |ΔP| > 20, sinon 0)
   *  - feature_pressure_bucket: String ("stable" / "transition" / "perturbe" / "unknown")
   *  - feature_pressure_visibility_combo: Double (|ΔP| * feature_visibility_risk_score)
   *  - feature_pressure_vis_combo_bin: String ("faible" / "modere" / "eleve" / "tres_eleve")
   */
  def createPressureFeatures(df: DataFrame): DataFrame = {
    println("\n[Feature Engineering] WeatherPressionFeatures.createPressureFeatures")
    println(s"  - Using columns: $TendencyCol, $ChangeCol, $VisibilityRiskCol (threshold=$StrongChangeThresh)")

    var cur = df

    // A) Tendance -> signe
    if (cur.columns.contains(TendencyCol)) {
      // Codes WMO rappel :
      // 0: steady, 1/2/3: rising, 4/5/6/7/8: falling
      cur = cur.withColumn(
        "press_trend_sign",
        when(col(TendencyCol).isin("1","2","3"), lit( 1))
          .when(col(TendencyCol).isin("4","5","6","7","8"), lit(-1))
          .when(col(TendencyCol) === "0", lit(0))
          .otherwise(lit(null).cast(IntegerType))
      )

      println("  - press_trend_sign distribution:")
    } else {
      println(s"  ! Column '$TendencyCol' not found. Skipping trend features.")
    }

    // B) Variation |ΔP| -> risque & bucket
    if (cur.columns.contains(ChangeCol)) {
      cur = cur
        .withColumn("press_change_raw", col(ChangeCol).cast(DoubleType))
        .withColumn("press_change_abs", abs(col("press_change_raw")))
        .withColumn(
          "feature_pressure_variation_risk",
          when(col("press_change_abs") > StrongChangeThresh, 1).otherwise(0)
        )
        .withColumn(
          "feature_pressure_bucket",
          when(col("press_change_abs").isNull, "unknown")
            .when(col("press_change_abs") <= 9.0, "stable")
            .when(col("press_change_abs") <= 20.0, "transition")
            .otherwise("perturbe")
        )

      println("  - feature_pressure_bucket distribution:")
    } else {
      println(s"  ! Column '$ChangeCol' not found. Skipping pressure-change features.")
    }

    // C) Interaction pression × visibilité
    if (cur.columns.contains(ChangeCol)) {
      val hasVis = cur.columns.contains(VisibilityRiskCol)
      cur = cur.withColumn(
        "feature_pressure_visibility_combo",
        col("press_change_abs") * (if (hasVis) coalesce(col(VisibilityRiskCol).cast(DoubleType), lit(0.0)) else lit(0.0))
      )

      // Binning par quantiles (si on a des valeurs non nulles)
      val nonNull = cur.select(col("feature_pressure_visibility_combo")).na.drop()
      val qs =
        if (nonNull.head(1).isEmpty) Array(0.0, 0.0, 0.0)
        else nonNull.stat.approxQuantile("feature_pressure_visibility_combo", Array(0.25, 0.5, 0.75), 1e-3)

      val (q1, q2, q3) = (qs(0), qs(1), qs(2))
      println(f"  - combo quantiles: q1=$q1%.3f, q2=$q2%.3f, q3=$q3%.3f")

      cur = cur.withColumn(
        "feature_pressure_vis_combo_bin",
        when(col("feature_pressure_visibility_combo").isNull, lit("faible"))
          .when(col("feature_pressure_visibility_combo") <= q1, lit("faible"))
          .when(col("feature_pressure_visibility_combo") <= q2, lit("modere"))
          .when(col("feature_pressure_visibility_combo") <= q3, lit("eleve"))
          .otherwise(lit("tres_eleve"))
      )

      println("  - feature_pressure_vis_combo_bin distribution:")
    }

    cur
  }
}