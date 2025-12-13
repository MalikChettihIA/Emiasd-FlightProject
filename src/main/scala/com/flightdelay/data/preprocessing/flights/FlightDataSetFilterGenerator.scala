package com.flightdelay.data.preprocessing.flights

import com.flightdelay.config.AppConfiguration
import com.flightdelay.data.preprocessing.DataPreprocessor
import com.flightdelay.utils.DebugUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * Génère des colonnes binaires D1, D2_<seuil>, D3, D4 (1 ou 0) selon la logique métier :
 *
 * - D1 : vols dont le retard total est imputé *seulement* à WEATHER et/ou NAS
 *        (ARR_DELAY_NEW ≈ WEATHER_DELAY + NAS_DELAY à epsilon près, et ARR_DELAY_NEW > 0)
 * - D2_<T> : vols avec WEATHER_DELAY > 0 et NAS_DELAY >= T
 * - D3 : vols avec WEATHER_DELAY > 0 ou NAS_DELAY > 0
 * - D4 : tous les vols avec ARR_DELAY_NEW > 0
 *
 * Nulls gérés via coalesce(..., 0.0)
 */
object FlightDataSetFilterGenerator extends DataPreprocessor {

  private val D2_THRESHOLDS_MINUTES: Seq[Int] = Seq(15, 30, 45, 60, 90)
  private val EPSILON_EQUALITY_MIN = 0.05

  private def nzDouble(colName: String): Column =
    coalesce(col(colName).cast("double"), lit(0.0))

  private def validateRequiredColumns(df: DataFrame): Unit = {
    val required = Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY")
    val missing = required.filterNot(df.columns.contains)
    require(
      missing.isEmpty,
      s"Colonnes manquantes: ${missing.mkString(", ")} (requis: ${required.mkString(", ")})"
    )
  }

  /**
   * Ajoute les colonnes D1, D2_<seuil>, D3, D4 en binaire (1 / 0)
   */
  def withDelayFilters(df: DataFrame, epsilon: Double = EPSILON_EQUALITY_MIN): DataFrame = {
    validateRequiredColumns(df)

    val arr  = nzDouble("ARR_DELAY_NEW")
    val wthr = nzDouble("WEATHER_DELAY")
    val nas  = nzDouble("NAS_DELAY")

    val d4 = when(arr > 0, 1).otherwise(0)
    val d3 = when((wthr > 0) or (nas > 0), 1).otherwise(0)
    val d1 = when((arr > 0) && (
      (abs(arr - (wthr + nas)) <= epsilon) or
        (abs(arr - wthr) <= epsilon) or
        (abs(arr - nas) <= epsilon)
      ), 1).otherwise(0)

    // Appliquer les colonnes de base
    val baseDf = df
      .withColumn("D4", d4)
      .withColumn("D3", d3)
      .withColumn("D1", d1)

    // Ajouter D2_<seuil>
    val dfWithD2 = D2_THRESHOLDS_MINUTES.foldLeft(baseDf) { (acc, thr) =>
      val colName = s"D2_$thr"
      val d2col = when((wthr > 0) or (nas >= thr) , 1).otherwise(0)
      acc.withColumn(colName, d2col)
    }

    dfWithD2
  }

  /**
   * Méthode exécutable pour test (facultative)
   */
  def preprocess(flightData: DataFrame, weatherData: DataFrame, wBANAirportTimezoneData: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame  = {

    info("- Calling com.flightdelay.data.preprocessing.flights.FlightDataSetFilterGenerator.preprocess")

    val enriched = withDelayFilters(flightData)

    //whenDebug{
      val total = enriched.count()
      info(s"[FlightDataSetFilterGenerator] Total vols: $total")

      val colsToLog = Seq("D1", "D3", "D4") ++ D2_THRESHOLDS_MINUTES.map(t => s"D2_$t")
      colsToLog.foreach { c =>
        val n = enriched.filter(col(c) === 1).count()
        val pct = if (total > 0) (n.toDouble / total * 100) else 0.0
        info(f" - $c%-6s : $n%8d vols (${pct}%.2f%%)")
      }
    //}

    enriched
  }


}