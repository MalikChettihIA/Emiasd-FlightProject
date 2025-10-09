package com.flightdelay.data.preprocessing.flights

import com.flightdelay.data.preprocessing.DataPreprocessor
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Classe spécialisée pour supprimer les colonnes causant du data leakage.
 *
 * Data leakage = utiliser des informations qui ne seraient pas disponibles au moment
 * de la prédiction en production (informations connues APRÈS le vol).
 *
 * Colonnes à supprimer (basées sur le schéma FlightDataLoader) :
 * - ARR_DELAY_NEW : Le retard réel à l'arrivée (c'est exactement ce qu'on veut prédire !)
 * - WEATHER_DELAY : Retard dû à la météo (connu APRÈS le vol)
 * - NAS_DELAY : Retard dû au système NAS (connu APRÈS le vol)
 *
 * Cette classe s'assure que le modèle ML n'utilise QUE des informations disponibles
 * AVANT le départ du vol pour faire ses prédictions.
 */
object FlightDataLeakageCleaner extends DataPreprocessor {

  /**
   * Liste des colonnes qui causent du data leakage
   * Ces colonnes contiennent des informations connues SEULEMENT après le vol
   */
  private val LEAKAGE_COLUMNS = Seq(
    "ARR_DELAY_NEW",    // Retard réel à l'arrivée - C'EST LA CIBLE !
    "WEATHER_DELAY",    // Retard météo (connu APRÈS le vol)
    "NAS_DELAY"         // Retard NAS (connu APRÈS le vol)
  )

  /**
   * Supprime les 3 colonnes causant du data leakage
   * @param enrichedData DataFrame avec toutes les features et labels
   * @param spark Session Spark
   * @return DataFrame nettoyé sans colonnes de leakage
   */
  override def preprocess(enrichedData: DataFrame)(implicit spark: SparkSession): DataFrame = {
    println("\n" + "=" * 80)
    println("[STEP 2][LeakageCleaner] Data Leakage Removal - Start")
    println("=" * 80)

    val originalColumns = enrichedData.columns.length
    println(s"\nOriginal columns: $originalColumns")

    // Identify columns to remove that actually exist
    val columnsToRemove = LEAKAGE_COLUMNS.filter(enrichedData.columns.contains)

    if (columnsToRemove.isEmpty) {
      println("  - No leakage columns found - Data is clean!")
    } else {
      println(s"\n  - Found ${columnsToRemove.length} leakage columns to remove:")
      columnsToRemove.foreach(col => println(s"  - $col"))
    }

    // Remove leakage columns
    val cleanedData = enrichedData.drop(columnsToRemove: _*)

    val finalColumns = cleanedData.columns.length
    val removedColumns = originalColumns - finalColumns

    println(s"\n" + "=" * 50)
    println("Leakage Removal Summary")
    println("=" * 50)
    println(f"Original columns:    $originalColumns%3d")
    println(f"Final columns:       $finalColumns%3d")
    println(f"Removed columns:     $removedColumns%3d")
    println(f"Dataset size:        ${cleanedData.count()}%,10d records")

    // Verify labels are still present
    val labelColumns = cleanedData.columns.filter(_.startsWith("label_"))
    println(f"\n- Kept ${labelColumns.length}%3d label columns for ML training")
    println("=" * 50)

    println("\n" + "=" * 80)
    println("[LeakageCleaner] Data Leakage Removal - End")
    println("=" * 80 + "\n")

    cleanedData
  }
}
