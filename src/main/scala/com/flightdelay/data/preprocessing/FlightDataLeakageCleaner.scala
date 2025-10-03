package com.flightdelay.data.preprocessing

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
    println("")
    println("")
    println("----------------------------------------------------------------------------------------------------------")
    println("--> [FlightDataLeakageCleaner] Flight Data Leakage Cleaner - Start ...")
    println("----------------------------------------------------------------------------------------------------------")

    val originalColumns = enrichedData.columns.length
    println(s"Original Column Count: $originalColumns")

    // Identifier les colonnes à supprimer qui existent réellement
    val columnsToRemove = LEAKAGE_COLUMNS.filter(enrichedData.columns.contains)

    if (columnsToRemove.isEmpty) {
      println("\n✓ No leakage columns found - Data is clean!")
    } else {
      println(s"\n⚠ Found ${columnsToRemove.length} leakage columns to remove:")
      columnsToRemove.foreach(col => println(s"  - $col"))
    }

    // Supprimer les colonnes de leakage
    val cleanedData = enrichedData.drop(columnsToRemove: _*)

    val finalColumns = cleanedData.columns.length
    val removedColumns = originalColumns - finalColumns

    println(s"\n=== Leakage Cleaning Summary ===")
    println(s"Original Columns: $originalColumns")
    println(s"Final Columns: $finalColumns")
    println(s"Removed Columns: $removedColumns")
    println(s"Dataset size: ${cleanedData.count()}")

    // Vérifier que les labels sont toujours présents
    val labelColumns = cleanedData.columns.filter(_.startsWith("label_"))
    println(s"\n✓ Kept ${labelColumns.length} label columns for ML training")

    println("")
    println("--> [FlightDataLeakageCleaner] Flight Data Leakage Cleaner - End ...")
    println("----------------------------------------------------------------------------------------------------------")
    println("")
    println("")

    cleanedData
  }
}
