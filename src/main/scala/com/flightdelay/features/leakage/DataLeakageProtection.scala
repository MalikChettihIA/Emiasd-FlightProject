package com.flightdelay.features.leakage

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.flightdelay.utils.DebugUtils._

/**
 * Classe centralisée pour la protection contre le data leakage
 *
 * Cette classe gère deux types de nettoyage :
 * 1. Suppression des colonnes sources de leakage (preprocessing)
 * 2. Suppression des labels inutilisés (feature extraction)
 *
 * Data leakage = utiliser des informations qui ne seraient pas disponibles au moment
 * de la prédiction en production (informations connues APRÈS le vol).
 */
object DataLeakageProtection {

  /**
   * Nettoie complètement un DataFrame contre le data leakage
   * Combine removeSourceLeakage() et removeUnusedLabels()
   *
   * @param df DataFrame à nettoyer
   * @param targetLabel Label cible à conserver
   * @param stage Stage du pipeline ("preprocessing" ou "feature_extraction")
   * @return DataFrame nettoyé
   */
  def clean(df: DataFrame, target: String)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    info(s"[DataLeakageProtection] Cleaning")

    val step1 = removeSourceLeakage(df, verbose = true)
    val step2 = removeUnusedLabels(step1, target, verbose = true)
    step2

  }

  /**
   * Supprime les colonnes sources de data leakage
   * (ARR_DELAY_NEW, WEATHER_DELAY, NAS_DELAY)
   *
   * Utilisé lors du preprocessing, AVANT la génération des labels
   *
   * @param df DataFrame avec potentiellement des colonnes de leakage
   * @param verbose Afficher les détails du nettoyage
   * @return DataFrame sans colonnes de leakage
   */
  def removeSourceLeakage(df: DataFrame, verbose: Boolean = false)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    if (verbose) {
      info("=" * 80)
      info("[LeakageProtection] Removing Source Leakage Columns")
      info("=" * 80)
    }

    val originalColumns = df.columns.length
    val columnsToRemove = LeakageConfig.SOURCE_LEAKAGE_COLUMNS.filter(df.columns.contains)

    if (columnsToRemove.isEmpty) {
      if (verbose) info("   No source leakage columns found - Data is clean!")
      df
    } else {
      if (verbose) {
        info(s"  Found ${columnsToRemove.length} source leakage columns:")
        columnsToRemove.foreach(col => info(s"     $col (will be removed)"))
      }

      val cleanedDf = df.drop(columnsToRemove: _*)

      if (verbose) {
        val finalColumns = cleanedDf.columns.length
        info(s"  - Removed: ${columnsToRemove.length} columns")
        info(s"  - Remaining: $finalColumns columns")
        info("   Source leakage columns removed")
      }

      cleanedDf
    }
  }

  /**
   * Supprime les labels inutilisés (garde uniquement le label cible)
   *
   * Utilisé lors de l'extraction de features, AVANT l'entraînement
   *
   * @param df DataFrame avec plusieurs labels
   * @param targetLabel Nom du label à conserver (ex: "label_is_delayed_15min")
   * @param verbose Afficher les détails du nettoyage
   * @return DataFrame avec uniquement le label cible
   */
  def removeUnusedLabels(df: DataFrame, targetLabel: String, verbose: Boolean = false)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    if (verbose) {
      info("=" * 80)
      info("[LeakageProtection] Removing Unused Labels")
      info("=" * 80)
      info(s"  Target label: $targetLabel")
    }

    val allLabels = df.columns.filter(_.startsWith(LeakageConfig.LABEL_PREFIX))
    val unusedLabels = allLabels.filter(_ != targetLabel)

    if (unusedLabels.isEmpty) {
      if (verbose) info("   No unused labels found")
      df
    } else {
      if (verbose) {
        info(s"  Found ${allLabels.length} label columns:")
        info(s"     $targetLabel (kept)")
        unusedLabels.foreach(label => info(s"    ✗ $label (removed)"))
      }

      val cleanedDf = df.drop(unusedLabels: _*)

      if (verbose) {
        info(s"  - Removed: ${unusedLabels.length} unused label columns")
        info(s"   Only target label kept: $targetLabel")
      }

      cleanedDf
    }
  }



  /**
   * Valide qu'un DataFrame est exempt de data leakage
   *
   * @param df DataFrame à valider
   * @param targetLabel Label cible attendu
   * @return Tuple (isValid, violations)
   */
  def validate(df: DataFrame, targetLabel: String)(implicit spark: SparkSession, configuration: AppConfiguration): (Boolean, Seq[String]) = {
    val violations = scala.collection.mutable.ArrayBuffer[String]()

    // Check for source leakage columns
    val sourceLeakage = LeakageConfig.SOURCE_LEAKAGE_COLUMNS.filter(df.columns.contains)
    if (sourceLeakage.nonEmpty) {
      violations ++= sourceLeakage.map(col => s"Source leakage column found: $col")
    }

    // Check for unused labels
    val allLabels = df.columns.filter(_.startsWith(LeakageConfig.LABEL_PREFIX))
    val unusedLabels = allLabels.filter(_ != targetLabel)
    if (unusedLabels.nonEmpty) {
      violations ++= unusedLabels.map(col => s"Unused label found: $col")
    }

    // Check for forbidden features
    val forbiddenFeatures = LeakageConfig.FORBIDDEN_FEATURE_COLUMNS.filter(df.columns.contains)
    if (forbiddenFeatures.nonEmpty) {
      violations ++= forbiddenFeatures.map(col => s"Forbidden feature found: $col")
    }

    (violations.isEmpty, violations.toSeq)
  }



}
