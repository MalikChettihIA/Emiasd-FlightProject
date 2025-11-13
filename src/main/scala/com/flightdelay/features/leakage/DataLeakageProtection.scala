package com.flightdelay.features.leakage

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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
  def clean(df: DataFrame, target: String): DataFrame = {
    println(s"[DataLeakageProtection] Cleaning")

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
  def removeSourceLeakage(df: DataFrame, verbose: Boolean = false): DataFrame = {
    if (verbose) {
      println("=" * 80)
      println("[LeakageProtection] Removing Source Leakage Columns")
      println("=" * 80)
    }

    val originalColumns = df.columns.length
    val columnsToRemove = LeakageConfig.SOURCE_LEAKAGE_COLUMNS.filter(df.columns.contains)

    if (columnsToRemove.isEmpty) {
      if (verbose) println("  ✓ No source leakage columns found - Data is clean!")
      df
    } else {
      if (verbose) {
        println(s"  Found ${columnsToRemove.length} source leakage columns:")
        columnsToRemove.foreach(col => println(s"    ✗ $col (will be removed)"))
      }

      val cleanedDf = df.drop(columnsToRemove: _*)

      if (verbose) {
        val finalColumns = cleanedDf.columns.length
        println(s"  - Removed: ${columnsToRemove.length} columns")
        println(s"  - Remaining: $finalColumns columns")
        println("   Source leakage columns removed")
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
  def removeUnusedLabels(df: DataFrame, targetLabel: String, verbose: Boolean = false): DataFrame = {
    if (verbose) {
      println("=" * 80)
      println("[LeakageProtection] Removing Unused Labels")
      println("=" * 80)
      println(s"  Target label: $targetLabel")
    }

    val allLabels = df.columns.filter(_.startsWith(LeakageConfig.LABEL_PREFIX))
    val unusedLabels = allLabels.filter(_ != targetLabel)

    if (unusedLabels.isEmpty) {
      if (verbose) println("  ✓ No unused labels found")
      df
    } else {
      if (verbose) {
        println(s"  Found ${allLabels.length} label columns:")
        println(s"     $targetLabel (kept)")
        unusedLabels.foreach(label => println(s"    ✗ $label (removed)"))
      }

      val cleanedDf = df.drop(unusedLabels: _*)

      if (verbose) {
        println(s"  - Removed: ${unusedLabels.length} unused label columns")
        println(s"  ✓ Only target label kept: $targetLabel")
      }

      cleanedDf
    }
  }

  /**
   * Supprime les colonnes interdites comme features
   * (identifiants, timestamps, etc.)
   *
   * @param df DataFrame avec potentiellement des colonnes interdites
   * @param verbose Afficher les détails du nettoyage
   * @return DataFrame sans colonnes interdites
   */
  def removeForbiddenFeatures(df: DataFrame, verbose: Boolean = false): DataFrame = {
    if (verbose) {
      println("=" * 80)
      println("[LeakageProtection] Removing Forbidden Feature Columns")
      println("=" * 80)
    }

    val columnsToRemove = LeakageConfig.FORBIDDEN_FEATURE_COLUMNS.filter(df.columns.contains)

    if (columnsToRemove.isEmpty) {
      if (verbose) println("  ✓ No forbidden feature columns found")
      df
    } else {
      if (verbose) {
        println(s"  Found ${columnsToRemove.length} forbidden feature columns:")
        columnsToRemove.foreach(col => println(s"    ✗ $col (removed)"))
      }

      val cleanedDf = df.drop(columnsToRemove: _*)

      if (verbose) {
        println(s"  - Removed: ${columnsToRemove.length} forbidden columns")
        println("   Forbidden features removed")
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
  def validate(df: DataFrame, targetLabel: String): (Boolean, Seq[String]) = {
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

  /**
   * Affiche un rapport de validation complet
   *
   * @param df DataFrame à analyser
   * @param targetLabel Label cible
   */
  def printValidationReport(df: DataFrame, targetLabel: String): Unit = {
    println("=" * 80)
    println("[DataLeakageProtection] Validation Report")
    println("=" * 80)

    val (isValid, violations) = validate(df, targetLabel)

    if (isValid) {
      println("  ✓ VALIDATION PASSED - No data leakage detected")
      println(s"  ✓ Target label: $targetLabel")
      println(s"  ✓ Total columns: ${df.columns.length}")
    } else {
      println("  ✗ VALIDATION FAILED - Data leakage detected!")
      println(s"  Found ${violations.length} violations:")
      violations.foreach(v => println(s"    ✗ $v"))
    }

    println("=" * 80)
  }

}
