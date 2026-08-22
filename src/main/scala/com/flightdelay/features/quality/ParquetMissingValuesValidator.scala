package com.flightdelay.features.quality

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Utilitaire pour valider la présence et la cohérence des valeurs manquantes
 * dans les fichiers parquet générés par le pipeline
 */
object ParquetMissingValuesValidator {

  /**
   * Valide et affiche un rapport détaillé sur la gestion des valeurs manquantes
   *
   * @param parquetPath Chemin vers le fichier parquet à valider
   * @param datasetName Nom du dataset (ex: "TRAIN", "TEST")
   * @param spark Session Spark
   */
  def validateParquet(
    parquetPath: String,
    datasetName: String = "DATASET"
  )(implicit spark: SparkSession): Unit = {

    println("=" * 100)
    println(s"[ParquetMissingValuesValidator] Validation du dataset: $datasetName")
    println(s"[ParquetMissingValuesValidator] Path: $parquetPath")
    println("=" * 100)

    // Charger le parquet
    val df = spark.read.parquet(parquetPath)
    val totalRows = df.count()

    println(s"\n📊 Dataset Overview:")
    println(f"  - Total rows: $totalRows%,d")
    println(f"  - Total columns: ${df.columns.length}%,d")

    // 1. Vérifier les colonnes de flags par heure
    validateHourlyFlags(df, totalRows)

    // 2. Vérifier les features agrégées et leurs compteurs
    validateAggregatedFeatures(df, totalRows)

    // 3. Vérifier les sentinelles
    validateSentinelValues(df, totalRows)

    // 4. Afficher un échantillon de données
    displaySampleData(df)

    // 5. Vérifier qu'il n'y a plus de NULL
    validateNoNulls(df, totalRows)

    println("=" * 100)
    println(s"[ParquetMissingValuesValidator] Validation terminée pour $datasetName")
    println("=" * 100)
  }

  /**
   * Valide les flags binaires par heure (_missing_h1, _missing_h2, ...)
   */
  private def validateHourlyFlags(df: DataFrame, totalRows: Long)(implicit spark: SparkSession): Unit = {
    println("\n" + "=" * 100)
    println("1️⃣  VALIDATION DES FLAGS PAR HEURE (_missing_h1 à _missing_h7)")
    println("=" * 100)

    val hourlyFlagCols = df.columns.filter(_.matches(".+_missing_h\\d+"))

    if (hourlyFlagCols.isEmpty) {
      println("  ⚠️  Aucune colonne de flag par heure trouvée")
      return
    }

    println(f"  ✓ Trouvé ${hourlyFlagCols.length}%3d colonnes de flags par heure")

    // Grouper par origin/destination
    val originFlags = hourlyFlagCols.filter(_.startsWith("origin_"))
    val destFlags = hourlyFlagCols.filter(_.startsWith("destination_"))

    println(f"\n  Répartition:")
    println(f"    - Origin flags:      ${originFlags.length}%3d colonnes")
    println(f"    - Destination flags: ${destFlags.length}%3d colonnes")

    // Statistiques sur les flags
    println(f"\n  Statistiques des données manquantes par heure:")
    println("  " + "-" * 96)
    println(f"  ${"Colonne"}%-60s | ${"Nb Missing"}%10s | ${"% Missing"}%10s")
    println("  " + "-" * 96)

    hourlyFlagCols.sorted.take(10).foreach { colName =>
      val missingCount = df.filter(col(colName) === 1).count()
      val missingPct = (missingCount.toDouble / totalRows * 100)
      println(f"  $colName%-60s | $missingCount%,10d | $missingPct%9.2f%%")
    }

    if (hourlyFlagCols.length > 10) {
      println(f"  ... et ${hourlyFlagCols.length - 10} autres colonnes")
    }
    println("  " + "-" * 96)
  }

  /**
   * Valide les features agrégées et leurs compteurs de missing
   */
  private def validateAggregatedFeatures(df: DataFrame, totalRows: Long)(implicit spark: SparkSession): Unit = {
    println("\n" + "=" * 100)
    println("2️⃣  VALIDATION DES FEATURES AGRÉGÉES (Sum, Avg, Max, Min)")
    println("=" * 100)

    val aggMethods = Seq("Sum", "Avg", "Max", "Min", "Std")
    val aggFeatureCols = df.columns.filter { colName =>
      aggMethods.exists(method => colName.contains(s"_$method"))
    }.filterNot(_.contains("_missing_count"))

    if (aggFeatureCols.isEmpty) {
      println("  ⚠️  Aucune feature agrégée trouvée")
      return
    }

    println(f"  ✓ Trouvé ${aggFeatureCols.length}%3d features agrégées")

    // Vérifier que chaque feature agrégée a son compteur
    println(f"\n  Vérification de la présence des compteurs (_missing_count):")
    println("  " + "-" * 96)
    println(f"  ${"Feature Agrégée"}%-60s | ${"Compteur"}%10s")
    println("  " + "-" * 96)

    aggFeatureCols.sorted.foreach { aggCol =>
      val expectedCounterCol = s"${aggCol}_missing_count"
      val hasCounter = df.columns.contains(expectedCounterCol)
      val status = if (hasCounter) "✓ Présent" else "✗ MANQUANT"

      println(f"  $aggCol%-60s | $status%10s")
    }
    println("  " + "-" * 96)

    // Statistiques sur les compteurs
    val counterCols = df.columns.filter(_.endsWith("_missing_count"))

    if (counterCols.nonEmpty) {
      println(f"\n  Statistiques des compteurs de missing:")
      println("  " + "-" * 96)
      println(f"  ${"Compteur"}%-60s | ${"Avg Count"}%10s | ${"Max Count"}%10s")
      println("  " + "-" * 96)

      counterCols.sorted.take(10).foreach { counterCol =>
        val stats = df.agg(
          avg(col(counterCol)).as("avg_count"),
          max(col(counterCol)).as("max_count")
        ).first()

        val avgCount = stats.getAs[Double]("avg_count")
        val maxCount = stats.getAs[Long]("max_count")

        println(f"  $counterCol%-60s | $avgCount%10.2f | $maxCount%10d")
      }

      if (counterCols.length > 10) {
        println(f"  ... et ${counterCols.length - 10} autres compteurs")
      }
      println("  " + "-" * 96)
    }
  }

  /**
   * Valide la présence de valeurs sentinelles (-999, "MISSING", etc.)
   */
  private def validateSentinelValues(df: DataFrame, totalRows: Long)(implicit spark: SparkSession): Unit = {
    println("\n" + "=" * 100)
    println("3️⃣  VALIDATION DES VALEURS SENTINELLES")
    println("=" * 100)

    // Colonnes numériques (double/int)
    val numericCols = df.schema.fields
      .filter(f => f.dataType.typeName == "double" || f.dataType.typeName == "integer" || f.dataType.typeName == "long")
      .map(_.name)
      .filterNot(_.contains("_missing")) // Exclure les flags et compteurs

    // Colonnes string
    val stringCols = df.schema.fields
      .filter(_.dataType.typeName == "string")
      .map(_.name)

    // Vérifier -999 dans les colonnes numériques
    if (numericCols.nonEmpty) {
      println(f"\n  Colonnes avec sentinelles numériques (-999 ou -999.0):")
      println("  " + "-" * 96)
      println(f"  ${"Colonne"}%-60s | ${"Nb -999"}%10s | ${"% -999"}%10s")
      println("  " + "-" * 96)

      val colsWithSentinels = numericCols.flatMap { colName =>
        val sentinelCount = df.filter(
          col(colName) === -999.0 || col(colName) === -999
        ).count()

        if (sentinelCount > 0) {
          val sentinelPct = (sentinelCount.toDouble / totalRows * 100)
          Some((colName, sentinelCount, sentinelPct))
        } else {
          None
        }
      }.sortBy(-_._2) // Trier par nombre décroissant

      colsWithSentinels.take(15).foreach { case (colName, count, pct) =>
        println(f"  $colName%-60s | $count%,10d | $pct%9.2f%%")
      }

      if (colsWithSentinels.length > 15) {
        println(f"  ... et ${colsWithSentinels.length - 15} autres colonnes avec sentinelles")
      }

      if (colsWithSentinels.isEmpty) {
        println("  ℹ️  Aucune sentinelle -999 détectée (normal si pas de données manquantes)")
      }

      println("  " + "-" * 96)
    }

    // Vérifier "MISSING" dans les colonnes string
    if (stringCols.nonEmpty) {
      println("\n  Colonnes avec sentinelles string (MISSING):")
      println("  " + "-" * 96)
      println(f"  ${"Colonne"}%-60s | ${"Nb MISSING"}%10s | ${"% MISSING"}%10s")
      println("  " + "-" * 96)

      val colsWithMissing = stringCols.flatMap { colName =>
        val missingCount = df.filter(col(colName) === "MISSING").count()

        if (missingCount > 0) {
          val missingPct = (missingCount.toDouble / totalRows * 100)
          Some((colName, missingCount, missingPct))
        } else {
          None
        }
      }.sortBy(-_._2)

      colsWithMissing.take(10).foreach { case (colName, count, pct) =>
        println(f"  $colName%-60s | $count%,10d | $pct%9.2f%%")
      }

      if (colsWithMissing.length > 10) {
        println(f"  ... et ${colsWithMissing.length - 10} autres colonnes avec MISSING")
      }

      if (colsWithMissing.isEmpty) {
        println("  ℹ️  Aucune sentinelle MISSING détectée (normal si pas de données manquantes)")
      }

      println("  " + "-" * 96)
    }
  }

  /**
   * Affiche un échantillon de données pour validation manuelle
   */
  private def displaySampleData(df: DataFrame)(implicit spark: SparkSession): Unit = {
    println("\n" + "=" * 100)
    println("4️⃣  ÉCHANTILLON DE DONNÉES (5 premières lignes)")
    println("=" * 100)

    // Sélectionner quelques colonnes intéressantes
    val sampleCols = Seq(
      // Features agrégées
      df.columns.find(_.contains("HourlyPrecip_Sum")),
      df.columns.find(_.contains("HourlyPrecip_Sum_missing_count")),
      df.columns.find(_.contains("press_change_abs_Max")),
      df.columns.find(_.contains("press_change_abs_Max_missing_count")),
      // Flags par heure
      df.columns.find(_ == "origin_weather_missing_h7"),
      df.columns.find(_ == "destination_weather_missing_h7")
    ).flatten

    if (sampleCols.nonEmpty) {
      println("\n  Colonnes sélectionnées pour l'échantillon:")
      sampleCols.foreach(c => println(s"    - $c"))
      println("")

      df.select(sampleCols.map(col): _*).show(5, truncate = false)
    } else {
      println("  ⚠️  Aucune colonne d'exemple trouvée")
    }
  }

  /**
   * Vérifie qu'il n'y a plus de valeurs NULL après traitement
   */
  private def validateNoNulls(df: DataFrame, totalRows: Long)(implicit spark: SparkSession): Unit = {
    println("\n" + "=" * 100)
    println("5️⃣  VÉRIFICATION DE L'ABSENCE DE NULL (toutes colonnes)")
    println("=" * 100)

    val columnsWithNulls = df.columns.flatMap { colName =>
      val nullCount = df.filter(col(colName).isNull).count()
      if (nullCount > 0) {
        val nullPct = (nullCount.toDouble / totalRows * 100)
        Some((colName, nullCount, nullPct))
      } else {
        None
      }
    }.sortBy(-_._2)

    if (columnsWithNulls.isEmpty) {
      println("  ✅ SUCCÈS: Aucune valeur NULL détectée dans aucune colonne !")
      println("  ✅ Tous les NULL ont été remplacés par des sentinelles")
    } else {
      println("  ⚠️  ATTENTION: Des valeurs NULL ont été détectées:")
      println("  " + "-" * 96)
      println(f"  ${"Colonne"}%-60s | ${"Nb NULL"}%10s | ${"% NULL"}%10s")
      println("  " + "-" * 96)

      columnsWithNulls.foreach { case (colName, count, pct) =>
        println(f"  $colName%-60s | $count%,10d | $pct%9.2f%%")
      }
      println("  " + "-" * 96)

      println("\n  ⚠️  Il reste des NULL - vérifier le pipeline de traitement")
    }
  }

  /**
   * Génère un rapport complet au format texte
   */
  def generateReport(
    trainPath: String,
    testPath: String
  )(implicit spark: SparkSession): Unit = {

    println("\n\n")
    println("╔" + "=" * 98 + "╗")
    println("║" + " " * 25 + "RAPPORT DE VALIDATION DES VALEURS MANQUANTES" + " " * 29 + "║")
    println("╚" + "=" * 98 + "╝")
    println("\n")

    // Valider TRAIN
    validateParquet(trainPath, "TRAIN")

    println("\n\n")

    // Valider TEST
    validateParquet(testPath, "TEST")

    println("\n\n")
    println("╔" + "=" * 98 + "╗")
    println("║" + " " * 40 + "FIN DU RAPPORT" + " " * 44 + "║")
    println("╚" + "=" * 98 + "╝")
  }
}
