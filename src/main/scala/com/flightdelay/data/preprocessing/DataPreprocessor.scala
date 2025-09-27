package com.flightdelay.data.preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger

/**
 * Classe abstraite définissant l'interface commune pour le preprocessing des données.
 *
 */
abstract class DataPreprocessor {

  protected val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Méthode principale de preprocessing
   * @param spark Session Spark
   * @param rawData DataFrame contenant les données brutes
   * @return DataFrame préprocessé
   */
  def preprocess(rawData: DataFrame)(implicit spark: SparkSession): DataFrame

  /**
   * Supprime les lignes dupliquées basées sur des colonnes spécifiques
   * @param df DataFrame d'entrée
   * @param columns Colonnes à considérer pour les doublons (optionnel)
   * @return DataFrame sans doublons
   */
  protected def removeDuplicates(df: DataFrame, columns: Seq[String] = Seq.empty): DataFrame = {
    logger.info(s"Suppression des doublons. Nombre de lignes avant: ${df.count()}")

    val result = if (columns.nonEmpty) {
      df.dropDuplicates(columns)
    } else {
      df.dropDuplicates()
    }

    logger.info(s"Nombre de lignes après suppression des doublons: ${result.count()}")
    result
  }

  /**
   * Supprime les lignes avec des valeurs nulles dans certaines colonnes
   * @param df DataFrame d'entrée
   * @param columns Colonnes à vérifier pour les valeurs nulles
   * @return DataFrame filtré
   */
  protected def removeNullValues(df: DataFrame, columns: Seq[String]): DataFrame = {
    logger.info(s"Suppression des valeurs nulles pour les colonnes: ${columns.mkString(", ")}")
    logger.info(s"Nombre de lignes avant: ${df.count()}")

    val result = columns.foldLeft(df) { (acc, columnName) =>
      acc.filter(col(columnName).isNotNull)
    }

    logger.info(s"Nombre de lignes après suppression des nulls: ${result.count()}")
    result
  }

  /**
   * Supprime les lignes où certaines colonnes ont des valeurs spécifiques à exclure
   * @param df DataFrame d'entrée
   * @param exclusions Map[colonne -> valeurs à exclure]
   * @return DataFrame filtré
   */
  protected def removeSpecificValues(df: DataFrame, exclusions: Map[String, Seq[Any]]): DataFrame = {
    logger.info(s"Suppression des valeurs spécifiques: $exclusions")
    logger.info(s"Nombre de lignes avant: ${df.count()}")

    val result = exclusions.foldLeft(df) { case (acc, (colName, values)) =>
      acc.filter(!col(colName).isin(values: _*))
    }

    logger.info(s"Nombre de lignes après suppression des valeurs spécifiques: ${result.count()}")
    result
  }

  /**
   * Convertit les types de données selon un mapping spécifié
   * @param df DataFrame d'entrée
   * @param typeMapping Map[colonne -> type de données]
   * @return DataFrame avec les types convertis
   */
  protected def convertDataTypes(df: DataFrame, typeMapping: Map[String, DataType]): DataFrame = {
    logger.info(s"Conversion des types de données: ${typeMapping.keys.mkString(", ")}")

    typeMapping.foldLeft(df) { case (acc, (colName, dataType)) =>
      if (acc.columns.contains(colName)) {
        acc.withColumn(colName, col(colName).cast(dataType))
      } else {
        logger.warn(s"Colonne '$colName' introuvable pour conversion de type")
        acc
      }
    }
  }

  /**
   * Ajoute des colonnes calculées basées sur des expressions
   * @param df DataFrame d'entrée
   * @param columnExpressions Map[nom_nouvelle_colonne -> expression]
   * @return DataFrame avec les nouvelles colonnes
   */
  protected def addCalculatedColumns(df: DataFrame, columnExpressions: Map[String, Column]): DataFrame = {
    logger.info(s"Ajout de colonnes calculées: ${columnExpressions.keys.mkString(", ")}")

    columnExpressions.foldLeft(df) { case (acc, (colName, expression)) =>
      acc.withColumn(colName, expression)
    }
  }

  /**
   * Normalise les colonnes numériques (z-score normalization)
   * @param df DataFrame d'entrée
   * @param columns Colonnes à normaliser
   * @return DataFrame avec colonnes normalisées
   */
  protected def normalizeColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
    logger.info(s"Normalisation des colonnes: ${columns.mkString(", ")}")

    columns.foldLeft(df) { (acc, colName) =>
      if (acc.columns.contains(colName)) {
        // Calculer moyenne et écart-type
        val stats = acc.select(mean(col(colName)).as("mean"), stddev(col(colName)).as("stddev")).collect()(0)
        val meanVal = stats.getAs[Double]("mean")
        val stddevVal = stats.getAs[Double]("stddev")

        if (stddevVal > 0) {
          acc.withColumn(s"${colName}_normalized", (col(colName) - meanVal) / stddevVal)
        } else {
          logger.warn(s"Écart-type nul pour la colonne '$colName', normalisation ignorée")
          acc
        }
      } else {
        logger.warn(s"Colonne '$colName' introuvable pour normalisation")
        acc
      }
    }
  }

  /**
   * Filtre les valeurs aberrantes basées sur l'IQR (Interquartile Range)
   * @param df DataFrame d'entrée
   * @param columns Colonnes à traiter pour les outliers
   * @param factor Facteur multiplicatif pour l'IQR (défaut: 1.5)
   * @return DataFrame sans outliers
   */
  protected def removeOutliers(df: DataFrame, columns: Seq[String], factor: Double = 1.5): DataFrame = {
    logger.info(s"Suppression des outliers pour les colonnes: ${columns.mkString(", ")}")
    logger.info(s"Nombre de lignes avant: ${df.count()}")

    val result = columns.foldLeft(df) { (acc, colName) =>
      if (acc.columns.contains(colName)) {
        val quantiles = acc.stat.approxQuantile(colName, Array(0.25, 0.75), 0.01)
        val q1 = quantiles(0)
        val q3 = quantiles(1)
        val iqr = q3 - q1
        val lowerBound = q1 - factor * iqr
        val upperBound = q3 + factor * iqr

        acc.filter(col(colName) >= lowerBound && col(colName) <= upperBound)
      } else {
        logger.warn(s"Colonne '$colName' introuvable pour suppression des outliers")
        acc
      }
    }

    logger.info(s"Nombre de lignes après suppression des outliers: ${result.count()}")
    result
  }

  /**
   * Valide la qualité des données après preprocessing
   * @param df DataFrame à valider
   * @param requiredColumns Colonnes requises
   * @return Boolean indiquant si les données sont valides
   */
  protected def validateDataQuality(df: DataFrame, requiredColumns: Seq[String]): Boolean = {
    logger.info("Validation de la qualité des données...")

    // Vérifier la présence des colonnes requises
    val missingColumns = requiredColumns.filterNot(df.columns.contains)
    if (missingColumns.nonEmpty) {
      logger.error(s"Colonnes manquantes: ${missingColumns.mkString(", ")}")
      return false
    }

    // Vérifier que le DataFrame n'est pas vide
    val rowCount = df.count()
    if (rowCount == 0) {
      logger.error("Le DataFrame est vide après preprocessing")
      return false
    }

    logger.info(s"Validation réussie. Nombre de lignes: $rowCount, Nombre de colonnes: ${df.columns.length}")
    true
  }

  /**
   * Affiche un résumé statistique du preprocessing
   * @param originalDf DataFrame original
   * @param processedDf DataFrame après preprocessing
   */
  protected def logPreprocessingSummary(originalDf: DataFrame, processedDf: DataFrame): Unit = {
    val originalCount = originalDf.count()
    val processedCount = processedDf.count()
    val reductionPercent = ((originalCount - processedCount).toDouble / originalCount * 100).round

    logger.info("=== RÉSUMÉ DU PREPROCESSING ===")
    logger.info(s"Lignes originales: $originalCount")
    logger.info(s"Lignes après preprocessing: $processedCount")
    logger.info(s"Réduction: $reductionPercent%")
    logger.info(s"Colonnes originales: ${originalDf.columns.length}")
    logger.info(s"Colonnes après preprocessing: ${processedDf.columns.length}")
  }
}
