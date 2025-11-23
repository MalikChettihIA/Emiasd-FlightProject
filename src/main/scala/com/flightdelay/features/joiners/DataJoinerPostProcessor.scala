package com.flightdelay.features.joiners

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.flightdelay.utils.DebugUtils._

/**
 * Post-processing des données jointes entre vols et météo
 *
 * Effectue des transformations et nettoyages supplémentaires sur les données
 * après la jointure spatio-temporelle.
 */
object DataJoinerPostProcessor {

  /**
   * Exécute le post-processing sur le DataFrame joint
   *
   * @param df DataFrame résultant de la jointure vols-météo
   * @param experiment Configuration de l'expérimentation
   * @param spark Session Spark implicite
   * @param configuration Configuration de l'application
   * @return DataFrame post-traité
   */
  def execute(df: DataFrame, experiment: ExperimentConfig)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.features.joiners.DataJoinerPostProcessor.execute()")

    // Nettoyage des lignes sans données météo
    val cleanedDF = clean(df, experiment)

    // Création des features d'accumulation
    val withAccumulationFeatures = createAccumulationFeatures(cleanedDF, experiment)

    withAccumulationFeatures
  }

  /**
   * Supprime les lignes pour lesquelles toutes les colonnes weather_hour sont null
   *
   * @param df DataFrame à nettoyer
   * @param experiment Configuration de l'expérimentation
   * @return DataFrame nettoyé
   */
  private def clean(df: DataFrame, experiment: ExperimentConfig)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.features.joiners.DataJoinerPostProcessor.clean()")

    val weatherOriginDepth = experiment.featureExtraction.weatherOriginDepthHours
    val weatherDestDepth = experiment.featureExtraction.weatherDestinationDepthHours

    // Construire les noms de colonnes à vérifier
    var columnsToCheck = Seq.empty[String]

    // Ajouter les colonnes origin si profondeur >= 0
    if (weatherOriginDepth >= 0) {
      val originCols = (1 to weatherOriginDepth).map(h => s"origin_weather_hour_h$h")
      columnsToCheck ++= originCols
      debug(s"  - Origin weather columns to check: ${originCols.mkString(", ")}")
    } else {
      debug("  - No origin weather columns (weatherOriginDepthHours < 0)")
    }

    // Ajouter les colonnes destination si profondeur >= 0
    if (weatherDestDepth >= 0) {
      val destCols = (1 to weatherDestDepth).map(h => s"destination_weather_hour_h$h")
      columnsToCheck ++= destCols
      debug(s"  - Destination weather columns to check: ${destCols.mkString(", ")}")
    } else {
      debug("  - No destination weather columns (weatherDestinationDepthHours < 0)")
    }

    // Si aucune colonne météo, retourner le DataFrame tel quel
    if (columnsToCheck.isEmpty) {
      info("  - No weather columns to check, returning DataFrame as-is")
      return df
    }

    // Filtrer : garder les lignes où AU MOINS UNE colonne weather_hour est non-null
    val condition = columnsToCheck
      .map(colName => col(colName).isNotNull)
      .reduce(_ || _)
    val cleanedDF = df.filter(condition)

    whenDebug {
      val countBefore = df.count()
      val countAfter = cleanedDF.count()
      val removedCount = countBefore - countAfter

      debug(s"  - Rows before cleaning: $countBefore")
      debug(s"  - Rows after cleaning: $countAfter")
      debug(s"  - Removed rows (all weather_hour null): $removedCount")

    }


    cleanedDF
  }

  /**
   * Crée des features d'accumulation en sommant les observations météo sur toutes les heures
   *
   * @param df DataFrame à enrichir
   * @param experiment Configuration de l'expérimentation
   * @return DataFrame avec les features d'accumulation
   */
  private def createAccumulationFeatures(df: DataFrame, experiment: ExperimentConfig)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("- Calling com.flightdelay.features.joiners.DataJoinerPostProcessor.createAccumulationFeatures()")

    val weatherOriginDepth = experiment.featureExtraction.weatherOriginDepthHours
    val weatherDestDepth = experiment.featureExtraction.weatherDestinationDepthHours

    // Récupérer les features à agréger depuis la configuration
    val aggregatedFeatures = experiment.featureExtraction.aggregatedSelectedFeatures.getOrElse(Map.empty)

    if (aggregatedFeatures.isEmpty) {
      info("  - No aggregated features configured, skipping accumulation")
      return df
    }

    var resultDF = df
    var createdFeaturesCount = 0

    // Pour chaque variable météo configurée
    aggregatedFeatures.foreach { case (varName, aggConfig) =>

      val aggMethod = aggConfig.aggregation.toLowerCase

      // Accumulation pour ORIGIN
      if (weatherOriginDepth > 0) {
        val originCols = (1 to weatherOriginDepth)
          .map(h => s"origin_weather_${varName}_h$h")
          .filter(colName => df.columns.contains(colName))

        if (originCols.nonEmpty) {
          val aggExpr = createAggregationExpression(originCols, aggMethod)
          resultDF = resultDF.withColumn(s"origin_weather_${varName}_${aggMethod.capitalize}", aggExpr)
          debug(s"  - Created origin_weather_${varName}_${aggMethod.capitalize} from ${originCols.length} columns")
          createdFeaturesCount += 1
        }
      }

      // Accumulation pour DESTINATION
      if (weatherDestDepth > 0) {
        val destCols = (1 to weatherDestDepth)
          .map(h => s"destination_weather_${varName}_h$h")
          .filter(colName => df.columns.contains(colName))

        if (destCols.nonEmpty) {
          val aggExpr = createAggregationExpression(destCols, aggMethod)
          resultDF = resultDF.withColumn(s"destination_weather_${varName}_${aggMethod.capitalize}", aggExpr)
          debug(s"  - Created destination_weather_${varName}_${aggMethod.capitalize} from ${destCols.length} columns")
          createdFeaturesCount += 1
        }
      }
    }

    info(s"  - Created $createdFeaturesCount accumulation features")
    resultDF
  }

  /**
   * Crée une expression d'agrégation Spark selon la méthode spécifiée
   *
   * @param columns Liste des noms de colonnes à agréger
   * @param method Méthode d'agrégation: "sum", "avg", "max", "min", "std"
   * @return Expression Spark Column
   */
  private def createAggregationExpression(columns: Seq[String], method: String): org.apache.spark.sql.Column = {
    method match {
      case "sum" =>
        columns.map(colName => coalesce(col(colName), lit(0.0))).reduce(_ + _)

      case "avg" | "mean" =>
        val sum = columns.map(colName => coalesce(col(colName), lit(0.0))).reduce(_ + _)
        sum / lit(columns.length.toDouble)

      case "max" =>
        columns.map(col).reduce((c1, c2) => greatest(c1, c2))

      case "min" =>
        columns.map(col).reduce((c1, c2) => least(c1, c2))

      case "std" | "stddev" =>
        // Standard deviation calculation
        val n = lit(columns.length.toDouble)
        val values = columns.map(colName => coalesce(col(colName), lit(0.0)))
        val mean = values.reduce(_ + _) / n
        val squaredDiffs = values.map(v => pow(v - mean, 2.0))
        val variance = squaredDiffs.reduce(_ + _) / n
        sqrt(variance)

      case unknown =>
        throw new IllegalArgumentException(s"Unknown aggregation method: $unknown. Supported: sum, avg, max, min, std")
    }
  }
}
