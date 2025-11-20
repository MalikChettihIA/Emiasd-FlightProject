package com.flightdelay.ml.evaluation

import com.flightdelay.config.AppConfiguration
import com.flightdelay.utils.MetricsWriter
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.flightdelay.utils.MetricsUtils
import com.flightdelay.utils.DebugUtils._

/**
 * Model evaluator for flight delay prediction.
 * Computes comprehensive metrics for binary classification tasks.
 */
object ModelEvaluator {

  /**
   * Evaluation metrics for binary classification
   */
  case class EvaluationMetrics(
    accuracy: Double,
    precision: Double,
    recall: Double,
    f1Score: Double,
    areaUnderROC: Double,
    areaUnderPR: Double,
    truePositives: Long,
    trueNegatives: Long,
    falsePositives: Long,
    falseNegatives: Long
  ) {
    def specificity: Double = {
      if (trueNegatives + falsePositives == 0) 0.0
      else trueNegatives.toDouble / (trueNegatives + falsePositives)
    }

    def falsePositiveRate: Double = 1.0 - specificity

    // RECo: Recall for On-time flights (class 0)
    // Also known as specificity or True Negative Rate
    def recallOnTime: Double = specificity

    // RECd: Recall for Delayed flights (class 1)
    // This is the standard recall for the positive class
    def recallDelayed: Double = {
      if (truePositives + falseNegatives == 0) 0.0
      else truePositives.toDouble / (truePositives + falseNegatives)
    }
  }

  /**
   * Evaluate model predictions and return comprehensive metrics
   * @param predictions DataFrame with "label" and "prediction" columns
   * @param metricsOutputPath Optional path to save metrics to CSV
   * @return EvaluationMetrics object with all computed metrics
   */
  def evaluate(predictions: DataFrame, metricsOutputPath: Option[String] = None, datasetType: String)(implicit spark: SparkSession, configuration: AppConfiguration): EvaluationMetrics = {
    info("=" * 80)
    info(s"[STEP 4] ${datasetType} -- Model Evaluation")
    info("=" * 80)

    // Check if already cached to avoid double caching
    val cachedPredictions = predictions.cache()

    // Compute confusion matrix
    val confusionMatrix = cachedPredictions
      .groupBy("label", "prediction")
      .count()
      .collect()
      .map(row => ((row.getDouble(0), row.getDouble(1)), row.getLong(2)))
      .toMap

    val tp = confusionMatrix.getOrElse((1.0, 1.0), 0L)
    val tn = confusionMatrix.getOrElse((0.0, 0.0), 0L)
    val fp = confusionMatrix.getOrElse((0.0, 1.0), 0L)
    val fn = confusionMatrix.getOrElse((1.0, 0.0), 0L)

    // Multiclass metrics evaluator
    val multiclassEval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    whenDebug {
      debug("Cached Predictions Schema : ...")
      cachedPredictions.printSchema
    }

    val accuracy = multiclassEval.setMetricName("accuracy").evaluate(cachedPredictions)
    val precision = multiclassEval.setMetricName("weightedPrecision").evaluate(cachedPredictions)
    val recall = multiclassEval.setMetricName("weightedRecall").evaluate(cachedPredictions)
    val f1 = multiclassEval.setMetricName("f1").evaluate(cachedPredictions)

    // Binary classification metrics evaluator
    val binaryEval = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")

    val auc = binaryEval.setMetricName("areaUnderROC").evaluate(cachedPredictions)
    val aupr = binaryEval.setMetricName("areaUnderPR").evaluate(cachedPredictions)

    val metrics = EvaluationMetrics(
      accuracy = accuracy,
      precision = precision,
      recall = recall,
      f1Score = f1,
      areaUnderROC = auc,
      areaUnderPR = aupr,
      truePositives = tp,
      trueNegatives = tn,
      falsePositives = fp,
      falseNegatives = fn
    )

    // Display metrics
    displayMetrics(metrics)

    // Save metrics to file if path provided
    metricsOutputPath.foreach { basePath =>
      saveMetricsToFile(metrics, basePath)
    }

    // Only unpersist if we cached it ourselves (not if it was already cached)
    if (!predictions.storageLevel.useMemory) {
      cachedPredictions.unpersist()
    }

    metrics
  }

  /**
   * Display evaluation metrics in a formatted table
   */
  private def displayMetrics(metrics: EvaluationMetrics)(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    info("--- Classification Metrics ---")
    info(f"Accuracy:            ${metrics.accuracy * 100}%6.2f%%")
    info(f"Precision:           ${metrics.precision * 100}%6.2f%%")
    info(f"Recall (Weighted):   ${metrics.recall * 100}%6.2f%%")
    info(f"F1-Score:            ${metrics.f1Score * 100}%6.2f%%")
    info(f"AUC-ROC:             ${metrics.areaUnderROC}%6.4f")
    info(f"AUC-PR:              ${metrics.areaUnderPR}%6.4f")

    info("--- Per-Class Recall ---")
    info(f"RECd (Delayed):      ${metrics.recallDelayed * 100}%6.2f%%  [TP/(TP+FN)]")
    info(f"RECo (On-time):      ${metrics.recallOnTime * 100}%6.2f%%  [TN/(TN+FP)]")

    info("--- Confusion Matrix ---")
    info(f"True Positives:      ${metrics.truePositives}%,10d")
    info(f"True Negatives:      ${metrics.trueNegatives}%,10d")
    info(f"False Positives:     ${metrics.falsePositives}%,10d")
    info(f"False Negatives:     ${metrics.falseNegatives}%,10d")

    val total = metrics.truePositives + metrics.trueNegatives +
                metrics.falsePositives + metrics.falseNegatives
    info(f"Total Predictions:   ${total}%,10d")

    info("=" * 80)
  }

  /**
   * Evaluate and compare train/test performance
   * @param trainPredictions Predictions on training set
   * @param testPredictions Predictions on test set
   * @param metricsOutputPath Optional base path to save metrics
   * @return Tuple of (train metrics, test metrics)
   */
  def evaluateTrainTest(
    trainPredictions: DataFrame,
    testPredictions: DataFrame,
    metricsOutputPath: Option[String] = None
  )(implicit spark: SparkSession, configuration: AppConfiguration): (EvaluationMetrics, EvaluationMetrics) = {

    info("=" * 80)
    info("[STEP 4] Train/Test Evaluation")
    info("=" * 80)

    info("[Training Set Evaluation]")
    val trainMetrics = evaluate(predictions = trainPredictions, datasetType = s"[K-Fold CV] [Training Set Evaluation]")

    info("[Test Set Evaluation]")
    val testMetrics = evaluate(predictions = testPredictions, datasetType = s"[K-Fold CV] [Training Set Evaluation]")

    // Compute overfitting indicator
    val accuracyGap = trainMetrics.accuracy - testMetrics.accuracy
    val f1Gap = trainMetrics.f1Score - testMetrics.f1Score

    info("--- Overfitting Analysis ---")
    info(f"Accuracy Gap (Train - Test): ${accuracyGap * 100}%6.2f%%")
    info(f"F1-Score Gap (Train - Test): ${f1Gap * 100}%6.2f%%")

    if (accuracyGap > 0.10 || f1Gap > 0.10) {
      info(" WARNING: Significant overfitting detected (gap > 10%)")
    } else if (accuracyGap > 0.05 || f1Gap > 0.05) {
      info(" Moderate overfitting detected (gap > 5%)")
    } else {
      info("- Model generalizes well")
    }

    info("=" * 80)

    // Save train/test comparison if path provided
    metricsOutputPath.foreach { basePath =>
      saveTrainTestComparison(trainMetrics, testMetrics, basePath)
    }

    (trainMetrics, testMetrics)
  }

  /**
   * Save metrics to CSV files
   */
  private def saveMetricsToFile(metrics: EvaluationMetrics, basePath: String): Unit = {
    // Save main metrics
    val metricsMap = Map(
      "accuracy" -> metrics.accuracy,
      "precision" -> metrics.precision,
      "recall" -> metrics.recall,
      "f1_score" -> metrics.f1Score,
      "auc_roc" -> metrics.areaUnderROC,
      "auc_pr" -> metrics.areaUnderPR,
      "specificity" -> metrics.specificity,
      "false_positive_rate" -> metrics.falsePositiveRate,
      "recall_delayed" -> metrics.recallDelayed,
      "recall_ontime" -> metrics.recallOnTime
    )

    val headers = Seq("metric", "value")
    val rows = metricsMap.map { case (name, value) => Seq(name, f"$value%.6f") }.toSeq

    MetricsWriter.writeCsv(headers, rows, s"$basePath/metrics.csv")

    // Save confusion matrix
    MetricsWriter.writeConfusionMatrix(
      metrics.truePositives,
      metrics.trueNegatives,
      metrics.falsePositives,
      metrics.falseNegatives,
      s"$basePath/confusion_matrix.csv"
    )
  }

  /**
   * Save predictions with probabilities for ROC curve generation
   */
  private def savePredictionsForROC(predictions: DataFrame, basePath: String, split: String)(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    import predictions.sparkSession.implicits._

    // Cache predictions if not already cached to avoid multiple broadcasts
    val cachedPreds = if (predictions.storageLevel.useMemory) predictions else predictions.cache()

    // Extract label, prediction, and probability of positive class
    val rocData = cachedPreds.select("label", "prediction", "probability")
      .rdd
      .map { row =>
        val label = row.getDouble(0)
        val prediction = row.getDouble(1)
        val probability = row.getAs[org.apache.spark.ml.linalg.Vector](2)
        val probPositive = probability(1) // Probability of class 1 (delayed)
        (label, prediction, probPositive)
      }
      .toDF("label", "prediction", "prob_positive")

    // Sample data if too large (keep max 10000 points for ROC curve)
    val count = rocData.count()
    val sampledData = if (count > 10000) {
      rocData.sample(withReplacement = false, 10000.0 / count)
    } else {
      rocData
    }

    // Save to CSV
    sampledData.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$basePath/roc_data_${split}_temp")

    // Move the part file to final location
    try {
      val fs = org.apache.hadoop.fs.FileSystem.get(predictions.sparkSession.sparkContext.hadoopConfiguration)
      val srcPath = new org.apache.hadoop.fs.Path(s"$basePath/roc_data_${split}_temp")
      val files = fs.listStatus(srcPath).filter(_.getPath.getName.startsWith("part-"))
      if (files.nonEmpty) {
        val partFile = files.head.getPath
        val destPath = new org.apache.hadoop.fs.Path(s"$basePath/roc_data_${split}.csv")
        fs.rename(partFile, destPath)
        fs.delete(srcPath, true)
        info(s"  - ROC data saved to: $basePath/roc_data_${split}.csv")
      }
    } catch {
      case ex: Exception =>
        error(s"   Could not rename ROC data file: ${ex.getMessage}")
    }
  }

  /**
   * Save train/test comparison to CSV
   */
  private def saveTrainTestComparison(
    trainMetrics: EvaluationMetrics,
    testMetrics: EvaluationMetrics,
    basePath: String
  ): Unit = {
    val trainMap = Map(
      "accuracy" -> trainMetrics.accuracy,
      "precision" -> trainMetrics.precision,
      "recall" -> trainMetrics.recall,
      "f1_score" -> trainMetrics.f1Score,
      "auc_roc" -> trainMetrics.areaUnderROC,
      "auc_pr" -> trainMetrics.areaUnderPR,
      "recall_delayed" -> trainMetrics.recallDelayed,
      "recall_ontime" -> trainMetrics.recallOnTime
    )

    val testMap = Map(
      "accuracy" -> testMetrics.accuracy,
      "precision" -> testMetrics.precision,
      "recall" -> testMetrics.recall,
      "f1_score" -> testMetrics.f1Score,
      "auc_roc" -> testMetrics.areaUnderROC,
      "auc_pr" -> testMetrics.areaUnderPR,
      "recall_delayed" -> testMetrics.recallDelayed,
      "recall_ontime" -> testMetrics.recallOnTime
    )

    MetricsWriter.writeTrainTestMetrics(trainMap, testMap, s"$basePath/train_test_comparison.csv")

    // Save separate confusion matrices
    MetricsWriter.writeConfusionMatrix(
      trainMetrics.truePositives,
      trainMetrics.trueNegatives,
      trainMetrics.falsePositives,
      trainMetrics.falseNegatives,
      s"$basePath/confusion_matrix_train.csv"
    )

    MetricsWriter.writeConfusionMatrix(
      testMetrics.truePositives,
      testMetrics.trueNegatives,
      testMetrics.falsePositives,
      testMetrics.falseNegatives,
      s"$basePath/confusion_matrix_test.csv"
    )
  }

  /**
   * Save train/test predictions for ROC curve
   */
  def saveROCData(
    trainPredictions: DataFrame,
    testPredictions: DataFrame,
    basePath: String
  )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    info("Saving ROC curve data...")
    savePredictionsForROC(trainPredictions, basePath, "train")
    savePredictionsForROC(testPredictions, basePath, "test")
  }
}
