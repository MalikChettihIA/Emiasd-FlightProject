package com.flightdelay.ml

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import com.flightdelay.ml.evaluation.ModelEvaluator
import com.flightdelay.ml.evaluation.ModelEvaluator.EvaluationMetrics
import com.flightdelay.ml.tracking.MLFlowTracker
import com.flightdelay.ml.training.{CrossValidator, Trainer}
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * MLPipeline - Point d'entrée unique pour l'entraînement et l'évaluation de modèles ML
 *
 * Architecture Option B : Hold-out test set + K-fold CV
 * 1. Split initial 80/20 (dev/test)
 * 2. K-fold CV + Grid Search sur 80% dev
 * 3. Entraîner modèle final sur 80% dev avec best params
 * 4. Évaluation finale sur 20% test hold-out
 *
 * @example
 * {{{
 *   val result = MLPipeline.train(featuresData, experiment)
 *   println(s"CV F1: ${result.cvMetrics.avgF1} ± ${result.cvMetrics.stdF1}")
 *   println(s"Test F1: ${result.holdOutMetrics.f1Score}")
 * }}}
 */
object MLPipeline {

  /**
   * Result of complete ML pipeline
   * @param experiment Experiment configuration
   * @param model Trained model (on dev set)
   * @param cvMetrics Cross-validation metrics (mean ± std)
   * @param holdOutMetrics Final evaluation on hold-out test set
   * @param bestHyperparameters Best hyperparameters from grid search (if enabled)
   * @param trainingTimeSeconds Total training time
   */
  case class MLResult(
    experiment: ExperimentConfig,
    model: Transformer,
    cvMetrics: CVMetrics,
    holdOutMetrics: EvaluationMetrics,
    bestHyperparameters: Map[String, Any],
    trainingTimeSeconds: Double
  )

  /**
   * Cross-validation aggregated metrics
   */
  case class CVMetrics(
    avgAccuracy: Double,
    stdAccuracy: Double,
    avgPrecision: Double,
    stdPrecision: Double,
    avgRecall: Double,
    stdRecall: Double,
    avgF1: Double,
    stdF1: Double,
    avgAUC: Double,
    stdAUC: Double,
    numFolds: Int
  )

  /**
   * Train and evaluate a model using Option B strategy
   *
   * Pipeline:
   * 1. Initial split: 80% dev / 20% test
   * 2. K-fold CV + optional Grid Search on dev set
   * 3. Train final model on full dev set with best params
   * 4. Evaluate on hold-out test set
   * 5. Save model and metrics per experiment
   *
   * @param data Input DataFrame with "features" and "label" columns
   * @param experiment Experiment configuration
   * @param spark Implicit SparkSession
   * @param config Implicit AppConfiguration
   * @return MLResult with trained model and comprehensive metrics
   */
  def train(
    data: DataFrame,
    experiment: ExperimentConfig
  )(implicit spark: SparkSession, config: AppConfiguration): MLResult = {

    println("\n" + "=" * 100)
    println(s"[ML PIPELINE] Starting for experiment: ${experiment.name}")
    println("=" * 100)
    println(s"Strategy: Hold-out test (${experiment.train.trainRatio * 100}%) + K-fold CV (${experiment.train.crossValidation.numFolds} folds)")
    println(s"Model: ${experiment.model.modelType}")
    println(s"Target: ${experiment.target}")
    if (experiment.train.gridSearch.enabled) {
      println(s"Grid Search: ENABLED (metric: ${experiment.train.gridSearch.evaluationMetric})")
    }
    println("=" * 100)

    val startTime = System.currentTimeMillis()

    // ========================================================================
    // MLFlow Tracking Initialization
    // ========================================================================
    MLFlowTracker.initialize(config.common.mlflow.trackingUri, config.common.mlflow.enabled)
    val experimentId = MLFlowTracker.getOrCreateExperiment()
    val runId = experimentId.flatMap(expId => MLFlowTracker.startRun(expId, experiment.name))

    // Log experiment configuration
    runId.foreach { rid =>
      MLFlowTracker.logParams(rid, Map(
        "experiment_name" -> experiment.name,
        "target" -> experiment.target,
        "model_type" -> experiment.model.modelType,
        "train_ratio" -> experiment.train.trainRatio,
        "cv_folds" -> experiment.train.crossValidation.numFolds,
        "grid_search_enabled" -> experiment.train.gridSearch.enabled,
        "grid_search_metric" -> experiment.train.gridSearch.evaluationMetric,
        "feature_extraction_type" -> experiment.featureExtraction.featureType,
        "pca_enabled" -> experiment.featureExtraction.isPcaEnabled,
        "pca_variance_threshold" -> experiment.featureExtraction.pcaVarianceThreshold,
        "random_seed" -> config.common.seed
      ))
      MLFlowTracker.setTag(rid, "experiment_description", experiment.description)
      MLFlowTracker.setTag(rid, "environment", config.environment)
    }

    // ========================================================================
    // STEP 1: Initial split (dev/test)
    // ========================================================================
    println("\n[STEP 1] Initial Hold-out Split")
    println("-" * 80)

    val testRatio = 1.0 - experiment.train.trainRatio
    val Array(devData, testData) = data.randomSplit(
      Array(experiment.train.trainRatio, testRatio),
      seed = config.common.seed
    )

    println(f"  - Development set: ${devData.count()}%,d samples (${experiment.train.trainRatio * 100}%.0f%%)")
    println(f"  - Hold-out test:   ${testData.count()}%,d samples (${testRatio * 100}%.0f%%)")

    // ========================================================================
    // STEP 2: K-fold CV + Grid Search on dev set
    // ========================================================================
    println("\n[STEP 2] Cross-Validation on Development Set")
    println("-" * 80)

    val cvResult = CrossValidator.validate(devData, experiment)

    println(f"\n  CV Results (${cvResult.numFolds} folds):")
    println(f"    Accuracy:  ${cvResult.avgMetrics.accuracy * 100}%6.2f%% ± ${cvResult.stdMetrics.accuracy * 100}%.2f%%")
    println(f"    Precision: ${cvResult.avgMetrics.precision * 100}%6.2f%% ± ${cvResult.stdMetrics.precision * 100}%.2f%%")
    println(f"    Recall:    ${cvResult.avgMetrics.recall * 100}%6.2f%% ± ${cvResult.stdMetrics.recall * 100}%.2f%%")
    println(f"    F1-Score:  ${cvResult.avgMetrics.f1Score * 100}%6.2f%% ± ${cvResult.stdMetrics.f1Score * 100}%.2f%%")
    println(f"    AUC-ROC:   ${cvResult.avgMetrics.areaUnderROC}%6.4f ± ${cvResult.stdMetrics.areaUnderROC}%.4f")

    // Log CV metrics to MLFlow
    runId.foreach { rid =>
      // Log best hyperparameters
      MLFlowTracker.logParams(rid, cvResult.bestHyperparameters)

      // Log per-fold metrics
      cvResult.foldMetrics.zipWithIndex.foreach { case (metrics, fold) =>
        MLFlowTracker.logMetric(rid, s"cv_fold${fold}_accuracy", metrics.accuracy, step = fold)
        MLFlowTracker.logMetric(rid, s"cv_fold${fold}_precision", metrics.precision, step = fold)
        MLFlowTracker.logMetric(rid, s"cv_fold${fold}_recall", metrics.recall, step = fold)
        MLFlowTracker.logMetric(rid, s"cv_fold${fold}_f1", metrics.f1Score, step = fold)
        MLFlowTracker.logMetric(rid, s"cv_fold${fold}_auc", metrics.areaUnderROC, step = fold)
      }

      // Log aggregated CV metrics
      MLFlowTracker.logMetrics(rid, Map(
        "cv_mean_accuracy" -> cvResult.avgMetrics.accuracy,
        "cv_std_accuracy" -> cvResult.stdMetrics.accuracy,
        "cv_mean_precision" -> cvResult.avgMetrics.precision,
        "cv_std_precision" -> cvResult.stdMetrics.precision,
        "cv_mean_recall" -> cvResult.avgMetrics.recall,
        "cv_std_recall" -> cvResult.stdMetrics.recall,
        "cv_mean_f1" -> cvResult.avgMetrics.f1Score,
        "cv_std_f1" -> cvResult.stdMetrics.f1Score,
        "cv_mean_auc" -> cvResult.avgMetrics.areaUnderROC,
        "cv_std_auc" -> cvResult.stdMetrics.areaUnderROC
      ))
    }

    // ========================================================================
    // STEP 3: Train final model on full dev set
    // ========================================================================
    println("\n[STEP 3] Training Final Model on Full Development Set")
    println("-" * 80)

    val finalModel = Trainer.trainFinal(
      devData,
      experiment,
      cvResult.bestHyperparameters
    )

    // ========================================================================
    // STEP 4: Final evaluation on hold-out test set
    // ========================================================================
    println("\n[STEP 4] Final Evaluation on Hold-out Test Set")
    println("-" * 80)

    val testPredictions = finalModel.transform(testData)
    val holdOutMetrics = ModelEvaluator.evaluate(testPredictions)

    println(f"\n  Hold-out Test Metrics:")
    println(f"    Accuracy:  ${holdOutMetrics.accuracy * 100}%6.2f%%")
    println(f"    Precision: ${holdOutMetrics.precision * 100}%6.2f%%")
    println(f"    Recall:    ${holdOutMetrics.recall * 100}%6.2f%%")
    println(f"    F1-Score:  ${holdOutMetrics.f1Score * 100}%6.2f%%")
    println(f"    AUC-ROC:   ${holdOutMetrics.areaUnderROC}%6.4f")

    // Log hold-out metrics to MLFlow
    runId.foreach { rid =>
      MLFlowTracker.logMetrics(rid, Map(
        "test_accuracy" -> holdOutMetrics.accuracy,
        "test_precision" -> holdOutMetrics.precision,
        "test_recall" -> holdOutMetrics.recall,
        "test_f1" -> holdOutMetrics.f1Score,
        "test_auc" -> holdOutMetrics.areaUnderROC
      ))
    }

    // ========================================================================
    // STEP 5: Save model and metrics
    // ========================================================================
    println("\n[STEP 5] Saving Model and Metrics")
    println("-" * 80)

    val experimentOutputPath = s"${config.common.output.basePath}/${experiment.name}"
    val modelPath = s"$experimentOutputPath/models/${experiment.model.modelType}_final"

    println(s"  - Model path: $modelPath")
    finalModel.asInstanceOf[PipelineModel].write.overwrite().save(modelPath)
    println("  ✓ Model saved")

    // Save comprehensive metrics
    saveMetrics(experiment, cvResult, holdOutMetrics, testPredictions, experimentOutputPath)

    val endTime = System.currentTimeMillis()
    val totalTime = (endTime - startTime) / 1000.0

    println(f"\n  ✓ Total pipeline time: $totalTime%.2f seconds")

    // Log training time and artifacts to MLFlow
    runId.foreach { rid =>
      MLFlowTracker.logMetric(rid, "training_time_seconds", totalTime)

      // Log artifacts (metrics CSVs, model directory)
      val metricsPath = s"$experimentOutputPath/metrics"
      MLFlowTracker.logArtifact(rid, metricsPath)
      MLFlowTracker.logArtifact(rid, modelPath)

      // Log PCA artifacts if available
      val pcaMetricsPath = s"$metricsPath/pca_variance.csv"
      if (new java.io.File(pcaMetricsPath).exists()) {
        MLFlowTracker.logArtifact(rid, pcaMetricsPath)
      }
    }

    // ========================================================================
    // Display Best Model Summary
    // ========================================================================
    displayBestModelSummary(experiment, cvResult, holdOutMetrics, totalTime)

    println("=" * 100)
    println(s"[ML PIPELINE] Completed for experiment: ${experiment.name}")
    println("=" * 100 + "\n")

    // End MLFlow run
    runId.foreach { rid =>
      MLFlowTracker.endRun(rid)
    }

    // Prepare CV metrics
    val cvMetrics = CVMetrics(
      avgAccuracy = cvResult.avgMetrics.accuracy,
      stdAccuracy = cvResult.stdMetrics.accuracy,
      avgPrecision = cvResult.avgMetrics.precision,
      stdPrecision = cvResult.stdMetrics.precision,
      avgRecall = cvResult.avgMetrics.recall,
      stdRecall = cvResult.stdMetrics.recall,
      avgF1 = cvResult.avgMetrics.f1Score,
      stdF1 = cvResult.stdMetrics.f1Score,
      avgAUC = cvResult.avgMetrics.areaUnderROC,
      stdAUC = cvResult.stdMetrics.areaUnderROC,
      numFolds = cvResult.numFolds
    )

    MLResult(
      experiment = experiment,
      model = finalModel,
      cvMetrics = cvMetrics,
      holdOutMetrics = holdOutMetrics,
      bestHyperparameters = cvResult.bestHyperparameters,
      trainingTimeSeconds = totalTime
    )
  }

  /**
   * Display best model summary with hyperparameters
   */
  private def displayBestModelSummary(
    experiment: ExperimentConfig,
    cvResult: CrossValidator.CVResult,
    holdOutMetrics: EvaluationMetrics,
    totalTime: Double
  ): Unit = {
    println("\n" + "=" * 100)
    println("BEST MODEL SUMMARY")
    println("=" * 100)

    println(s"\n📊 Experiment: ${experiment.name}")
    println(s"   ${experiment.description}")

    println(s"\n🤖 Model Type: ${experiment.model.modelType.toUpperCase}")

    // Display best hyperparameters
    println(s"\n⚙️  Best Hyperparameters:")
    if (cvResult.bestHyperparameters.nonEmpty) {
      cvResult.bestHyperparameters.toSeq.sortBy(_._1).foreach { case (param, value) =>
        println(s"   - ${param.padTo(25, ' ')} : $value")
      }
    } else {
      // Display default hyperparameters from config
      val hp = experiment.train.hyperparameters
      println(s"   - ${"numTrees".padTo(25, ' ')} : ${hp.numTrees.head}")
      println(s"   - ${"maxDepth".padTo(25, ' ')} : ${hp.maxDepth.head}")
      println(s"   - ${"maxBins".padTo(25, ' ')} : ${hp.maxBins}")
      println(s"   - ${"minInstancesPerNode".padTo(25, ' ')} : ${hp.minInstancesPerNode}")
      println(s"   - ${"subsamplingRate".padTo(25, ' ')} : ${hp.subsamplingRate}")
      println(s"   - ${"featureSubsetStrategy".padTo(25, ' ')} : ${hp.featureSubsetStrategy}")
      println(s"   - ${"impurity".padTo(25, ' ')} : ${hp.impurity}")
    }

    // Display performance metrics
    println(s"\n📈 Performance Metrics:")
    println(s"   Cross-Validation (${cvResult.numFolds}-fold):")
    println(f"     Accuracy  : ${cvResult.avgMetrics.accuracy * 100}%6.2f%% ± ${cvResult.stdMetrics.accuracy * 100}%5.2f%%")
    println(f"     Precision : ${cvResult.avgMetrics.precision * 100}%6.2f%% ± ${cvResult.stdMetrics.precision * 100}%5.2f%%")
    println(f"     Recall    : ${cvResult.avgMetrics.recall * 100}%6.2f%% ± ${cvResult.stdMetrics.recall * 100}%5.2f%%")
    println(f"     F1-Score  : ${cvResult.avgMetrics.f1Score * 100}%6.2f%% ± ${cvResult.stdMetrics.f1Score * 100}%5.2f%%")
    println(f"     AUC-ROC   : ${cvResult.avgMetrics.areaUnderROC}%6.4f ± ${cvResult.stdMetrics.areaUnderROC}%6.4f")

    println(s"\n   Hold-out Test Set:")
    println(f"     Accuracy  : ${holdOutMetrics.accuracy * 100}%6.2f%%")
    println(f"     Precision : ${holdOutMetrics.precision * 100}%6.2f%%")
    println(f"     Recall    : ${holdOutMetrics.recall * 100}%6.2f%%")
    println(f"     F1-Score  : ${holdOutMetrics.f1Score * 100}%6.2f%%")
    println(f"     AUC-ROC   : ${holdOutMetrics.areaUnderROC}%6.4f")

    // Display confusion matrix
    println(s"\n   Confusion Matrix (Test Set):")
    println(f"     True Positives  : ${holdOutMetrics.truePositives}%,d")
    println(f"     True Negatives  : ${holdOutMetrics.trueNegatives}%,d")
    println(f"     False Positives : ${holdOutMetrics.falsePositives}%,d")
    println(f"     False Negatives : ${holdOutMetrics.falseNegatives}%,d")

    println(f"\n⏱️  Total Training Time: $totalTime%.2f seconds")

    println("\n" + "=" * 100)
  }

  /**
   * Save all metrics (CV + hold-out) to CSV files
   */
  private def saveMetrics(
    experiment: ExperimentConfig,
    cvResult: CrossValidator.CVResult,
    holdOutMetrics: EvaluationMetrics,
    testPredictions: DataFrame,
    basePath: String
  )(implicit spark: SparkSession): Unit = {
    import com.flightdelay.utils.MetricsWriter
    import org.apache.spark.sql.functions._

    val metricsPath = s"$basePath/metrics"

    // Save CV fold metrics
    val cvHeaders = Seq("fold", "accuracy", "precision", "recall", "f1_score", "auc_roc", "auc_pr")
    val cvRows = cvResult.foldMetrics.zipWithIndex.map { case (metrics, idx) =>
      Seq(
        (idx + 1).toString,
        f"${metrics.accuracy}%.6f",
        f"${metrics.precision}%.6f",
        f"${metrics.recall}%.6f",
        f"${metrics.f1Score}%.6f",
        f"${metrics.areaUnderROC}%.6f",
        f"${metrics.areaUnderPR}%.6f"
      )
    }
    MetricsWriter.writeCsv(cvHeaders, cvRows, s"$metricsPath/cv_fold_metrics.csv")

    // Save CV average metrics
    val avgHeaders = Seq("metric", "mean", "std")
    val avgRows = Seq(
      Seq("accuracy", f"${cvResult.avgMetrics.accuracy}%.6f", f"${cvResult.stdMetrics.accuracy}%.6f"),
      Seq("precision", f"${cvResult.avgMetrics.precision}%.6f", f"${cvResult.stdMetrics.precision}%.6f"),
      Seq("recall", f"${cvResult.avgMetrics.recall}%.6f", f"${cvResult.stdMetrics.recall}%.6f"),
      Seq("f1_score", f"${cvResult.avgMetrics.f1Score}%.6f", f"${cvResult.stdMetrics.f1Score}%.6f"),
      Seq("auc_roc", f"${cvResult.avgMetrics.areaUnderROC}%.6f", f"${cvResult.stdMetrics.areaUnderROC}%.6f")
    )
    MetricsWriter.writeCsv(avgHeaders, avgRows, s"$metricsPath/cv_summary.csv")

    // Save hold-out test metrics
    val testHeaders = Seq("metric", "value")
    val testRows = Seq(
      Seq("accuracy", f"${holdOutMetrics.accuracy}%.6f"),
      Seq("precision", f"${holdOutMetrics.precision}%.6f"),
      Seq("recall", f"${holdOutMetrics.recall}%.6f"),
      Seq("f1_score", f"${holdOutMetrics.f1Score}%.6f"),
      Seq("auc_roc", f"${holdOutMetrics.areaUnderROC}%.6f"),
      Seq("auc_pr", f"${holdOutMetrics.areaUnderPR}%.6f"),
      Seq("true_positives", holdOutMetrics.truePositives.toString),
      Seq("true_negatives", holdOutMetrics.trueNegatives.toString),
      Seq("false_positives", holdOutMetrics.falsePositives.toString),
      Seq("false_negatives", holdOutMetrics.falseNegatives.toString)
    )
    MetricsWriter.writeCsv(testHeaders, testRows, s"$metricsPath/holdout_test_metrics.csv")

    // Save best hyperparameters if grid search was used
    if (cvResult.bestHyperparameters.nonEmpty) {
      val paramHeaders = Seq("parameter", "value")
      val paramRows = cvResult.bestHyperparameters.map { case (k, v) =>
        Seq(k, v.toString)
      }.toSeq
      MetricsWriter.writeCsv(paramHeaders, paramRows, s"$metricsPath/best_hyperparameters.csv")
    }

    // Save ROC data for hold-out test (for ROC curve visualization)
    // Extract label, probability for class 1, and prediction
    import org.apache.spark.ml.linalg.Vector
    import org.apache.spark.sql.functions.udf

    val getProbPositive = udf((v: Vector) => v(1))

    val rocData = testPredictions
      .select(
        col("label"),
        getProbPositive(col("probability")).alias("prob_positive"),
        col("prediction")
      )
      .limit(10000)  // Limit to 10k samples for performance

    val rocHeaders = Seq("label", "prob_positive", "prediction")
    val rocRows = rocData.collect().map { row =>
      Seq(
        row.getDouble(0).toString,
        f"${row.getDouble(1)}%.6f",
        row.getDouble(2).toString
      )
    }
    MetricsWriter.writeCsv(rocHeaders, rocRows, s"$metricsPath/holdout_roc_data.csv")

    println(s"  ✓ Metrics saved to: $metricsPath")
    println(s"\n  [Visualization] To visualize ml pipeline metrics, run:")
    println(s"    python work/scripts/visualize_ml_pipeline.py $metricsPath")
  }
}
