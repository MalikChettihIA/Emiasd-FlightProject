package com.flightdelay.ml

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import com.flightdelay.features.FeatureExtractor
import com.flightdelay.features.balancer.DelayBalancedDatasetBuilder
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
    experiment: ExperimentConfig
  )(implicit spark: SparkSession, configuration: AppConfiguration): MLResult = {

    println("=" * 100)
    println(s"[ML PIPELINE] Starting for experiment: ${experiment.name}")
    println("=" * 100)
    println(s"Strategy: Hold-out test (${experiment.train.trainRatio * 100}%) + K-fold CV (${experiment.train.crossValidation.numFolds} folds)")
    println(s"Model: ${experiment.model.modelType}")
    println(s"Target: ${experiment.target}")
    if (experiment.train.gridSearch.enabled) {
      println(s"Grid Search: ENABLED (metric: ${experiment.train.gridSearch.evaluationMetric})")
    }
    println("=" * 100)

    // Load joined and exploded data (before feature extraction)
    val joinedDataPath = s"${configuration.common.output.basePath}/${experiment.name}/data/joined_exploded_data.parquet"

    println(s"Loading prepared data:")
    println(s"  - Path: $joinedDataPath")
    val rawData = spark.read.parquet(joinedDataPath)
    println(f"  - Loaded ${rawData.count()}%,d records")
    println(f"  - Schema")
    rawData.printSchema()


    val startTime = System.currentTimeMillis()

    // ========================================================================
    // MLFlow Tracking Initialization
    // ========================================================================
    MLFlowTracker.initialize(configuration.common.mlflow.trackingUri, configuration.common.mlflow.enabled)
    val experimentId = MLFlowTracker.getOrCreateExperiment()
    val runId = experimentId.flatMap(expId => MLFlowTracker.startRun(expId, experiment.name, Some(experiment.description)))

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
        "random_seed" -> configuration.common.seed
      ))
      MLFlowTracker.setTag(rid, "experiment_description", experiment.description)
      MLFlowTracker.setTag(rid, "environment", configuration.environment)

      MLFlowTracker.logDatasetsFromConfig(rid, configuration)
    }

    // ========================================================================
    // STEP 1: Initial split (dev/test) BEFORE feature extraction
    // ========================================================================
    println("[ML PIPELINE][STEP 1] Initial Hold-out Split (before feature extraction)")
    println("-" * 80)
    println("Note: Splitting BEFORE feature extraction to avoid data leakage")

    val testRatio = 1.0 - experiment.train.trainRatio
    val (devDataRaw, testDataRaw) = DelayBalancedDatasetBuilder.buildBalancedTrainTest(
      labeledDf = rawData,
      trainRatio = experiment.train.trainRatio
    )

    println(f"  - Development set: ${devDataRaw.count()}%,d samples (${experiment.train.trainRatio * 100}%.0f%%)")
    println(f"  - Hold-out test:   ${testDataRaw.count()}%,d samples (${testRatio * 100}%.0f%%)")

    // ========================================================================
    // STEP 2,3: Feature extraction (fit on TRAIN only, transform both)
    // ========================================================================
    println("[ML PIPELINE][STEP 2] Feature Extraction (fit on dev set only)")
    println("-" * 80)
    println("✓ CRITICAL: Feature transformers are fit ONLY on training data")
    println("            to prevent data leakage from test set")

    // Extract features from dev set (fit + transform)
    // Returns both transformed data AND fitted models for reuse on test set
    val (devData, featureModels) = FeatureExtractor.extract(devDataRaw, experiment)
    println(f"  ✓ Dev features extracted: ${devData.count()}%,d records")

    // Transform test set using pre-fitted models from dev set (NO REFITTING)
    println("-" * 80)
    println("[ML PIPELINE][STEP 3] Feature Extraction (test set)")
    println("-" * 80)
    println(" Using pre-fitted models from dev set - NO DATA LEAKAGE")
    println("  - StringIndexer: uses categories learned from dev set only")
    println("  - Scaler: uses statistics (mean/std) from dev set only")
    println("  - PCA: uses components fitted on dev set only\n")
    val testData = FeatureExtractor.transform(testDataRaw, featureModels, experiment)
    println(f"  ✓ Test features extracted: ${testData.count()}%,d records")

    // ========================================================================
    // STEP 4: K-fold CV + Grid Search on dev set
    // ========================================================================
    println("[ML PIPELINE][STEP 4] Cross-Validation on Development Set")
    println("-" * 80)

    val cvResult = CrossValidator.validate(devData, experiment)

    println(f"  CV Results (${cvResult.numFolds} folds):")
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
    // STEP 5: Train final model on full dev set
    // ========================================================================
    println("[ML PIPELINE][STEP 5] Training Final Model on Full Development Set")
    println("-" * 80)

    val finalModel = Trainer.trainFinal(
      devData,
      experiment,
      cvResult.bestHyperparameters
    )

    // ========================================================================
    // STEP 6: Final evaluation on hold-out test set
    // ========================================================================
    println("[STEP 6] Final Evaluation on Hold-out Test Set")
    println("-" * 80)

    // ✅ OPTIMIZATION: Save final model then reload to avoid broadcast OOM
    // This is critical for large models (e.g., Random Forest with 300 trees)
    val experimentOutputPath = s"${configuration.common.output.basePath}/${experiment.name}"
    val modelPath = s"$experimentOutputPath/models/${experiment.model.modelType}_final"

    println(s"  💾 Saving final model to: $modelPath")
    finalModel.asInstanceOf[PipelineModel].write.overwrite().save(modelPath)

    println(s"  📂 Reloading model for evaluation to avoid broadcast...")
    val reloadedModel = PipelineModel.load(modelPath)

    println(s"  🔍 Evaluating on hold-out test set...")
    val testPredictions = reloadedModel.transform(testData)
    val holdOutMetrics = ModelEvaluator.evaluate(testPredictions)

    println(f"  Hold-out Test Metrics:")
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
        "test_auc" -> holdOutMetrics.areaUnderROC,
        "test_recall_delayed" -> holdOutMetrics.recallDelayed,
        "test_recall_ontime" -> holdOutMetrics.recallOnTime
      ))
    }

    // ========================================================================
    // STEP 7: Save metrics
    // ========================================================================
    println("[STEP 7] Saving Metrics")
    println("-" * 80)
    println("  ✓ Model already saved in Step 5 (to avoid broadcast OOM)")

    // Save comprehensive metrics (CSV + TXT summary)
    saveMetrics(experiment, cvResult, holdOutMetrics, testPredictions, experimentOutputPath)

    val endTime = System.currentTimeMillis()
    val totalTime = (endTime - startTime) / 1000.0

    // Save training summary as TXT file
    saveTrainingSummary(experiment, cvResult, holdOutMetrics, totalTime, experimentOutputPath)

    // Generate visualization plots
    val metricsPath = s"$experimentOutputPath/metrics"
    generatePlots(metricsPath)

    println(f"  ✓ Total pipeline time: $totalTime%.2f seconds")

    // Log training time and artifacts to MLFlow
    runId.foreach { rid =>
      MLFlowTracker.logMetric(rid, "training_time_seconds", totalTime)

      // Log artifacts organized in subdirectories
      // 1. Log metrics CSVs to "metrics/" subdirectory
      MLFlowTracker.logArtifactWithPath(rid, metricsPath, "metrics")

      // 2. Log model to "models/" subdirectory
      MLFlowTracker.logArtifactWithPath(rid, modelPath, "models")

      // 3. Log YAML configuration to "configuration/" subdirectory
      val configPath = s"${configuration.environment}-config.yml"
      val configSourcePath = getClass.getClassLoader.getResource(configPath)
      if (configSourcePath != null) {
        // Copy config to experiment output for logging
        val configDestPath = s"$experimentOutputPath/configuration"
        val configDestFile = s"$configDestPath/${configuration.environment}-config.yml"

        // Create config directory and copy file
        val configDir = new java.io.File(configDestPath)
        if (!configDir.exists()) configDir.mkdirs()

        val source = scala.io.Source.fromURL(configSourcePath, "UTF-8")
        val configContent = source.mkString
        source.close()

        val writer = new java.io.PrintWriter(configDestFile)
        writer.write(configContent)
        writer.close()

        // Log configuration directory to MLFlow
        MLFlowTracker.logArtifactWithPath(rid, configDestPath, "configuration")
        println(s"  ✓ Configuration saved to MLFlow: configuration/${configuration.environment}-config.yml")
      }

      // 4. Log feature files to "features/" subdirectory
      val featuresPath = s"$experimentOutputPath/features"
      val featuresDir = new java.io.File(featuresPath)
      if (featuresDir.exists() && featuresDir.isDirectory) {
        MLFlowTracker.logArtifactWithPath(rid, featuresPath, "features")
        println(s"  ✓ Feature files saved to MLFlow: features/")
      }

      // 5. Log visualization plots to "plots/" subdirectory
      val plotsPath = s"$metricsPath/plots-ml-pipeline"
      val plotsDir = new java.io.File(plotsPath)
      if (plotsDir.exists() && plotsDir.isDirectory) {
        MLFlowTracker.logArtifactWithPath(rid, plotsPath, "plots")
        println(s"  ✓ Visualization plots saved to MLFlow: plots/")
      }
    }

    // ========================================================================
    // Display Best Model Summary
    // ========================================================================
    displayBestModelSummary(experiment, cvResult, holdOutMetrics, totalTime)

    println("=" * 100)
    println(s"[ML PIPELINE] Completed for experiment: ${experiment.name}")
    println("=" * 100)

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
      model = reloadedModel,
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
      val hp = experiment.model.hyperparameters
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
    println(f"     RECd      : ${holdOutMetrics.recallDelayed * 100}%6.2f%%  (Recall Delayed)")
    println(f"     RECo      : ${holdOutMetrics.recallOnTime * 100}%6.2f%%  (Recall On-time)")

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
      Seq("recall_delayed", f"${holdOutMetrics.recallDelayed}%.6f"),
      Seq("recall_ontime", f"${holdOutMetrics.recallOnTime}%.6f"),
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

  /**
   * Save training summary as a readable TXT file
   */
  private def saveTrainingSummary(
    experiment: ExperimentConfig,
    cvResult: CrossValidator.CVResult,
    holdOutMetrics: EvaluationMetrics,
    totalTime: Double,
    basePath: String
  ): Unit = {
    val metricsPath = s"$basePath/metrics"
    val summaryFile = s"$metricsPath/training_summary.txt"

    val summary = new StringBuilder()

    // Header
    summary.append("=" * 100 + "\n")
    summary.append("TRAINING SUMMARY\n")
    summary.append("=" * 100 + "\n\n")

    // Experiment Information
    summary.append("EXPERIMENT INFORMATION\n")
    summary.append("-" * 100 + "\n")
    summary.append(s"Name:        ${experiment.name}\n")
    summary.append(s"Description: ${experiment.description}\n")
    summary.append(s"Model Type:  ${experiment.model.modelType.toUpperCase}\n")
    summary.append(s"Target:      ${experiment.target}\n")
    summary.append("\n")

    // Hyperparameters
    summary.append("HYPERPARAMETERS\n")
    summary.append("-" * 100 + "\n")
    if (cvResult.bestHyperparameters.nonEmpty) {
      cvResult.bestHyperparameters.toSeq.sortBy(_._1).foreach { case (param, value) =>
        summary.append(f"  ${param.padTo(30, ' ')} : $value\n")
      }
    } else {
      val hp = experiment.model.hyperparameters
      summary.append(f"  ${"numTrees".padTo(30, ' ')} : ${hp.numTrees.headOption.getOrElse("N/A")}\n")
      summary.append(f"  ${"maxDepth".padTo(30, ' ')} : ${hp.maxDepth.headOption.getOrElse("N/A")}\n")
      summary.append(f"  ${"maxBins".padTo(30, ' ')} : ${hp.maxBins.headOption.getOrElse("N/A")}\n")
      summary.append(f"  ${"minInstancesPerNode".padTo(30, ' ')} : ${hp.minInstancesPerNode.headOption.getOrElse("N/A")}\n")
      summary.append(f"  ${"subsamplingRate".padTo(30, ' ')} : ${hp.subsamplingRate.headOption.getOrElse("N/A")}\n")
      summary.append(f"  ${"featureSubsetStrategy".padTo(30, ' ')} : ${hp.featureSubsetStrategy.headOption.getOrElse("N/A")}\n")
      summary.append(f"  ${"impurity".padTo(30, ' ')} : ${hp.impurity.getOrElse("N/A")}\n")
    }
    summary.append("\n")

    // Cross-Validation Results
    summary.append("CROSS-VALIDATION RESULTS\n")
    summary.append("-" * 100 + "\n")
    summary.append(s"Number of Folds: ${cvResult.numFolds}\n\n")
    summary.append(f"  Metric       Mean          Std Dev\n")
    summary.append(f"  ${"=" * 50}\n")
    summary.append(f"  Accuracy     ${cvResult.avgMetrics.accuracy * 100}%6.2f%%      ± ${cvResult.stdMetrics.accuracy * 100}%5.2f%%\n")
    summary.append(f"  Precision    ${cvResult.avgMetrics.precision * 100}%6.2f%%      ± ${cvResult.stdMetrics.precision * 100}%5.2f%%\n")
    summary.append(f"  Recall       ${cvResult.avgMetrics.recall * 100}%6.2f%%      ± ${cvResult.stdMetrics.recall * 100}%5.2f%%\n")
    summary.append(f"  F1-Score     ${cvResult.avgMetrics.f1Score * 100}%6.2f%%      ± ${cvResult.stdMetrics.f1Score * 100}%5.2f%%\n")
    summary.append(f"  AUC-ROC      ${cvResult.avgMetrics.areaUnderROC}%6.4f       ± ${cvResult.stdMetrics.areaUnderROC}%6.4f\n")
    summary.append("\n")

    // Hold-out Test Results
    summary.append("HOLD-OUT TEST SET RESULTS\n")
    summary.append("-" * 100 + "\n")
    summary.append(f"  Accuracy:     ${holdOutMetrics.accuracy * 100}%6.2f%%\n")
    summary.append(f"  Precision:    ${holdOutMetrics.precision * 100}%6.2f%%\n")
    summary.append(f"  Recall:       ${holdOutMetrics.recall * 100}%6.2f%%\n")
    summary.append(f"  F1-Score:     ${holdOutMetrics.f1Score * 100}%6.2f%%\n")
    summary.append(f"  AUC-ROC:      ${holdOutMetrics.areaUnderROC}%6.4f\n")
    summary.append(f"  RECd:         ${holdOutMetrics.recallDelayed * 100}%6.2f%%  (Recall Delayed)\n")
    summary.append(f"  RECo:         ${holdOutMetrics.recallOnTime * 100}%6.2f%%  (Recall On-time)\n")
    summary.append("\n")

    // Confusion Matrix
    summary.append("CONFUSION MATRIX (Test Set)\n")
    summary.append("-" * 100 + "\n")
    summary.append(f"  True Positives:   ${holdOutMetrics.truePositives}%,10d\n")
    summary.append(f"  True Negatives:   ${holdOutMetrics.trueNegatives}%,10d\n")
    summary.append(f"  False Positives:  ${holdOutMetrics.falsePositives}%,10d\n")
    summary.append(f"  False Negatives:  ${holdOutMetrics.falseNegatives}%,10d\n")
    summary.append("\n")

    // Training Time
    summary.append("TRAINING TIME\n")
    summary.append("-" * 100 + "\n")
    summary.append(f"  Total Time: $totalTime%.2f seconds (${totalTime / 60}%.2f minutes)\n")
    summary.append("\n")

    // Footer
    summary.append("=" * 100 + "\n")
    summary.append(s"Generated: ${java.time.LocalDateTime.now()}\n")
    summary.append("=" * 100 + "\n")

    // Write to file
    val writer = new java.io.PrintWriter(summaryFile)
    try {
      writer.write(summary.toString())
      println(s"  ✓ Training summary saved to: $summaryFile")
    } finally {
      writer.close()
    }
  }

  /**
   * Generate visualization plots using Python script
   *
   * Calls visualize_ml_pipeline.py to create comprehensive visualizations:
   * - CV fold metrics
   * - CV vs hold-out comparison
   * - Stability box plots
   * - Confusion matrix
   * - Radar chart
   * - ROC curve
   * - Hyperparameters summary
   *
   * @param metricsPath Path to metrics directory containing CSV files
   */
  private def generatePlots(metricsPath: String): Unit = {
    import scala.sys.process._
    import scala.util.{Try, Success, Failure}

    println("\n[STEP 8] Generating Visualization Plots")
    println("-" * 80)

    // Path to Python script (mounted in Docker container at /scripts)
    val scriptPath = "/scripts/visualize_ml_pipeline.py"

    // Check if script exists
    val scriptFile = new java.io.File(scriptPath)
    if (!scriptFile.exists()) {
      println(s"  ⚠ Warning: Visualization script not found: $scriptPath")
      println(s"  ⚠ Skipping plot generation")
      return
    }

    // Execute Python script
    val command = s"python3 $scriptPath $metricsPath"
    println(s"  📊 Running: $command")

    Try {
      val exitCode = command.!
      exitCode
    } match {
      case Success(0) =>
        println(s"  ✓ Plots generated successfully in: $metricsPath/plots-ml-pipeline/")
      case Success(exitCode) =>
        println(s"  ⚠ Warning: Plot generation failed with exit code: $exitCode")
        println(s"  ⚠ Continuing without plots...")
      case Failure(e) =>
        println(s"  ⚠ Warning: Could not execute Python script: ${e.getMessage}")
        println(s"  ⚠ Make sure Python 3 and required libraries are installed")
        println(s"  ⚠ Continuing without plots...")
    }
  }
}
