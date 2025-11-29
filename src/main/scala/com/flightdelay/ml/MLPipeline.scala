package com.flightdelay.ml

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import com.flightdelay.features.FeatureExtractor
import com.flightdelay.features.balancer.DelayBalancedDatasetBuilder
import com.flightdelay.ml.evaluation.ModelEvaluator
import com.flightdelay.ml.evaluation.ModelEvaluator.EvaluationMetrics
import com.flightdelay.ml.tracking.MLFlowTracker
import com.flightdelay.ml.training.{CrossValidator, Trainer}
import com.flightdelay.utils.{ExecutionTimeTracker, HDFSHelper}
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.flightdelay.utils.DebugUtils._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets

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
   * Train and evaluate a model using pre-split balanced datasets
   *
   * Pipeline:
   * 1. Receive pre-split train/test datasets (already balanced from FeaturePipeline)
   * 2. K-fold CV + optional Grid Search on dev set (skipped if fast=true)
   * 3. Train final model on full dev set with best params
   * 4. Evaluate on hold-out test set
   * 5. Save model and metrics per experiment
   *
   * @param devDataRaw Pre-split training dataset (balanced, exploded, but features not extracted yet)
   * @param testDataRaw Pre-split test dataset (balanced, exploded, but features not extracted yet)
   * @param experiment Experiment configuration
   * @param fast Skip cross-validation and go directly to final model training (default: false)
   * @param timeTracker Execution time tracker
   * @param spark Implicit SparkSession
   * @param config Implicit AppConfiguration
   * @return MLResult with trained model and comprehensive metrics
   */
  def train(
             devDataRaw: DataFrame,
             testDataRaw: DataFrame,
             experiment: ExperimentConfig,
             fast: Boolean = false,
             timeTracker: ExecutionTimeTracker = null
  )(implicit spark: SparkSession, configuration: AppConfiguration): MLResult = {

    info("=" * 100)
    info(s"[ML PIPELINE] Starting for experiment: ${experiment.name}")
    info("=" * 100)
    if (fast) {
      info(s"Mode: FAST (skipping cross-validation)")
    } else {
      info(s"Strategy: Hold-out test (${experiment.train.trainRatio * 100}%) + K-fold CV (${experiment.train.crossValidation.numFolds} folds)")
    }
    info(s"Model: ${experiment.model.modelType}")
    info(s"Target: ${experiment.target}")
    if (!fast && experiment.train.gridSearch.enabled) {
      info(s"Grid Search: ENABLED (metric: ${experiment.train.gridSearch.evaluationMetric})")
    }
    info("=" * 100)


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
    // STEP 1: Verify pre-split datasets (already balanced from FeaturePipeline)
    // ========================================================================
    info("[ML PIPELINE][STEP 1] Received pre-split balanced datasets from FeaturePipeline")
    info("=" * 80)
    info("Note: Train/test split was done in FeaturePipeline BEFORE join/explosion")
    info("      This ensures balanced datasets and avoids manipulating huge datasets")

    whenDebug{
      val devCount = devDataRaw.count()
      val testCount = testDataRaw.count()
      val testRatio = 1.0 - experiment.train.trainRatio
      debug(f"  - Development set: $devCount%,d samples (${experiment.train.trainRatio * 100}%.0f%%)")
      debug(f"  - Hold-out test:   $testCount%,d samples (${testRatio * 100}%.0f%%)")
    }

    // ========================================================================
    // STEP 2,3: Feature extraction (fit on TRAIN only, transform both)
    // ========================================================================
    info("[ML PIPELINE][STEP 2] Feature Extraction (fit on dev set only)")
    info("=" * 80)
    info(" CRITICAL: Feature transformers are fit ONLY on training data")
    info("            to prevent data leakage from test set")

    // Extract features from dev set (fit + transform)
    // Returns both transformed data AND fitted models for reuse on test set
    if (timeTracker != null) timeTracker.startStep("ml_feature_extraction.dev")
    val (devData, featureModels) = FeatureExtractor.extract(devDataRaw, experiment)
    if (timeTracker != null) timeTracker.endStep("ml_feature_extraction.dev")
    debug(f"   Dev features extracted: ${devData.count()}%,d records")

    // Transform test set using pre-fitted models from dev set (NO REFITTING)
    info("=" * 80)
    info("[ML PIPELINE][STEP 3] Feature Extraction (test set)")
    info("=" * 80)
    info(" Using pre-fitted models from dev set - NO DATA LEAKAGE")
    info("  - StringIndexer: uses categories learned from dev set only")
    info("  - Scaler: uses statistics (mean/std) from dev set only")
    info("  - PCA: uses components fitted on dev set only")
    if (timeTracker != null) timeTracker.startStep("ml_feature_extraction.test")
    val testData = FeatureExtractor.transform(testDataRaw, featureModels, experiment)
    if (timeTracker != null) timeTracker.endStep("ml_feature_extraction.test")
    debug(f"   Test features extracted: ${testData.count()}%,d records")

    // ========================================================================
    // STEP 4: K-fold CV + Grid Search on dev set (skipped if fast=true)
    // ========================================================================
    val cvResult = if (fast) {
      info("[ML PIPELINE][STEP 4] FAST MODE - Skipping Cross-Validation")
      info("=" * 80)
      info("  Using default hyperparameters from configuration")

      if (timeTracker != null) {
        timeTracker.setStepNA("ml_grid_search")
        timeTracker.setStepNA("ml_kfold_cv")
      }

      // Create a dummy CV result with empty metrics
      val emptyMetrics = EvaluationMetrics(
        accuracy = 0.0,
        precision = 0.0,
        recall = 0.0,
        f1Score = 0.0,
        areaUnderROC = 0.0,
        areaUnderPR = 0.0,
        truePositives = 0L,
        trueNegatives = 0L,
        falsePositives = 0L,
        falseNegatives = 0L
      )

      CrossValidator.CVResult(
        avgMetrics = emptyMetrics,
        stdMetrics = emptyMetrics,
        foldMetrics = Seq.empty,
        bestHyperparameters = Map.empty,
        numFolds = 0
      )
    } else {
      info("[ML PIPELINE][STEP 4] Cross-Validation on Development Set")
      info("=" * 80)

      // Track CV (Grid Search is included in CV, not separate)
      if (timeTracker != null) {
        timeTracker.startStep("ml_kfold_cv")
      }

      val cvRes = CrossValidator.validate(devData, experiment)

      if (timeTracker != null) {
        timeTracker.endStep("ml_kfold_cv")
        // Grid Search is part of CV, mark as NA to show it's included
        timeTracker.setStepNA("ml_grid_search")
      }

      info(f"  CV Results (${cvRes.numFolds} folds):")
      info(f"    Accuracy:  ${cvRes.avgMetrics.accuracy * 100}%6.2f%% ± ${cvRes.stdMetrics.accuracy * 100}%.2f%%")
      info(f"    Precision: ${cvRes.avgMetrics.precision * 100}%6.2f%% ± ${cvRes.stdMetrics.precision * 100}%.2f%%")
      info(f"    Recall:    ${cvRes.avgMetrics.recall * 100}%6.2f%% ± ${cvRes.stdMetrics.recall * 100}%.2f%%")
      info(f"    F1-Score:  ${cvRes.avgMetrics.f1Score * 100}%6.2f%% ± ${cvRes.stdMetrics.f1Score * 100}%.2f%%")
      info(f"    AUC-ROC:   ${cvRes.avgMetrics.areaUnderROC}%6.4f ± ${cvRes.stdMetrics.areaUnderROC}%.4f")

      // Log CV metrics to MLFlow
      runId.foreach { rid =>
        // Log best hyperparameters
        MLFlowTracker.logParams(rid, cvRes.bestHyperparameters)

        // Log per-fold metrics
        cvRes.foldMetrics.zipWithIndex.foreach { case (metrics, fold) =>
          MLFlowTracker.logMetric(rid, s"cv_fold${fold}_accuracy", metrics.accuracy, step = fold)
          MLFlowTracker.logMetric(rid, s"cv_fold${fold}_precision", metrics.precision, step = fold)
          MLFlowTracker.logMetric(rid, s"cv_fold${fold}_recall", metrics.recall, step = fold)
          MLFlowTracker.logMetric(rid, s"cv_fold${fold}_f1", metrics.f1Score, step = fold)
          MLFlowTracker.logMetric(rid, s"cv_fold${fold}_auc", metrics.areaUnderROC, step = fold)
        }

        // Log aggregated CV metrics
        MLFlowTracker.logMetrics(rid, Map(
          "cv_mean_accuracy" -> cvRes.avgMetrics.accuracy,
          "cv_std_accuracy" -> cvRes.stdMetrics.accuracy,
          "cv_mean_precision" -> cvRes.avgMetrics.precision,
          "cv_std_precision" -> cvRes.stdMetrics.precision,
          "cv_mean_recall" -> cvRes.avgMetrics.recall,
          "cv_std_recall" -> cvRes.stdMetrics.recall,
          "cv_mean_f1" -> cvRes.avgMetrics.f1Score,
          "cv_std_f1" -> cvRes.stdMetrics.f1Score,
          "cv_mean_auc" -> cvRes.avgMetrics.areaUnderROC,
          "cv_std_auc" -> cvRes.stdMetrics.areaUnderROC
        ))
      }

      cvRes
    }

    // ========================================================================
    // STEP 5: Train final model on full dev set
    // ========================================================================
    info("[ML PIPELINE][STEP 5] Training Final Model on Full Development Set")
    info("=" * 80)

    if (timeTracker != null) timeTracker.startStep("ml_train")
    val finalModel = Trainer.trainFinal(
      devData,
      experiment,
      cvResult.bestHyperparameters
    )
    if (timeTracker != null) timeTracker.endStep("ml_train")

    // ========================================================================
    // STEP 6: Final evaluation on hold-out test set
    // =======================================================================
    info("[STEP 6] Final Evaluation on Hold-out Test Set")
    info("=" * 80)

    if (timeTracker != null) timeTracker.startStep("ml_evaluation")

    // OPTIMIZATION: Save final model then reload to avoid broadcast OOM
    // This is critical for large models (e.g., Random Forest with 300 trees)
    val experimentOutputPath = s"${configuration.common.output.basePath}/${experiment.name}"
    val modelPath = s"$experimentOutputPath/models/${experiment.model.modelType}_final"

    info(s"   Saving final model to: $modelPath")
    finalModel.asInstanceOf[PipelineModel].write.overwrite().save(modelPath)

    // Save and log feature importances report for tree-based models
    try {
      val pipelineModel = finalModel.asInstanceOf[PipelineModel]
      val baseModel = pipelineModel.stages(0)

      // Check if model has feature importances (RandomForest, GBT)
      val featureImportancesOpt = baseModel match {
        case rf: org.apache.spark.ml.classification.RandomForestClassificationModel =>
          Some((rf.featureImportances.toArray, "RandomForest"))
        case gbt: org.apache.spark.ml.classification.GBTClassificationModel =>
          Some((gbt.featureImportances.toArray, "GBT"))
        case _ => None
      }

      featureImportancesOpt.foreach { case (importances, modelType) =>
        // Load feature names
        val featureNamesPath = s"$experimentOutputPath/features/selected_features.txt"
        val featureNames = try {
          val source = scala.io.Source.fromFile(featureNamesPath)
          val names = source.getLines().toArray
          source.close()
          names
        } catch {
          case _: Exception =>
            // If file not found, generate generic names
            importances.indices.map(i => s"Feature_$i").toArray
        }

        // Create feature importances report
        val reportPath = s"$experimentOutputPath/features/feature_importances_report.txt"
        val report = new StringBuilder
        report.append("=" * 90).append("\n")
        report.append(s"Top 20 Feature Importances ($modelType)\n")
        report.append("=" * 90).append("\n")
        report.append(f"${"Rank"}%-6s ${"Index"}%-7s ${"Feature Name"}%-60s ${"Importance"}%12s\n")
        report.append("=" * 90).append("\n")

        importances.zipWithIndex
          .sortBy(-_._1)
          .take(20)
          .zipWithIndex
          .foreach { case ((importance, featureIdx), rank) =>
            val featureName = if (featureIdx < featureNames.length) featureNames(featureIdx) else s"Feature_$featureIdx"
            val importancePercent = importance * 100

            // Visual indicator for importance level
            val indicator = if (importancePercent >= 10) "█"
                           else if (importancePercent >= 5) "▓"
                           else if (importancePercent >= 1) "▒"
                           else "░"

            report.append(f"${rank + 1}%-6d [${featureIdx}%3d]  ${featureName}%-60s ${indicator}  ${importancePercent}%5.2f%%\n")
          }

        report.append("=" * 90).append("\n")
        report.append("Importance Levels:  █≥10% ▓≥5% ▒≥1% ░<1%\n")
        report.append("Abbreviations:\n")
        report.append("  indexed_              = idx_\n")
        report.append("  origin_weather_       = org_w_\n")
        report.append("  destination_weather_  = dst_w_\n")
        report.append("  feature_              = f_\n")

        // Write report to file using Hadoop FileSystem
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val reportPathObj = new org.apache.hadoop.fs.Path(reportPath)
        val out = fs.create(reportPathObj, true)
        val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
        try {
          writer.write(report.toString)
          info(s"   Feature importances report saved to: $reportPath")
        } finally {
          writer.close()
        }

        // Log to MLFlow
        runId.foreach { rid =>
          MLFlowTracker.logFeatureImportances(
            rid,
            featureNames,
            importances,
            topN = Some(20)
          )
          info(s"   Feature importances logged to MLFlow (top 20)")
        }
      }
    } catch {
      case ex: Exception =>
        info(s"   Warning: Could not save feature importances report: ${ex.getMessage}")
    }

    info(s"   Reloading model for evaluation to avoid broadcast...")
    val reloadedModel = PipelineModel.load(modelPath)

    info(s"   Evaluating on hold-out test set...")
    val testPredictions = reloadedModel.transform(testData)
    val holdOutMetrics = ModelEvaluator.evaluate(predictions=testPredictions, datasetType="[Hold-out Test] ")

    if (timeTracker != null) timeTracker.endStep("ml_evaluation")

    info(f"  Hold-out Test Metrics:")
    info(f"    Accuracy:  ${holdOutMetrics.accuracy * 100}%6.2f%%")
    info(f"    Precision: ${holdOutMetrics.precision * 100}%6.2f%%")
    info(f"    Recall:    ${holdOutMetrics.recall * 100}%6.2f%%")
    info(f"    F1-Score:  ${holdOutMetrics.f1Score * 100}%6.2f%%")
    info(f"    AUC-ROC:   ${holdOutMetrics.areaUnderROC}%6.4f")

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
    info("[STEP 7] Saving Metrics")
    info("=" * 80)
    info("   Model already saved in Step 5 (to avoid broadcast OOM)")

    if (timeTracker != null) timeTracker.startStep("ml_save_metrics")

    // Save comprehensive metrics (CSV + TXT summary)
    saveMetrics(experiment, cvResult, holdOutMetrics, testPredictions, experimentOutputPath, fast)

    val endTime = System.currentTimeMillis()
    val totalTime = (endTime - startTime) / 1000.0

    // Save training summary as TXT file
    saveTrainingSummary(experiment, cvResult, holdOutMetrics, totalTime, experimentOutputPath, fast)

    if (timeTracker != null) timeTracker.endStep("ml_save_metrics")

    // Calculate and set totals BEFORE saving to files
    if (timeTracker != null) {
      // ML Feature Extraction total
      val mlFeatureExtractionTotal = Seq(
        timeTracker.getStepTime("ml_feature_extraction.dev"),
        timeTracker.getStepTime("ml_feature_extraction.test")
      ).flatten.filterNot(_.isNaN).sum
      timeTracker.setStepTime("ml_feature_extraction.total", mlFeatureExtractionTotal)

      // ML total - only sum high-level steps (no double counting)
      val mlTotal = Seq(
        timeTracker.getStepTime("ml_feature_extraction.total"),
        timeTracker.getStepTime("ml_kfold_cv"), // Already includes Grid Search
        timeTracker.getStepTime("ml_train"),
        timeTracker.getStepTime("ml_evaluation"),
        timeTracker.getStepTime("ml_save_metrics")
      ).flatten.filterNot(_.isNaN).sum
      timeTracker.setStepTime("ml.total", mlTotal)
    }

    // Copy HDFS to local if localPath is configured (for visualization and MLFlow)
    val localExperimentPath = HDFSHelper.copyExperimentMetrics(
      spark,
      experimentOutputPath,
      configuration.common.output.localPath,
      experiment.name
    )

    // Generate visualization plots using local path (or HDFS if no localPath)
    val metricsPath = s"$localExperimentPath/metrics"
    generatePlots(metricsPath)

    info(f"   Total pipeline time: $totalTime%.2f seconds")

    // Log training time and artifacts to MLFlow
    runId.foreach { rid =>
      MLFlowTracker.logMetric(rid, "training_time_seconds", totalTime)

      // Use local paths for MLFlow artifact logging (already copied from HDFS if needed)
      val localModelPath = s"$localExperimentPath/models/${experiment.model.modelType}_final"

      // Log artifacts organized in subdirectories
      // 1. Log metrics CSVs to "metrics/" subdirectory
      MLFlowTracker.logArtifactWithPath(rid, metricsPath, "metrics")

      // 2. Log model to "models/" subdirectory
      MLFlowTracker.logArtifactWithPath(rid, localModelPath, "models")

      // 3. Log YAML configuration to "configuration/" subdirectory
      val configPath = s"${configuration.environment}-config.yml"
      val configSourcePath = getClass.getClassLoader.getResource(configPath)
      if (configSourcePath != null) {
        // Copy config to HDFS experiment output
        val configDestPathHDFS = s"$experimentOutputPath/configuration"
        val configDestFileHDFS = s"$configDestPathHDFS/${configuration.environment}-config.yml"

        val source = scala.io.Source.fromURL(configSourcePath, "UTF-8")
        val configContent = source.mkString
        source.close()

        // Write using Hadoop FileSystem (HDFS-compatible)
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val configDirPath = new Path(configDestPathHDFS)
        if (!fs.exists(configDirPath)) {
          fs.mkdirs(configDirPath)
        }
        val configFilePath = new Path(configDestFileHDFS)
        val out = fs.create(configFilePath, true)
        val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
        try {
          writer.write(configContent)
        } finally {
          writer.close()
        }

        // Log configuration from local path (already copied from HDFS)
        val localConfigDestPath = s"$localExperimentPath/configuration"
        MLFlowTracker.logArtifactWithPath(rid, localConfigDestPath, "configuration")
        info(s"   Configuration saved to MLFlow: configuration/${configuration.environment}-config.yml")
      }

      // 4. Log feature files to "features/" subdirectory
      val localFeaturesPath = s"$localExperimentPath/features"
      val featuresDir = new java.io.File(localFeaturesPath)
      if (featuresDir.exists() && featuresDir.isDirectory) {
        MLFlowTracker.logArtifactWithPath(rid, localFeaturesPath, "features")
        info(s"   Feature files saved to MLFlow: features/")
      }

      // 5. Log visualization plots to "plots/" subdirectory
      val plotsPath = s"$metricsPath/plots-ml-pipeline"
      val plotsDir = new java.io.File(plotsPath)
      if (plotsDir.exists() && plotsDir.isDirectory) {
        MLFlowTracker.logArtifactWithPath(rid, plotsPath, "plots")
        info(s"   Visualization plots saved to MLFlow: plots/")
      }

      // 6. Log execution time metrics if tracker is provided
      if (timeTracker != null) {
        // Save execution time metrics to files
        val execTimeBasePath = s"$localExperimentPath/execution_time"
        val execTimeCsvPath = s"$execTimeBasePath/execution_times.csv"
        val execTimeTxtPath = s"$execTimeBasePath/execution_times.txt"

        // Save files (directory creation handled inside saveToCSV/saveToText with Hadoop FileSystem API)
        timeTracker.saveToCSV(execTimeCsvPath)
        timeTracker.saveToText(execTimeTxtPath)

        // Log to MLFlow
        MLFlowTracker.logArtifactWithPath(rid, execTimeBasePath, "execution_time")
        info(s"   Execution time metrics saved to MLFlow: execution_time/")

        // Also log individual execution time metrics as MLFlow metrics
        timeTracker.getAllTimes.foreach { case (stepName, time) =>
          if (!time.isNaN) {
            val metricName = s"exec_time_${stepName.replace(".", "_")}"
            MLFlowTracker.logMetric(rid, metricName, time)
          }
        }
        info(s"   Execution time metrics logged to MLFlow as individual metrics")
      }
    }

    // ========================================================================
    // Display Best Model Summary
    // ========================================================================
    displayBestModelSummary(experiment, cvResult, holdOutMetrics, totalTime, fast)

    info("=" * 100)
    info(s"[ML PIPELINE] Completed for experiment: ${experiment.name}")
    info("=" * 100)

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
    totalTime: Double,
    fast: Boolean = false
  )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    info("=" * 100)
    info("BEST MODEL SUMMARY")
    info("=" * 100)

    info(s"\n Experiment: ${experiment.name}")
    info(s"   ${experiment.description}")

    info(s"\n Model Type: ${experiment.model.modelType.toUpperCase}")

    // Display best hyperparameters
    info(s"\n  Best Hyperparameters:")
    if (cvResult.bestHyperparameters.nonEmpty) {
      cvResult.bestHyperparameters.toSeq.sortBy(_._1).foreach { case (param, value) =>
        info(s"   - ${param.padTo(25, ' ')} : $value")
      }
    } else {
      // Display default hyperparameters from config
      val hp = experiment.model.hyperparameters
      info(s"   - ${"numTrees".padTo(25, ' ')} : ${hp.numTrees.head}")
      info(s"   - ${"maxDepth".padTo(25, ' ')} : ${hp.maxDepth.head}")
      info(s"   - ${"maxBins".padTo(25, ' ')} : ${hp.maxBins}")
      info(s"   - ${"minInstancesPerNode".padTo(25, ' ')} : ${hp.minInstancesPerNode}")
      info(s"   - ${"subsamplingRate".padTo(25, ' ')} : ${hp.subsamplingRate}")
      info(s"   - ${"featureSubsetStrategy".padTo(25, ' ')} : ${hp.featureSubsetStrategy}")
      info(s"   - ${"impurity".padTo(25, ' ')} : ${hp.impurity}")
    }

    // Display performance metrics
    info(s" Performance Metrics:")
    if (!fast) {
      info(s"   Cross-Validation (${cvResult.numFolds}-fold):")
      info(f"     Accuracy  : ${cvResult.avgMetrics.accuracy * 100}%6.2f%% ± ${cvResult.stdMetrics.accuracy * 100}%5.2f%%")
      info(f"     Precision : ${cvResult.avgMetrics.precision * 100}%6.2f%% ± ${cvResult.stdMetrics.precision * 100}%5.2f%%")
      info(f"     Recall    : ${cvResult.avgMetrics.recall * 100}%6.2f%% ± ${cvResult.stdMetrics.recall * 100}%5.2f%%")
      info(f"     F1-Score  : ${cvResult.avgMetrics.f1Score * 100}%6.2f%% ± ${cvResult.stdMetrics.f1Score * 100}%5.2f%%")
      info(f"     AUC-ROC   : ${cvResult.avgMetrics.areaUnderROC}%6.4f ± ${cvResult.stdMetrics.areaUnderROC}%6.4f")
    } else {
      info(s"   Cross-Validation: SKIPPED (fast mode)")
    }

    info(s"\n   Hold-out Test Set:")
    info(f"     Accuracy  : ${holdOutMetrics.accuracy * 100}%6.2f%%")
    info(f"     Precision : ${holdOutMetrics.precision * 100}%6.2f%%")
    info(f"     Recall    : ${holdOutMetrics.recall * 100}%6.2f%%")
    info(f"     F1-Score  : ${holdOutMetrics.f1Score * 100}%6.2f%%")
    info(f"     AUC-ROC   : ${holdOutMetrics.areaUnderROC}%6.4f")
    info(f"     RECd      : ${holdOutMetrics.recallDelayed * 100}%6.2f%%  (Recall Delayed)")
    info(f"     RECo      : ${holdOutMetrics.recallOnTime * 100}%6.2f%%  (Recall On-time)")

    // Display confusion matrix
    info(s"\n   Confusion Matrix (Test Set):")
    info(f"     True Positives  : ${holdOutMetrics.truePositives}%,d")
    info(f"     True Negatives  : ${holdOutMetrics.trueNegatives}%,d")
    info(f"     False Positives : ${holdOutMetrics.falsePositives}%,d")
    info(f"     False Negatives : ${holdOutMetrics.falseNegatives}%,d")

    info(f"\n  Total Training Time: $totalTime%.2f seconds")

    info("\n" + "=" * 100)
  }

  /**
   * Save all metrics (CV + hold-out) to CSV files
   */
  private def saveMetrics(
    experiment: ExperimentConfig,
    cvResult: CrossValidator.CVResult,
    holdOutMetrics: EvaluationMetrics,
    testPredictions: DataFrame,
    basePath: String,
    fast: Boolean = false
  )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    import com.flightdelay.utils.MetricsWriter
    import org.apache.spark.sql.functions._

    val metricsPath = s"$basePath/metrics"

    // Save CV fold metrics (only if not in fast mode)
    if (!fast) {
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
    }

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

    info(s"   Metrics saved to: $metricsPath")
    info(s"\n  [Visualization] To visualize ml pipeline metrics, run:")
    info(s"    python work/scripts/visualize_ml_pipeline.py $metricsPath")
  }

  /**
   * Save training summary as a readable TXT file
   */
  private def saveTrainingSummary(
    experiment: ExperimentConfig,
    cvResult: CrossValidator.CVResult,
    holdOutMetrics: EvaluationMetrics,
    totalTime: Double,
    basePath: String,
    fast: Boolean = false
  )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
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
    if (fast) {
      summary.append("SKIPPED (fast mode enabled)\n")
      summary.append("Using default hyperparameters from configuration\n\n")
    } else {
      summary.append(s"Number of Folds: ${cvResult.numFolds}\n\n")
      summary.append(f"  Metric       Mean          Std Dev\n")
      summary.append(f"  ${"=" * 50}\n")
      summary.append(f"  Accuracy     ${cvResult.avgMetrics.accuracy * 100}%6.2f%%      ± ${cvResult.stdMetrics.accuracy * 100}%5.2f%%\n")
      summary.append(f"  Precision    ${cvResult.avgMetrics.precision * 100}%6.2f%%      ± ${cvResult.stdMetrics.precision * 100}%5.2f%%\n")
      summary.append(f"  Recall       ${cvResult.avgMetrics.recall * 100}%6.2f%%      ± ${cvResult.stdMetrics.recall * 100}%5.2f%%\n")
      summary.append(f"  F1-Score     ${cvResult.avgMetrics.f1Score * 100}%6.2f%%      ± ${cvResult.stdMetrics.f1Score * 100}%5.2f%%\n")
      summary.append(f"  AUC-ROC      ${cvResult.avgMetrics.areaUnderROC}%6.4f       ± ${cvResult.stdMetrics.areaUnderROC}%6.4f\n")
      summary.append("\n")
    }

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
    summary.append("CONFUSION MATRIX (Test Set)")
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

    // Write to file using Hadoop FileSystem (HDFS-compatible)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val metricsDirPath = new Path(metricsPath)
    if (!fs.exists(metricsDirPath)) {
      fs.mkdirs(metricsDirPath)
    }
    
    val summaryPath = new Path(summaryFile)
    val out = fs.create(summaryPath, true)
    val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
    try {
      writer.write(summary.toString())
      println(s"   Training summary saved to: $summaryFile")
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
  private def generatePlots(metricsPath: String)(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    import scala.sys.process._
    import scala.util.{Try, Success, Failure}

    info("[STEP 8] Generating Visualization Plots")
    info("=" * 80)

    // Path to Python script (configurable, defaults to /scripts for Docker)
    val scriptsBasePath = configuration.common.scriptsPath
    val scriptPath = s"$scriptsBasePath/visualize_ml_pipeline.py"

    info(s"   Looking for visualization script at: $scriptPath")

    // Check if script exists
    val scriptFile = new java.io.File(scriptPath)
    if (!scriptFile.exists()) {
      warn(s"   Warning: Visualization script not found: $scriptPath")
      warn(s"   Skipping plot generation")
      return
    }

    // Execute Python script
    val command = s"python3 $scriptPath $metricsPath"
    info(s"   Running: $command")

    Try {
      val exitCode = command.!
      exitCode
    } match {
      case Success(0) =>
        info(s"   Plots generated successfully in: $metricsPath/plots-ml-pipeline/")
      case Success(exitCode) =>
        warn(s"   Warning: Plot generation failed with exit code: $exitCode")
        warn(s"   Continuing without plots...")
      case Failure(e) =>
        warn(s"   Warning: Could not execute Python script: ${e.getMessage}")
        warn(s"   Make sure Python 3 and required libraries are installed")
        warn(s"   Continuing without plots...")
    }
  }
}
