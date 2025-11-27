package com.flightdelay.ml.training

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import com.flightdelay.ml.evaluation.ModelEvaluator
import com.flightdelay.ml.evaluation.ModelEvaluator.EvaluationMetrics
import com.flightdelay.ml.models.ModelFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.{PipelineModel, Model}
import com.flightdelay.utils.MetricsUtils
import com.flightdelay.utils.DebugUtils._

object CrossValidator {

  case class CVResult(
                       avgMetrics: EvaluationMetrics,
                       stdMetrics: EvaluationMetrics,
                       foldMetrics: Seq[EvaluationMetrics],
                       bestHyperparameters: Map[String, Any],
                       numFolds: Int
                     )

  def validate(
                devData: DataFrame,
                experiment: ExperimentConfig
              )(implicit spark: SparkSession, config: AppConfiguration): CVResult = {

    val numFolds = experiment.train.crossValidation.numFolds

    // If numFolds <= 1, do simple train/test split instead of CV
    if (numFolds <= 1) {
      info(s"[CrossValidator] Simple Train/Test Mode (no cross-validation)")
      info(s"  - Using 80/20 train/validation split")
      info(s"  - Grid Search: ${if (experiment.train.gridSearch.enabled) "ENABLED" else "DISABLED"}")

      if (experiment.train.gridSearch.enabled) {
        return validateSimpleTrainTestWithGridSearch(devData, experiment)
      } else {
        return validateSimpleTrainTest(devData, experiment)
      }
    }

    info(s"[CrossValidator] Starting K-Fold Cross-Validation")
    info(s"  - Number of folds: $numFolds")
    info(s"  - Grid Search: ${if (experiment.train.gridSearch.enabled) "ENABLED" else "DISABLED"}")

    if (experiment.train.gridSearch.enabled) {
      validateWithGridSearch(devData, experiment, numFolds)
    } else {
      validateSimple(devData, experiment, numFolds)
    }
  }

  /**
   * Simple train/test split without cross-validation (for fast debugging)
   * Used when numFolds <= 1 and gridSearch = false
   */
  private def validateSimpleTrainTest(
                                       devData: DataFrame,
                                       experiment: ExperimentConfig
                                     )(implicit spark: SparkSession, config: AppConfiguration): CVResult = {

    val trainRatio = experiment.train.trainRatio
    val testRatio = 1.0 - trainRatio
    info(s"[Simple Train/Test] Performing single train/validation split (${trainRatio * 100}%/${testRatio * 100}%)...")

    // Split data using configured trainRatio
    val Array(trainDataTemp, valDataTemp) = devData.randomSplit(Array(trainRatio, testRatio), seed = config.common.seed)

    // Cache data to avoid DAG recomputation
    val trainData = trainDataTemp.cache()
    val valData = valDataTemp.cache()

    val trainCount = trainData.count()  // Materialize cache
    val valCount = valData.count()      // Materialize cache
    info(f"  Train: $trainCount%,d samples | Validation: $valCount%,d samples")
    info(s"  Train/Val data cached")

    // Train model
    val model = ModelFactory.create(experiment)
    info(s"  Training ${experiment.model.modelType} model...")
    val trainedModel = model.train(trainData)

    // SOLUTION: Save and reload model to avoid broadcast OOM
    val tempModelPath = s"${config.common.output.basePath}/${experiment.name}/model/temp_simple_${System.currentTimeMillis()}"

    val metrics = try {
      trainedModel match {
        case pm: PipelineModel =>
          info(s"  Saving model to avoid broadcast OOM...")
          pm.write.overwrite().save(tempModelPath)
          info(s"  Reloading model from disk...")
          val reloadedModel = PipelineModel.load(tempModelPath)

          // Evaluate on validation set with reloaded model
          info(s"  Evaluating on validation set...")
          val predictions = reloadedModel.transform(valData)
          ModelEvaluator.evaluate(predictions, None, "validation")

        case _ =>
          // Fallback: evaluate directly (may cause broadcast issues)
          info(s"  Model is not PipelineModel, evaluating directly (may cause OOM)")
          info(s"  Evaluating on validation set...")
          val predictions = trainedModel.transform(valData)
          ModelEvaluator.evaluate(predictions, None, "validation")
      }
    } finally {
      cleanupTempModel(tempModelPath)
    }

    info(f"  Validation Results:")
    info(f"    Accuracy:  ${metrics.accuracy * 100}%6.2f%%")
    info(f"    Precision: ${metrics.precision * 100}%6.2f%%")
    info(f"    Recall:    ${metrics.recall * 100}%6.2f%%")
    info(f"    F1-Score:  ${metrics.f1Score * 100}%6.2f%%")
    info(f"    AUC-ROC:   ${metrics.areaUnderROC}%6.4f")

    // No hyperparameter search in simple mode - use default hyperparameters from config
    val bestHyperparameters = Map.empty[String, Any]

    // Unpersist cached data to free memory
    trainData.unpersist()
    valData.unpersist()

    // Return CVResult with single fold (std = 0)
    val zeroMetrics = EvaluationMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    CVResult(
      avgMetrics = metrics,
      stdMetrics = zeroMetrics,  // No std deviation with single split
      foldMetrics = Seq(metrics),
      bestHyperparameters = bestHyperparameters,
      numFolds = 1
    )
  }

  /**
   * Grid search with simple train/test split (no K-fold CV)
   * Used when numFolds <= 1 and gridSearch = true
   * Much faster than K-fold grid search: tests each combo only ONCE
   */
  private def validateSimpleTrainTestWithGridSearch(
                                                     devData: DataFrame,
                                                     experiment: ExperimentConfig
                                                   )(implicit spark: SparkSession, config: AppConfiguration): CVResult = {

    info(s"[Grid Search] Building parameter grid...")
    val paramGrid = buildParameterGrid(experiment)

    val trainRatio = experiment.train.trainRatio
    val testRatio = 1.0 - trainRatio

    info(s"  - Total combinations: ${paramGrid.size}")
    info(s"  - Evaluation metric: ${experiment.train.gridSearch.evaluationMetric}")
    info(s"  - Mode: Simple train/validation split (NO K-fold)")
    info(f"  - Split ratio: ${trainRatio * 100}%.0f%% train / ${testRatio * 100}%.0f%% validation")

    // Split data using configured trainRatio
    val Array(trainDataTemp, valDataTemp) = devData.randomSplit(Array(trainRatio, testRatio), seed = config.common.seed)

    // CRITICAL: Cache train/val data to avoid recomputing DAG for each hyperparameter combo
    val trainData = trainDataTemp.cache()
    val valData = valDataTemp.cache()

    val trainCount = trainData.count()  // Materialize cache
    val valCount = valData.count()      // Materialize cache
    info(f"  - Train: $trainCount%,d samples | Validation: $valCount%,d samples")
    info(s"  - Train/Val data cached to avoid DAG recomputation")

    // Test all combinations on this single split
    val gridResults = paramGrid.zipWithIndex.map { case (params, idx) =>
      info(s"[Grid Search] Testing combination ${idx + 1}/${paramGrid.size}")
      params.foreach { case (k, v) => info(s"    $k: $v") }

      // Train with specific params
      val trainedModel = Trainer.trainWithParams(trainData, experiment, params)

      // SOLUTION: Save and reload model to avoid broadcast OOM
      val tempModelPath = s"${config.common.output.basePath}/${experiment.name}/model/temp_gridsearch_${idx}_${System.currentTimeMillis()}"

      val metrics = try {
        trainedModel match {
          case pm: PipelineModel =>
            pm.write.overwrite().save(tempModelPath)
            val reloadedModel = PipelineModel.load(tempModelPath)

            // Evaluate on validation set with reloaded model
            val predictions = reloadedModel.transform(valData)
            val evaluationMetrics = ModelEvaluator.evaluate(predictions, None, "validation")

            val metricValue = getMetricValue(evaluationMetrics, experiment.train.gridSearch.evaluationMetric)
            info(f"    → ${experiment.train.gridSearch.evaluationMetric}: $metricValue%.4f")

            evaluationMetrics

          case _ =>
            // Fallback: evaluate directly (may cause broadcast issues)
            info(s"    Model is not PipelineModel, evaluating directly (may cause OOM)")
            val predictions = trainedModel.transform(valData)
            val evaluationMetrics = ModelEvaluator.evaluate(predictions, None, "validation")

            val metricValue = getMetricValue(evaluationMetrics, experiment.train.gridSearch.evaluationMetric)
            info(f"    → ${experiment.train.gridSearch.evaluationMetric}: $metricValue%.4f")

            evaluationMetrics
        }
      } finally {
        cleanupTempModel(tempModelPath)
      }

      val metricValue = getMetricValue(metrics, experiment.train.gridSearch.evaluationMetric)
      (params, metrics, metricValue)
    }

    // Find best combination
    val (bestParams, bestMetrics, bestMetricValue) = gridResults.maxBy(_._3)

    info(s"=" * 80)
    info("[Grid Search] BEST COMBINATION FOUND")
    info("=" * 80)
    bestParams.toSeq.sortBy(_._1).foreach { case (k, v) =>
      info(f"  $k%-25s : $v")
    }
    info(f"  Best ${experiment.train.gridSearch.evaluationMetric}%-25s : $bestMetricValue%.6f")
    info("=" * 80)

    // Unpersist cached data to free memory
    trainData.unpersist()
    valData.unpersist()
    info(s"  - Train/Val data unpersisted to free memory")

    // Return CVResult with single fold (std = 0)
    val zeroMetrics = EvaluationMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    CVResult(
      avgMetrics = bestMetrics,
      stdMetrics = zeroMetrics,  // No std deviation with single split
      foldMetrics = Seq(bestMetrics),
      bestHyperparameters = bestParams,
      numFolds = 1
    )
  }

  /**
   * Simple K-fold CV without grid search
   */
  private def validateSimple(
                              devData: DataFrame,
                              experiment: ExperimentConfig,
                              numFolds: Int
                            )(implicit spark: SparkSession, config: AppConfiguration): CVResult = {

    info(s"[K-Fold CV] Performing $numFolds-fold cross-validation...")

    // Add fold index column
    val dataWithFold = devData.withColumn("fold", (rand(config.common.seed) * numFolds).cast("int"))

    // Perform K-fold CV
    val foldMetrics = (0 until numFolds).map { foldIdx =>
      info(s"  --- Fold ${foldIdx + 1}/$numFolds ---")

      // Split data
      val trainFold = dataWithFold.filter(col("fold") =!= foldIdx).drop("fold")
      val valFold = dataWithFold.filter(col("fold") === foldIdx).drop("fold")

      whenDebug{
        val trainCount = trainFold.count()
        val valCount = valFold.count()
        info(f"    Train: $trainCount%,d | Validation: $valCount%,d")
      }

      // Create and train model
      val model = ModelFactory.create(experiment)
      val trainedModel = model.train(trainFold)

      // ✅ SOLUTION : Sauvegarder et recharger le modèle pour éviter le broadcast
      val tempModelPath = s"${config.common.output.basePath}/${experiment.name}/model/temp_cv_fold_${foldIdx}_${System.currentTimeMillis()}"

      val metrics = try {
        info(s"    Saving model to avoid broadcast...")

        // ✅ Cast en PipelineModel pour accéder à write
        trainedModel match {
          case pm: PipelineModel =>
            pm.write.overwrite().save(tempModelPath)
            info(s"     Reloading model from disk...")
            val reloadedModel = PipelineModel.load(tempModelPath)

            // Evaluate on validation fold with reloaded model
            info(s"     Evaluating on validation fold...")
            val valPredictions = reloadedModel.transform(valFold)
            val evaluationMetrics = ModelEvaluator.evaluate(predictions = valPredictions, datasetType = "[No K-Fold CV]")

            info(f"     Val Metrics: Acc=${evaluationMetrics.accuracy * 100}%.2f%% | F1=${evaluationMetrics.f1Score * 100}%.2f%% | AUC=${evaluationMetrics.areaUnderROC}%.4f")

            evaluationMetrics

          case _ =>
            // Si ce n'est pas un PipelineModel, évaluer directement (risque de broadcast)
            info(s"      Model is not PipelineModel, evaluating directly (may cause broadcast issues)")
            val valPredictions = trainedModel.transform(valFold)
            val evaluationMetrics = ModelEvaluator.evaluate(predictions = valPredictions, datasetType = "[No K-Fold CV]")

            info(f"     Val Metrics: Acc=${evaluationMetrics.accuracy * 100}%.2f%% | F1=${evaluationMetrics.f1Score * 100}%.2f%% | AUC=${evaluationMetrics.areaUnderROC}%.4f")

            evaluationMetrics
        }

      } finally {
        // Cleanup temp model
        cleanupTempModel(tempModelPath)
      }

      metrics
    }

    // Calculate average and std metrics
    val (avgMetrics, stdMetrics) = calculateStatistics(foldMetrics)

    CVResult(
      avgMetrics = avgMetrics,
      stdMetrics = stdMetrics,
      foldMetrics = foldMetrics,
      bestHyperparameters = Map.empty,
      numFolds = numFolds
    )
  }

  /**
   * K-fold CV with specific hyperparameters
   * ✅ MODIFIÉ : Sauvegarde/rechargement du modèle pour éviter le broadcast
   */
  private def validateWithParams(
                                  devData: DataFrame,
                                  experiment: ExperimentConfig,
                                  params: Map[String, Any],
                                  numFolds: Int
                                )(implicit spark: SparkSession, config: AppConfiguration): CVResult = {

    // Add fold index column
    val dataWithFold = devData.withColumn("fold", (rand(config.common.seed) * numFolds).cast("int"))
    info(s"[CrossValidator][numFolds] ${numFolds}")
    // Perform K-fold CV
    val foldMetrics = (0 until numFolds).map { foldIdx =>
      info(s"[CrossValidator][foldIdx] ${foldIdx}")
      // Split data
      val trainFold = dataWithFold.filter(col("fold") =!= foldIdx).drop("fold")
      val valFold = dataWithFold.filter(col("fold") === foldIdx).drop("fold")
      debug(s"[CrossValidator][trainFold] ${trainFold.count()}")
      debug(s"[CrossValidator][valFold] ${valFold.count()}")

      // Train with specific params
      val trainedModel = Trainer.trainWithParams(trainFold, experiment, params)

      // SOLUTION : Sauvegarder et recharger le modèle pour éviter le broadcast
      val tempModelPath = s"${config.common.output.basePath}/${experiment.name}/model/temp_cv_fold_${foldIdx}_${System.currentTimeMillis()}"

      val metrics = try {
        // Cast en PipelineModel pour accéder à write
        trainedModel match {
          case pm: PipelineModel =>
            pm.write.overwrite().save(tempModelPath)
            val reloadedModel = PipelineModel.load(tempModelPath)

            // Evaluate
            val valPredictions = reloadedModel.transform(valFold)
            ModelEvaluator.evaluate(predictions = valPredictions, datasetType = s"[K-Fold CV] [FoldIndex ${foldIdx}]")

          case _ =>
            // Fallback : évaluer directement
            val valPredictions = trainedModel.transform(valFold)
            ModelEvaluator.evaluate(predictions = valPredictions, datasetType = s"[K-Fold CV] [FoldIndex ${foldIdx}]")
        }

      } finally {
        cleanupTempModel(tempModelPath)
      }

      metrics
    }

    // Calculate statistics
    val (avgMetrics, stdMetrics) = calculateStatistics(foldMetrics)

    CVResult(
      avgMetrics = avgMetrics,
      stdMetrics = stdMetrics,
      foldMetrics = foldMetrics,
      bestHyperparameters = params,
      numFolds = numFolds
    )
  }

  /**
   * Fonction helper pour nettoyer les modèles temporaires
   */
  private def cleanupTempModel(modelPath: String)(implicit spark: SparkSession, config: AppConfiguration): Unit = {
    try {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val path = new Path(modelPath)
      if (fs.exists(path)) {
        fs.delete(path, true)
        info(s"    Cleaned up temp model: $modelPath")
      }
    } catch {
      case e: Exception =>
        info(s"      Warning: Could not delete temp model $modelPath: ${e.getMessage}")
    }
  }

  private def validateWithGridSearch(
                                      devData: DataFrame,
                                      experiment: ExperimentConfig,
                                      numFolds: Int
                                    )(implicit spark: SparkSession, config: AppConfiguration): CVResult = {

    info(s"[Grid Search] Building parameter grid...")

    val paramGrid = buildParameterGrid(experiment)

    info(s"  - Total combinations: ${paramGrid.size}")
    info(s"  - Evaluation metric: ${experiment.train.gridSearch.evaluationMetric}")

    val gridResults = paramGrid.zipWithIndex.map { case (params, idx) =>
      info(s"[Grid Search] Testing combination ${idx + 1}/${paramGrid.size}")
      params.foreach { case (k, v) => info(s"    $k: $v") }

      val cvResult = validateWithParams(devData, experiment, params, numFolds)
      val metricValue = getMetricValue(cvResult.avgMetrics, experiment.train.gridSearch.evaluationMetric)

      info(f"    → Avg ${experiment.train.gridSearch.evaluationMetric}: $metricValue%.4f")

      (params, cvResult, metricValue)
    }

    val (bestParams, bestCVResult, bestMetricValue) = gridResults.maxBy(_._3)

    info(s"=" * 80)
    info("[Grid Search] BEST COMBINATION FOUND")
    info("=" * 80)
    bestParams.toSeq.sortBy(_._1).foreach { case (k, v) =>
      info(f"  $k%-25s : $v")
    }
    info(f"  Best ${experiment.train.gridSearch.evaluationMetric}%-25s : $bestMetricValue%.6f")
    info("=" * 80)

    bestCVResult.copy(bestHyperparameters = bestParams)
  }

  private def buildParameterGrid(experiment: ExperimentConfig): Seq[Map[String, Any]] = {
    val hp = experiment.model.hyperparameters
    val modelType = experiment.model.modelType.toLowerCase

    modelType match {
      case "randomforest" | "rf" =>
        val numTreesValues = hp.numTrees.getOrElse(Seq(100))
        val maxDepthValues = hp.maxDepth.getOrElse(Seq(5))
        val maxBinsValues = hp.maxBins.getOrElse(Seq(32))
        val minInstancesPerNodeValues = hp.minInstancesPerNode.getOrElse(Seq(1))
        val subsamplingRateValues = hp.subsamplingRate.getOrElse(Seq(1.0))
        val featureSubsetStrategyValues = hp.featureSubsetStrategy.getOrElse(Seq("auto"))
        val impurityValue = hp.impurity.getOrElse("gini")

        val combinations = for {
          numTrees <- numTreesValues
          maxDepth <- maxDepthValues
          maxBins <- maxBinsValues
          minInstancesPerNode <- minInstancesPerNodeValues
          subsamplingRate <- subsamplingRateValues
          featureSubsetStrategy <- featureSubsetStrategyValues
        } yield Map[String, Any](
          "numTrees" -> numTrees,
          "maxDepth" -> maxDepth,
          "maxBins" -> maxBins,
          "minInstancesPerNode" -> minInstancesPerNode,
          "subsamplingRate" -> subsamplingRate,
          "featureSubsetStrategy" -> featureSubsetStrategy,
          "impurity" -> impurityValue
        )
        combinations

      case "gbt" | "gradientboostedtrees" =>
        val maxIterValues = hp.maxIter.getOrElse(Seq(100))
        val maxDepthValues = hp.maxDepth.getOrElse(Seq(5))
        val maxBinsValues = hp.maxBins.getOrElse(Seq(32))
        val minInstancesPerNodeValues = hp.minInstancesPerNode.getOrElse(Seq(1))
        val subsamplingRateValues = hp.subsamplingRate.getOrElse(Seq(1.0))
        val stepSizeValues = hp.stepSize.getOrElse(Seq(0.1))

        val combinations = for {
          maxIter <- maxIterValues
          maxDepth <- maxDepthValues
          maxBins <- maxBinsValues
          minInstancesPerNode <- minInstancesPerNodeValues
          subsamplingRate <- subsamplingRateValues
          stepSize <- stepSizeValues
        } yield Map[String, Any](
          "maxIter" -> maxIter,
          "maxDepth" -> maxDepth,
          "maxBins" -> maxBins,
          "minInstancesPerNode" -> minInstancesPerNode,
          "subsamplingRate" -> subsamplingRate,
          "stepSize" -> stepSize
        )
        combinations

      case "logisticregression" | "lr" =>
        val maxIterValues = hp.maxIter.getOrElse(Seq(100))
        val regParamValues = hp.regParam.getOrElse(Seq(0.0))
        val elasticNetParamValues = hp.elasticNetParam.getOrElse(Seq(0.0))

        val combinations = for {
          maxIter <- maxIterValues
          regParam <- regParamValues
          elasticNetParam <- elasticNetParamValues
        } yield Map[String, Any](
          "maxIter" -> maxIter,
          "regParam" -> regParam,
          "elasticNetParam" -> elasticNetParam
        )
        combinations

      case other =>
        throw new IllegalArgumentException(s"Unsupported model type in buildParameterGrid: $other")
    }
  }

  private def calculateStatistics(
                                   foldMetrics: Seq[EvaluationMetrics]
                                 ): (EvaluationMetrics, EvaluationMetrics) = {

    val n = foldMetrics.length.toDouble

    val avgAccuracy = foldMetrics.map(_.accuracy).sum / n
    val avgPrecision = foldMetrics.map(_.precision).sum / n
    val avgRecall = foldMetrics.map(_.recall).sum / n
    val avgF1 = foldMetrics.map(_.f1Score).sum / n
    val avgAUC = foldMetrics.map(_.areaUnderROC).sum / n
    val avgAUPR = foldMetrics.map(_.areaUnderPR).sum / n

    val stdAccuracy = math.sqrt(foldMetrics.map(m => math.pow(m.accuracy - avgAccuracy, 2)).sum / n)
    val stdPrecision = math.sqrt(foldMetrics.map(m => math.pow(m.precision - avgPrecision, 2)).sum / n)
    val stdRecall = math.sqrt(foldMetrics.map(m => math.pow(m.recall - avgRecall, 2)).sum / n)
    val stdF1 = math.sqrt(foldMetrics.map(m => math.pow(m.f1Score - avgF1, 2)).sum / n)
    val stdAUC = math.sqrt(foldMetrics.map(m => math.pow(m.areaUnderROC - avgAUC, 2)).sum / n)
    val stdAUPR = math.sqrt(foldMetrics.map(m => math.pow(m.areaUnderPR - avgAUPR, 2)).sum / n)

    val avgMetrics = EvaluationMetrics(
      accuracy = avgAccuracy,
      precision = avgPrecision,
      recall = avgRecall,
      f1Score = avgF1,
      areaUnderROC = avgAUC,
      areaUnderPR = avgAUPR,
      truePositives = 0L,
      trueNegatives = 0L,
      falsePositives = 0L,
      falseNegatives = 0L
    )

    val stdMetrics = EvaluationMetrics(
      accuracy = stdAccuracy,
      precision = stdPrecision,
      recall = stdRecall,
      f1Score = stdF1,
      areaUnderROC = stdAUC,
      areaUnderPR = stdAUPR,
      truePositives = 0L,
      trueNegatives = 0L,
      falsePositives = 0L,
      falseNegatives = 0L
    )

    (avgMetrics, stdMetrics)
  }

  private def getMetricValue(metrics: EvaluationMetrics, metricName: String): Double = {
    metricName.toLowerCase match {
      case "accuracy" => metrics.accuracy
      case "precision" => metrics.precision
      case "recall" => metrics.recall
      case "f1" | "f1_score" => metrics.f1Score
      case "auc" | "auc_roc" | "auroc" => metrics.areaUnderROC
      case "auc_pr" | "aupr" => metrics.areaUnderPR
      case unknown =>
        println(s"  ⚠ Unknown metric: $unknown, defaulting to F1")
        metrics.f1Score
    }
  }
}