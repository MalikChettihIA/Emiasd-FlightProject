package com.flightdelay.ml.training

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import com.flightdelay.ml.evaluation.ModelEvaluator
import com.flightdelay.ml.evaluation.ModelEvaluator.EvaluationMetrics
import com.flightdelay.ml.models.ModelFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * CrossValidator - K-Fold Cross-Validation with optional Grid Search
 *
 * Performs robust model evaluation and hyperparameter tuning:
 * 1. K-fold split of development data
 * 2. Optional Grid Search over hyperparameter space
 * 3. Returns best hyperparameters and CV metrics
 */
object CrossValidator {

  /**
   * Result of cross-validation
   *
   * @param avgMetrics Average metrics across all folds
   * @param stdMetrics Standard deviation of metrics across folds
   * @param foldMetrics Metrics for each individual fold
   * @param bestHyperparameters Best hyperparameters from grid search (empty if grid search disabled)
   * @param numFolds Number of folds used
   */
  case class CVResult(
    avgMetrics: EvaluationMetrics,
    stdMetrics: EvaluationMetrics,
    foldMetrics: Seq[EvaluationMetrics],
    bestHyperparameters: Map[String, Any],
    numFolds: Int
  )

  /**
   * Perform K-fold cross-validation with optional grid search
   *
   * @param devData Development data (80% of original data)
   * @param experiment Experiment configuration
   * @param spark Implicit SparkSession
   * @param config Implicit AppConfiguration
   * @return CVResult with metrics and best hyperparameters
   */
  def validate(
    devData: DataFrame,
    experiment: ExperimentConfig
  )(implicit spark: SparkSession, config: AppConfiguration): CVResult = {

    val numFolds = experiment.train.crossValidation.numFolds

    println(s"\n[CrossValidator] Starting K-Fold Cross-Validation")
    println(s"  - Number of folds: $numFolds")
    println(s"  - Grid Search: ${if (experiment.train.gridSearch.enabled) "ENABLED" else "DISABLED"}")

    if (experiment.train.gridSearch.enabled) {
      validateWithGridSearch(devData, experiment, numFolds)
    } else {
      validateSimple(devData, experiment, numFolds)
    }
  }

  /**
   * Simple K-fold CV without grid search
   */
  private def validateSimple(
    devData: DataFrame,
    experiment: ExperimentConfig,
    numFolds: Int
  )(implicit spark: SparkSession, config: AppConfiguration): CVResult = {

    println(s"\n[K-Fold CV] Performing $numFolds-fold cross-validation...")

    // Add fold index column
    val dataWithFold = devData.withColumn("fold", (rand(config.common.seed) * numFolds).cast("int"))

    // Perform K-fold CV
    val foldMetrics = (0 until numFolds).map { foldIdx =>
      println(s"\n  --- Fold ${foldIdx + 1}/$numFolds ---")

      // Split data
      val trainFold = dataWithFold.filter(col("fold") =!= foldIdx).drop("fold")
      val valFold = dataWithFold.filter(col("fold") === foldIdx).drop("fold")

      val trainCount = trainFold.count()
      val valCount = valFold.count()
      println(f"    Train: $trainCount%,d | Validation: $valCount%,d")

      // Create and train model
      val model = ModelFactory.create(experiment)
      val trainedModel = model.train(trainFold)

      // Evaluate on validation fold
      val valPredictions = trainedModel.transform(valFold)
      val metrics = ModelEvaluator.evaluate(valPredictions)

      println(f"    Val Metrics: Acc=${metrics.accuracy * 100}%.2f%% | F1=${metrics.f1Score * 100}%.2f%% | AUC=${metrics.areaUnderROC}%.4f")

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
   * K-fold CV with Grid Search for hyperparameter tuning
   *
   * For each hyperparameter combination:
   * 1. Perform K-fold CV
   * 2. Calculate average validation metric
   * 3. Select best combination based on evaluation metric
   */
  private def validateWithGridSearch(
    devData: DataFrame,
    experiment: ExperimentConfig,
    numFolds: Int
  )(implicit spark: SparkSession, config: AppConfiguration): CVResult = {

    println(s"\n[Grid Search] Building parameter grid...")

    // Build parameter grid from experiment config
    val paramGrid = buildParameterGrid(experiment)

    println(s"  - Total combinations: ${paramGrid.size}")
    println(s"  - Evaluation metric: ${experiment.train.gridSearch.evaluationMetric}")

    // For each parameter combination, perform K-fold CV
    val gridResults = paramGrid.zipWithIndex.map { case (params, idx) =>
      println(s"\n[Grid Search] Testing combination ${idx + 1}/${paramGrid.size}")
      params.foreach { case (k, v) => println(s"    $k: $v") }

      // Perform K-fold CV with these params
      val cvResult = validateWithParams(devData, experiment, params, numFolds)

      // Get metric value for comparison
      val metricValue = getMetricValue(cvResult.avgMetrics, experiment.train.gridSearch.evaluationMetric)

      println(f"    → Avg ${experiment.train.gridSearch.evaluationMetric}: $metricValue%.4f")

      (params, cvResult, metricValue)
    }

    // Select best combination based on evaluation metric
    val (bestParams, bestCVResult, bestMetricValue) = gridResults.maxBy(_._3)

    println(s"\n" + "=" * 80)
    println("[Grid Search] BEST COMBINATION FOUND")
    println("=" * 80)
    bestParams.toSeq.sortBy(_._1).foreach { case (k, v) =>
      println(f"  $k%-25s : $v")
    }
    println(f"  Best ${experiment.train.gridSearch.evaluationMetric}%-25s : $bestMetricValue%.6f")
    println("=" * 80)

    bestCVResult.copy(bestHyperparameters = bestParams)
  }

  /**
   * Perform K-fold CV with specific hyperparameters
   */
  private def validateWithParams(
    devData: DataFrame,
    experiment: ExperimentConfig,
    params: Map[String, Any],
    numFolds: Int
  )(implicit spark: SparkSession, config: AppConfiguration): CVResult = {

    // Add fold index column
    val dataWithFold = devData.withColumn("fold", (rand(config.common.seed) * numFolds).cast("int"))

    // Perform K-fold CV
    val foldMetrics = (0 until numFolds).map { foldIdx =>
      // Split data
      val trainFold = dataWithFold.filter(col("fold") =!= foldIdx).drop("fold")
      val valFold = dataWithFold.filter(col("fold") === foldIdx).drop("fold")

      // Train with specific params
      val trainedModel = Trainer.trainWithParams(trainFold, experiment, params)

      // Evaluate
      val valPredictions = trainedModel.transform(valFold)
      ModelEvaluator.evaluate(valPredictions)
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
   * Build parameter grid from experiment configuration
   * Creates cartesian product of all hyperparameter arrays
   */
  private def buildParameterGrid(experiment: ExperimentConfig): Seq[Map[String, Any]] = {
    val hp = experiment.train.hyperparameters
    val modelType = experiment.model.modelType.toLowerCase

    modelType match {
      case "randomforest" | "rf" =>
        // Create combinations of ALL RandomForest hyperparameters
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
        // Create combinations of GBT hyperparameters
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
        // Create combinations of Logistic Regression hyperparameters
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

  /**
   * Calculate average and standard deviation of metrics across folds
   */
  private def calculateStatistics(
    foldMetrics: Seq[EvaluationMetrics]
  ): (EvaluationMetrics, EvaluationMetrics) = {

    val n = foldMetrics.length.toDouble

    // Calculate means
    val avgAccuracy = foldMetrics.map(_.accuracy).sum / n
    val avgPrecision = foldMetrics.map(_.precision).sum / n
    val avgRecall = foldMetrics.map(_.recall).sum / n
    val avgF1 = foldMetrics.map(_.f1Score).sum / n
    val avgAUC = foldMetrics.map(_.areaUnderROC).sum / n
    val avgAUPR = foldMetrics.map(_.areaUnderPR).sum / n

    // Calculate standard deviations
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

  /**
   * Get specific metric value by name
   */
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
