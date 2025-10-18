package com.flightdelay.ml.training

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import com.flightdelay.ml.models.ModelFactory
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Trainer - Handles final model training on full development set
 *
 * Used after cross-validation to train the final model with best hyperparameters
 * on the full development set (before hold-out test evaluation).
 */
object Trainer {

  /**
   * Train final model on full development set with best hyperparameters
   *
   * This is called after K-fold CV + Grid Search to train the final model
   * that will be evaluated on the hold-out test set.
   *
   * @param devData Full development set (80% of original data)
   * @param experiment Experiment configuration
   * @param bestHyperparameters Best hyperparameters from CV/Grid Search
   * @param spark Implicit SparkSession
   * @param config Implicit AppConfiguration
   * @return Trained model
   */
  def trainFinal(
    devData: DataFrame,
    experiment: ExperimentConfig,
    bestHyperparameters: Map[String, Any]
  )(implicit spark: SparkSession, config: AppConfiguration): Transformer = {

    println(s"\n[Trainer] Training final model on full development set")
    println(s"  - Samples: ${devData.count()}")

    if (bestHyperparameters.nonEmpty) {
      println(s"  - Using best hyperparameters from Grid Search:")
      bestHyperparameters.foreach { case (param, value) =>
        println(s"      $param: $value")
      }
    }

    // Create model from factory
    val model = ModelFactory.create(experiment)

    // Train on full dev set
    val startTime = System.currentTimeMillis()
    val trainedModel = model.train(devData)
    val endTime = System.currentTimeMillis()
    val trainingTime = (endTime - startTime) / 1000.0

    println(f"  ✓ Final model trained in $trainingTime%.2f seconds")

    trainedModel
  }

  /**
   * Train model with specific hyperparameters (used internally by CrossValidator)
   *
   * @param data Training data
   * @param experiment Experiment configuration
   * @param hyperparameters Specific hyperparameters to use
   * @param spark Implicit SparkSession
   * @param config Implicit AppConfiguration
   * @return Trained model
   */
  def trainWithParams(
    data: DataFrame,
    experiment: ExperimentConfig,
    hyperparameters: Map[String, Any]
  )(implicit spark: SparkSession, config: AppConfiguration): Transformer = {

    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.classification.{RandomForestClassifier, GBTClassifier, LogisticRegression}

    val modelType = experiment.model.modelType.toLowerCase

    val classifier = modelType match {
      case "randomforest" | "rf" =>
        // Extract hyperparameters
        val numTrees = hyperparameters("numTrees").asInstanceOf[Int]
        val maxDepth = hyperparameters("maxDepth").asInstanceOf[Int]
        val maxBins = hyperparameters("maxBins").asInstanceOf[Int]
        val minInstancesPerNode = hyperparameters("minInstancesPerNode").asInstanceOf[Int]
        val subsamplingRate = hyperparameters("subsamplingRate").asInstanceOf[Double]
        val featureSubsetStrategy = hyperparameters("featureSubsetStrategy").asInstanceOf[String]
        val impurity = hyperparameters("impurity").asInstanceOf[String]

        // Create RandomForest with specific hyperparameters
        new RandomForestClassifier()
          .setLabelCol("label")
          .setFeaturesCol("features")
          .setPredictionCol("prediction")
          .setProbabilityCol("probability")
          .setRawPredictionCol("rawPrediction")
          .setNumTrees(numTrees)
          .setMaxDepth(maxDepth)
          .setMaxBins(maxBins)
          .setMinInstancesPerNode(minInstancesPerNode)
          .setFeatureSubsetStrategy(featureSubsetStrategy)
          .setImpurity(impurity)
          .setSubsamplingRate(subsamplingRate)

      case "gbt" | "gradientboostedtrees" =>
        // Extract hyperparameters
        val maxIter = hyperparameters("maxIter").asInstanceOf[Int]
        val maxDepth = hyperparameters("maxDepth").asInstanceOf[Int]
        val maxBins = hyperparameters("maxBins").asInstanceOf[Int]
        val minInstancesPerNode = hyperparameters("minInstancesPerNode").asInstanceOf[Int]
        val subsamplingRate = hyperparameters("subsamplingRate").asInstanceOf[Double]
        val stepSize = hyperparameters("stepSize").asInstanceOf[Double]

        // Create GBT with specific hyperparameters
        new GBTClassifier()
          .setLabelCol("label")
          .setFeaturesCol("features")
          .setPredictionCol("prediction")
          .setProbabilityCol("probability")
          .setRawPredictionCol("rawPrediction")
          .setMaxIter(maxIter)
          .setMaxDepth(maxDepth)
          .setMaxBins(maxBins)
          .setMinInstancesPerNode(minInstancesPerNode)
          .setSubsamplingRate(subsamplingRate)
          .setStepSize(stepSize)

      case "logisticregression" | "lr" =>
        // Extract hyperparameters
        val maxIter = hyperparameters("maxIter").asInstanceOf[Int]
        val regParam = hyperparameters("regParam").asInstanceOf[Double]
        val elasticNetParam = hyperparameters("elasticNetParam").asInstanceOf[Double]

        // Create Logistic Regression with specific hyperparameters
        new LogisticRegression()
          .setLabelCol("label")
          .setFeaturesCol("features")
          .setPredictionCol("prediction")
          .setProbabilityCol("probability")
          .setRawPredictionCol("rawPrediction")
          .setMaxIter(maxIter)
          .setRegParam(regParam)
          .setElasticNetParam(elasticNetParam)
          .setFamily("binomial")

      case other =>
        throw new IllegalArgumentException(s"Unsupported model type in trainWithParams: $other")
    }

    // Create and fit pipeline
    val pipeline = new Pipeline().setStages(Array(classifier))
    pipeline.fit(data)
  }
}
