package com.flightdelay.ml.training

import com.flightdelay.config.{AppConfiguration, ModelConfig}
import com.flightdelay.ml.models.{MLModel, RandomForestModel}
import com.flightdelay.ml.evaluation.ModelEvaluator
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Model trainer with factory pattern for selecting different ML algorithms.
 * Supports configurable model types via configuration file.
 */
object ModelTrainer {

  /**
   * Training result containing model, predictions, and metrics
   */
  case class TrainingResult(
    model: Transformer,
    trainPredictions: DataFrame,
    testPredictions: DataFrame,
    trainMetrics: ModelEvaluator.EvaluationMetrics,
    testMetrics: ModelEvaluator.EvaluationMetrics
  )

  /**
   * Train a model based on configuration
   * @param data Input DataFrame with "features" and "label" columns
   * @param spark Implicit SparkSession
   * @param config Implicit AppConfiguration
   * @return TrainingResult with trained model and evaluation metrics
   */
  def train(data: DataFrame)(implicit spark: SparkSession, config: AppConfiguration): TrainingResult = {
    println("\n" + "=" * 80)
    println("Model Training Pipeline")
    println("=" * 80)

    // Split data into train and test sets
    val Array(trainData, testData) = splitData(data, config.model.trainRatio, config.model.seed)

    println(s"\nDataset split:")
    println(s"  - Training set: ${trainData.count()} samples (${config.model.trainRatio * 100}%.0f%%)")
    println(s"  - Test set:     ${testData.count()} samples (${(1 - config.model.trainRatio) * 100}%.0f%%)")

    // Create model based on configuration
    val mlModel = createModel(config.model)

    // Train the model
    val trainedModel = mlModel.train(trainData)

    // Make predictions on train and test sets
    println("\n[Making Predictions]")
    val trainPredictions = mlModel.predict(trainedModel, trainData)
    val testPredictions = mlModel.predict(trainedModel, testData)

    // Evaluate model
    val (trainMetrics, testMetrics) = ModelEvaluator.evaluateTrainTest(
      trainPredictions,
      testPredictions
    )

    // Save model
    val modelPath = s"${config.output.model.path}/${config.model.name}_${config.model.modelType}"
    println(s"\n[Saving Model]")
    println(s"Model path: $modelPath")
    mlModel.saveModel(trainedModel, modelPath)
    println("✓ Model saved successfully")

    println("\n" + "=" * 80)
    println("Training Pipeline Completed!")
    println("=" * 80 + "\n")

    TrainingResult(
      model = trainedModel,
      trainPredictions = trainPredictions,
      testPredictions = testPredictions,
      trainMetrics = trainMetrics,
      testMetrics = testMetrics
    )
  }

  /**
   * Factory method to create the appropriate model based on configuration
   * @param config Model configuration
   * @return MLModel instance
   */
  private def createModel(config: ModelConfig): MLModel = {
    val modelType = config.modelType.toLowerCase

    println(s"\n[Model Factory]")
    println(s"Creating model of type: $modelType")

    modelType match {
      case "randomforest" | "rf" =>
        println("✓ Initialized Random Forest Classifier")
        RandomForestModel(config)

      case "gbt" | "gradientboostedtrees" =>
        throw new NotImplementedError(
          "Gradient Boosted Trees not yet implemented. " +
          "Available models: randomforest"
        )

      case "logisticregression" | "lr" =>
        throw new NotImplementedError(
          "Logistic Regression not yet implemented. " +
          "Available models: randomforest"
        )

      case "decisiontree" | "dt" =>
        throw new NotImplementedError(
          "Decision Tree not yet implemented. " +
          "Available models: randomforest"
        )

      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown model type: $unknown. " +
          s"Available models: randomforest, gbt, logisticregression, decisiontree"
        )
    }
  }

  /**
   * Split data into train and test sets
   * @param data Input DataFrame
   * @param trainRatio Ratio for training set (0.0 to 1.0)
   * @param seed Random seed for reproducibility
   * @return Array with [trainData, testData]
   */
  private def splitData(data: DataFrame, trainRatio: Double, seed: Long): Array[DataFrame] = {
    require(trainRatio > 0.0 && trainRatio < 1.0, s"Train ratio must be between 0 and 1, got $trainRatio")
    data.randomSplit(Array(trainRatio, 1.0 - trainRatio), seed)
  }
}
