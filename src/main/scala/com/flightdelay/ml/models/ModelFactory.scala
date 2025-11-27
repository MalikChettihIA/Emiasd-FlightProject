package com.flightdelay.ml.models

import com.flightdelay.config.ExperimentConfig

/**
 * Factory for creating ML models based on experiment configuration
 *
 * Implements Factory Pattern for extensibility:
 * - Easy to add new model types
 * - Centralized model creation logic
 * - Type-safe model selection
 *
 * @example
 * {{{
 *   val model = ModelFactory.create(experiment)
 *   val trained = model.train(data)
 * }}}
 */
object ModelFactory {

  /**
   * Create appropriate ML model based on experiment configuration
   *
   * @param experiment Experiment configuration with model type and hyperparameters
   * @return MLModel instance ready for training
   * @throws IllegalArgumentException if model type is unknown
   * @throws NotImplementedError if model type is not yet implemented
   */
  def create(experiment: ExperimentConfig): MLModel = {
    val modelType = experiment.model.modelType.toLowerCase

    println(s"[ModelFactory] Creating model: $modelType")

    modelType match {
      case "randomforest" | "rf" =>
        println("  → Random Forest Classifier")
        new RandomForestModel(experiment)

      case "gbt" | "gradientboostedtrees" =>
        println("  → Gradient Boosted Trees Classifier")
        new GradientBoostedTreesModel(experiment)

      case "logisticregression" | "lr" =>
        println("  → Logistic Regression Classifier")
        new LogisticRegressionModel(experiment)

      case "xgboost" | "xgb" =>
        println("  → XGBoost Classifier")
        new XGBoostModel(experiment)

      case "decisiontree" | "dt" =>
        throw new NotImplementedError(
          "Decision Tree not yet implemented. " +
            "Available models: randomforest, gbt, logisticregression, xgboost"
        )

      case "lightgbm" =>
        throw new NotImplementedError(
          "LightGBM not yet implemented. " +
            "Available models: randomforest"
        )

      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown model type: '$unknown'. " +
            s"Available models: randomforest, gbt, logisticregression, xgboost, decisiontree, lightgbm"
        )
    }
  }

  /**
   * Check if a model type is supported
   *
   * @param modelType Model type string
   * @return true if model is implemented, false otherwise
   */
  def isSupported(modelType: String): Boolean = {
    val normalized = modelType.toLowerCase
    normalized match {
      case "randomforest" | "rf" => true
      case "gbt" | "gradientboostedtrees" => true
      case "logisticregression" | "lr" => true
      case "xgboost" | "xgb" => true
      case _ => false
    }
  }

  /**
   * Get list of all supported model types
   *
   * @return Sequence of supported model type names
   */
  def supportedModels: Seq[String] = Seq(
    "randomforest",
    "gbt",
    "logisticregression",
    "xgboost"
  )

  /**
   * Get list of all planned (but not yet implemented) model types
   *
   * @return Sequence of planned model type names
   */
  def plannedModels: Seq[String] = Seq(
    "decisiontree",
    "lightgbm"
  )
}
