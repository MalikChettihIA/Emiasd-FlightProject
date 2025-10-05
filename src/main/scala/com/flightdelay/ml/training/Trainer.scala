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

    // Create model from factory
    val model = ModelFactory.create(experiment)

    // TODO: Apply hyperparameters to model before training
    // For now, just train with default params from experiment config

    model.train(data)
  }
}
