package com.flightdelay.ml.models

import com.flightdelay.config.AppConfiguration
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Abstract trait for all ML models in the flight delay prediction system.
 * Provides a unified interface for training, prediction, and model persistence.
 */
trait MLModel {

  /**
   * Train the model on the provided dataset
   * @param data Training data with "features" and "label" columns
   * @return Trained model as a Transformer
   */
  def train(data: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): Transformer

  /**
   * Make predictions on the provided dataset
   * @param model Trained model
   * @param data Data to predict on (must have "features" column)
   * @return DataFrame with predictions
   */
  def predict(model: Transformer, data: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {
    model.transform(data)
  }

  /**
   * Save the trained model to disk
   * @param model Trained model to save
   * @param path Output path
   */
  def saveModel(model: Transformer, path: String): Unit = {
    model.asInstanceOf[PipelineModel].write.overwrite().save(path)
  }

  /**
   * Load a trained model from disk
   * @param path Path to the saved model
   * @return Loaded model
   */
  def loadModel(path: String): Transformer = {
    PipelineModel.load(path)
  }
}
