package com.flightdelay.config

/**
 * Model configuration for an experiment
 * @param modelType Type of ML model: "randomforest", "gbt", "logisticregression", "decisiontree"
 * @param hyperparameters Model hyperparameters for training
 */
case class ExperimentModelConfig(
  modelType: String,
  hyperparameters: HyperparametersConfig
)
