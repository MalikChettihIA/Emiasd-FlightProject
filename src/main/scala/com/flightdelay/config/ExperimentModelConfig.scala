package com.flightdelay.config

/**
 * Model configuration for an experiment
 * @param modelType Type of ML model: "randomforest", "gbt", "logisticregression", "decisiontree"
 */
case class ExperimentModelConfig(
  modelType: String
  // Future model-specific configurations can be added here
  // e.g., earlyStoppingRounds, learningRate, etc.
)
