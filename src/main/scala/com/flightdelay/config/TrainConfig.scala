package com.flightdelay.config

/**
 * Training configuration for an experiment
 * @param trainRatio Ratio of data used for training (0.0 to 1.0)
 * @param crossValidation Cross-validation configuration
 * @param gridSearch Grid search configuration
 * @param hyperparameters Model hyperparameters
 */
case class TrainConfig(
  trainRatio: Double,
  crossValidation: CrossValidationConfig,
  gridSearch: GridSearchConfig,
  hyperparameters: HyperparametersConfig
)
