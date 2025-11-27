package com.flightdelay.config

/**
 * Training configuration for an experiment
 * @param trainRatio Ratio of data used for training (0.0 to 1.0)
 * @param fast Skip cross-validation and go directly to final model training (default: false)
 * @param crossValidation Cross-validation configuration
 * @param gridSearch Grid search configuration
 */
case class TrainConfig(
  trainRatio: Double,
  fast: Boolean = false,
  crossValidation: CrossValidationConfig,
  gridSearch: GridSearchConfig
)
