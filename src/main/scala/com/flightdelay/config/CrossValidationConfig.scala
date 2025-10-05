package com.flightdelay.config

/**
 * Cross-validation configuration
 * @param numFolds Number of folds for K-Fold cross-validation
 */
case class CrossValidationConfig(
  numFolds: Int
)
