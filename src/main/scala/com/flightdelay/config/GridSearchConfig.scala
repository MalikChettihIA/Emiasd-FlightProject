package com.flightdelay.config

/**
 * Grid search configuration for hyperparameter tuning
 * @param enabled Whether grid search is enabled
 * @param evaluationMetric Metric to optimize (e.g., "f1", "accuracy", "areaUnderROC")
 */
case class GridSearchConfig(
  enabled: Boolean,
  evaluationMetric: String
)
