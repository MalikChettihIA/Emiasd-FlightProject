package com.flightdelay.config

/**
 * Configuration for a single experiment
 * @param name Experiment name (used for output folder naming)
 * @param description Human-readable description of the experiment
 * @param enabled Whether this experiment should be executed
 * @param target Target column name (e.g., "label_is_delayed_15min")
 * @param featureExtraction Feature extraction configuration
 * @param model Model configuration
 * @param train Training configuration
 */
case class ExperimentConfig(
  name: String,
  description: String,
  enabled: Boolean,
  target: String,
  featureExtraction: FeatureExtractionConfig,
  model: ExperimentModelConfig,
  train: TrainConfig
)
