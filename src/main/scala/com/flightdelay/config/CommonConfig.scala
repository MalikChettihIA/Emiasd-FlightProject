package com.flightdelay.config

/**
 * Common configuration shared across all experiments
 * @param seed Random seed for reproducibility
 * @param data Data sources configuration
 * @param output Output base path configuration
 * @param mlflow MLFlow tracking configuration
 */
case class CommonConfig(
  seed: Long,
  data: DataConfig,
  output: OutputConfig,
  mlflow: MLFlowConfig = MLFlowConfig()
)
