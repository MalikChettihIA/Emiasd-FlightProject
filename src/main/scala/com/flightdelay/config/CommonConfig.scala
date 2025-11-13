package com.flightdelay.config

/**
 * Common configuration shared across all experiments
 * @param seed Random seed for reproducibility
 * @param log Enable/disable all logging output
 * @param logLevel Minimum log level to display (debug, info, warn, error)
 * @param loadDataFromCSV If true, load from CSV/TXT files; if false, load from raw parquet files
 * @param data Data sources configuration
 * @param output Output base path configuration
 * @param mlflow MLFlow tracking configuration
 */
case class CommonConfig(
  seed: Long,
  log: Boolean = true,
  logLevel: String = "info",
  loadDataFromCSV: Boolean = true,
  data: DataConfig,
  output: OutputConfig,
  mlflow: MLFlowConfig = MLFlowConfig()
) {
  /**
   * Convertit le logLevel string en LogLevel enum
   */
  def getLogLevel: LogLevel.LogLevel = LogLevel.fromString(logLevel)
}
