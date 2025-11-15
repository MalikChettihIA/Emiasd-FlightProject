package com.flightdelay.config

/**
 * Application configuration
 * @param environment Environment name (e.g., "local", "lamsade")
 * @param common Common configuration shared across all experiments
 * @param experiments List of experiment configurations
 */
case class AppConfiguration(
  environment: String,
  common: CommonConfig,
  experiments: Seq[ExperimentConfig]
) {
  /**
   * Get only enabled experiments
   */
  def enabledExperiments: Seq[ExperimentConfig] = experiments.filter(_.enabled)

  /**
   * Check if logging is enabled
   */
  def isLogEnabled: Boolean = common.log

  /**
   * Check if DEBUG level is active
   */
  def isDebug: Boolean = common.log && common.getLogLevel == LogLevel.DEBUG

  /**
   * Get current log level
   */
  def getLogLevel: LogLevel.LogLevel = common.getLogLevel
}
