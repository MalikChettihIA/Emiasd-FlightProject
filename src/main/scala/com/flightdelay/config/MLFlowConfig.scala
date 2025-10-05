package com.flightdelay.config

/**
 * MLFlow configuration for experiment tracking
 * @param enabled Enable/disable MLFlow tracking
 * @param trackingUri MLFlow tracking server URI (e.g., "http://localhost:5555")
 */
case class MLFlowConfig(
  enabled: Boolean = false,
  trackingUri: String = "http://localhost:5555"
)
