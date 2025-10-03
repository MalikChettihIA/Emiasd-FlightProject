package com.flightdelay.config

case class AppConfiguration(
 environment: String,
 data: DataConfig,
 featureExtraction: FeatureExtractionConfig,
 model:ModelConfig,
 output: OutputConfig
)
