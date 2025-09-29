package com.flightdelay.config

case class AppConfiguration(
 environment: String,
 data: DataConfig,
 output: OutputConfig
)
