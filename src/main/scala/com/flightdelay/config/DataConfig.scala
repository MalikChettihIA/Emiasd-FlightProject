package com.flightdelay.config

case class DataConfig(
  basePath: String,
  flight: DataFileConfig,
  weather: DataFileConfig,
  airportMapping: DataFileConfig
)
