package com.flightdelay.config

case class DataConfig(
   basePath: String,
   flight: FileConfig,
   weather: FileConfig,
   airportMapping: FileConfig
)
