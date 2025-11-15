package com.flightdelay.data

import com.flightdelay.config.AppConfiguration
import com.flightdelay.data.loaders.{FlightDataLoader, WeatherDataLoader, WBANAirportTimezoneLoader}
import com.flightdelay.data.preprocessing.flights.FlightPreprocessingPipeline
import com.flightdelay.data.preprocessing.weather.WeatherPreprocessingPipeline
import com.flightdelay.data.utils.SchemaValidator
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.flightdelay.utils.DebugUtils._

object DataPipeline {

  /**
   * Pipeline complet de traitement des données
   * Charge les données depuis la configuration, preprocesse les données de vols et météo, puis les joint
   * @param spark Session Spark
   * @param configuration Configuration de l'application
   * @return Tuple (FlightData, Option[WeatherData]) - Weather is None if no experiments use weather features
   */
  def execute()(implicit spark: SparkSession, configuration: AppConfiguration): (DataFrame, DataFrame) = {

    val pipelineStartTime = System.currentTimeMillis()

    info("=" * 80)
    info("[DataPipeline] Complete Data Pipeline - Start")
    info("=" * 80)

    // Chargement des données brutes
    info("[DataPipeline][Step 1/7] Loading raw flight data...")
    var stepStartTime = System.currentTimeMillis()
    FlightDataLoader.loadFromConfiguration()
    var stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    info(s"[DataPipeline][Step 1/7] Completed in ${stepDuration}s")

    // Preprocessing des données de météo
    info("=" * 80)
    info("[DataPipeline][Step 2/7] Loading raw weather data...")
    stepStartTime = System.currentTimeMillis()
    WeatherDataLoader.loadFromConfiguration()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    info(s"[DataPipeline][Step 2/7] Completed in ${stepDuration}s")

    info("=" * 80)
    info("[DataPipeline][Step 3/7] Loading WBAN-Airport-Timezone mapping...")
    stepStartTime = System.currentTimeMillis()
    WBANAirportTimezoneLoader.loadFromConfiguration()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    info(s"[DataPipeline][Step 3/7] Completed in ${stepDuration}s")

    // Preprocessing des données de vols
    stepStartTime = System.currentTimeMillis()
    val processedFlightData = FlightPreprocessingPipeline.execute()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0

    info(s"[DataPipeline][Step 4/7] Completed in ${stepDuration}s")

    stepStartTime = System.currentTimeMillis()
    val processedWeatherData = WeatherPreprocessingPipeline.execute(processedFlightData)
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    info(s"[DataPipeline][Step 5/7] Completed in ${stepDuration}s")

    // Validate and normalize schemas
    //info("[DataPipeline][Step 6/7] Validating and normalizing schemas...")
    //stepStartTime = System.currentTimeMillis()
    //debug("--- Flight Data Schema Validation ---")
    //val validatedFlightData = SchemaValidator.validateAndNormalize(processedFlightData, strictMode = false)
    //debug("--- Weather Data Schema Validation ---")
    //val validatedWeatherData = SchemaValidator.validateAndNormalize(weather, strictMode = false)
    //stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    //info(s"[DataPipeline][Step 6/7] Completed in ${stepDuration}s")

    // OPTIMIZATION: Cache normalized data since it will be used by all experiments
    //info("[DataPipeline][Step 7/7] Caching normalized data for reuse across experiments...")
    //stepStartTime = System.currentTimeMillis()
    val cachedFlightData = processedFlightData.cache()
    val cachedWeatherData = processedWeatherData.cache()

    // Force materialization
    whenDebug{
      val flightCount = cachedFlightData.count()
      debug(s"  - Cached flight data: ${flightCount} records")
      val weatherCount = cachedWeatherData.count()
      debug(s"  - Cached weather data: ${weatherCount} records")
      stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    }
    //info(s"[DataPipeline][Step 7/7] Completed in ${stepDuration}s")

    val totalDuration = (System.currentTimeMillis() - pipelineStartTime) / 1000.0
    info("=" * 80)
    info(s"[DataPipeline] Complete Data Pipeline - End (Total: ${totalDuration}s)")
    info("=" * 80)

    (cachedFlightData, cachedWeatherData)
  }

}
