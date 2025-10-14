package com.flightdelay.data

import com.flightdelay.config.AppConfiguration
import com.flightdelay.data.loaders.{FlightDataLoader, WeatherDataLoader, WBANAirportTimezoneLoader}
import com.flightdelay.data.preprocessing.flights.FlightPreprocessingPipeline
import com.flightdelay.data.preprocessing.weather.WeatherPreprocessingPipeline
import com.flightdelay.data.utils.SchemaValidator
import com.flightdelay.features.joiners.FlightWeatherDataJoiner
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataPipeline {

  /**
   * Pipeline complet de traitement des données
   * Charge les données depuis la configuration, preprocesse les données de vols et météo, puis les joint
   * @param spark Session Spark
   * @param configuration Configuration de l'application
   * @return DataFrame final contenant les données jointes et préprocessées
   */
  def execute()(implicit spark: SparkSession, configuration: AppConfiguration): (DataFrame, DataFrame) = {

    val pipelineStartTime = System.currentTimeMillis()

    println("\n" + "=" * 80)
    println("[DataPipeline] Complete Data Pipeline - Start")
    println("=" * 80)

    // Chargement des données brutes
    println("\n[Step 1/5] Loading raw flight data...")
    var stepStartTime = System.currentTimeMillis()
    FlightDataLoader.loadFromConfiguration()
    var stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 1/5] Completed in ${stepDuration}s")

    println("\n[Step 2/5] Loading raw weather data...")
    stepStartTime = System.currentTimeMillis()
    WeatherDataLoader.loadFromConfiguration()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 2/5] Completed in ${stepDuration}s")

    println("\n[Step 3/5] Loading WBAN-Airport-Timezone mapping...")
    stepStartTime = System.currentTimeMillis()
    WBANAirportTimezoneLoader.loadFromConfiguration()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 3/5] Completed in ${stepDuration}s")

    // Preprocessing des données de vols
    println("\n[Step 4/5] Preprocessing flight data...")
    stepStartTime = System.currentTimeMillis()
    val processedFlightData = FlightPreprocessingPipeline.execute()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 4/5] Completed in ${stepDuration}s")

    // Preprocessing des données météo
    println("\n[Step 5/5] Preprocessing weather data...")
    stepStartTime = System.currentTimeMillis()
    val processedWeatherData = WeatherPreprocessingPipeline.execute()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 5/5] Completed in ${stepDuration}s")

    // Validate and normalize schemas
    println("\n[Step 6/6] Validating and normalizing schemas...")
    stepStartTime = System.currentTimeMillis()

    println("\n--- Flight Data Schema Validation ---")
    val normalizedFlightData = SchemaValidator.validateAndNormalize(processedFlightData, strictMode = false)

    println("\n--- Weather Data Schema Validation ---")
    val normalizedWeatherData = SchemaValidator.validateAndNormalize(processedWeatherData, strictMode = false)

    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 6/6] Completed in ${stepDuration}s")

    // OPTIMIZATION: Cache normalized data since it will be used by all experiments
    println("\n[Step 7/7] Caching normalized data for reuse across experiments...")
    stepStartTime = System.currentTimeMillis()
    val cachedFlightData = normalizedFlightData.cache()
    val cachedWeatherData = normalizedWeatherData.cache()

    // Force materialization
    val flightCount = cachedFlightData.count()
    val weatherCount = cachedWeatherData.count()
    println(s"  - Cached flight data: ${flightCount} records")
    println(s"  - Cached weather data: ${weatherCount} records")

    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 7/7] Completed in ${stepDuration}s")

    val totalDuration = (System.currentTimeMillis() - pipelineStartTime) / 1000.0
    println("\n" + "=" * 80)
    println(s"[DataPipeline] Complete Data Pipeline - End (Total: ${totalDuration}s)")
    println("=" * 80 + "\n")

    (cachedFlightData, cachedWeatherData)
  }

}
