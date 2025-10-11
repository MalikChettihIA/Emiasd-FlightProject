package com.flightdelay.data

import com.flightdelay.config.AppConfiguration
import com.flightdelay.data.loaders.{FlightDataLoader, WeatherDataLoader, WBANAirportTimezoneLoader}
import com.flightdelay.data.preprocessing.flights.FlightPreprocessingPipeline
import com.flightdelay.data.preprocessing.weather.WeatherPreprocessingPipeline
import com.flightdelay.data.joiners.FlightWeatherDataJoiner
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataPipeline {

  /**
   * Pipeline complet de traitement des données
   * Charge les données depuis la configuration, preprocesse les données de vols et météo, puis les joint
   * @param spark Session Spark
   * @param configuration Configuration de l'application
   * @return DataFrame final contenant les données jointes et préprocessées
   */
  def execute()(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    val pipelineStartTime = System.currentTimeMillis()

    println("\n" + "=" * 80)
    println("[DataPipeline] Complete Data Pipeline - Start")
    println("=" * 80)

    // Chargement des données brutes
    println("\n[Step 1/6] Loading raw flight data...")
    var stepStartTime = System.currentTimeMillis()
    FlightDataLoader.loadFromConfiguration()
    var stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 1/6] Completed in ${stepDuration}s")

    println("\n[Step 2/6] Loading raw weather data...")
    stepStartTime = System.currentTimeMillis()
    WeatherDataLoader.loadFromConfiguration()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 2/6] Completed in ${stepDuration}s")

    println("\n[Step 3/6] Loading WBAN-Airport-Timezone mapping...")
    stepStartTime = System.currentTimeMillis()
    WBANAirportTimezoneLoader.loadFromConfiguration()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 3/6] Completed in ${stepDuration}s")

    // Preprocessing des données de vols
    println("\n[Step 4/6] Preprocessing flight data...")
    stepStartTime = System.currentTimeMillis()
    val processedFlightData = FlightPreprocessingPipeline.execute()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 4/6] Completed in ${stepDuration}s")

    // Preprocessing des données météo
    println("\n[Step 5/6] Preprocessing weather data...")
    stepStartTime = System.currentTimeMillis()
    val processedWeatherData = WeatherPreprocessingPipeline.execute()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 5/6] Completed in ${stepDuration}s")

    // Jointure des données
    println("\n[Step 6/6] Joining flight and weather data...")
    stepStartTime = System.currentTimeMillis()
    val joinedData = FlightWeatherDataJoiner.join(processedFlightData, processedWeatherData)
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"[Step 6/6] Completed in ${stepDuration}s")

    val totalDuration = (System.currentTimeMillis() - pipelineStartTime) / 1000.0
    println("\n" + "=" * 80)
    println(s"[DataPipeline] Complete Data Pipeline - End (Total: ${totalDuration}s)")
    println("=" * 80 + "\n")


    val joinedParquetPath = s"${configuration.common.output.basePath}/common/data/joined_flights_weather.parquet"
    println(s"\nSaving joined flight weather parquet data to parquet:")
    println(s"  - Path: $joinedParquetPath")
    joinedData.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(joinedParquetPath)
    println(s"  - Saved ${joinedData.count()} preprocessed records")

    joinedData
  }

}
