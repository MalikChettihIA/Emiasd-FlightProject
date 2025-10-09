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

    println("\n" + "=" * 80)
    println("[DataPipeline] Complete Data Pipeline - Start")
    println("=" * 80)

    // Chargement des données brutes
    println("\n[Step 1/6] Loading raw flight data...")
    FlightDataLoader.loadFromConfiguration()

    println("\n[Step 2/6] Loading raw weather data...")
    WeatherDataLoader.loadFromConfiguration()

    println("\n[Step 3/6] Loading WBAN-Airport-Timezone mapping...")
    WBANAirportTimezoneLoader.loadFromConfiguration()

    // Preprocessing des données de vols
    println("\n[Step 4/6] Preprocessing flight data...")
    val processedFlightData = FlightPreprocessingPipeline.execute()

    // Preprocessing des données météo
    println("\n[Step 5/6] Preprocessing weather data...")
    val processedWeatherData = WeatherPreprocessingPipeline.execute()

    // Jointure des données
    println("\n[Step 6/6] Joining flight and weather data...")
    val joinedData = FlightWeatherDataJoiner.join(processedFlightData, processedWeatherData)

    println("\n" + "=" * 80)
    println("[DataPipeline] Complete Data Pipeline - End")
    println("=" * 80 + "\n")

    joinedData
  }

}
