package com.flightdelay.data.joiners

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlightWeatherDataJoiner {

  /**
   * Joint les données de vols et les données météo
   * @param flightData DataFrame contenant les données de vols préprocessées
   * @param weatherData DataFrame contenant les données météo préprocessées
   * @param spark Session Spark
   * @param configuration Configuration de l'application
   * @return DataFrame joint des données de vols et météo
   */
  def join(flightData: DataFrame, weatherData: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    println("\n" + "=" * 80)
    println("[Joiner] Flight-Weather Data Join - Start")
    println("=" * 80)

    println(s"\nInput flight data:")
    println(s"  - Records: ${flightData.count()}")
    println(s"  - Columns: ${flightData.columns.length}")

    println(s"\nInput weather data:")
    println(s"  - Records: ${weatherData.count()}")
    println(s"  - Columns: ${weatherData.columns.length}")

    // TODO: Implémenter la logique de jointure
    // Pour l'instant, on retourne uniquement les données de vol
    val joinedData = flightData

    println(s"\nOutput joined data:")
    println(s"  - Records: ${joinedData.count()}")
    println(s"  - Columns: ${joinedData.columns.length}")

    println("\n" + "=" * 80)
    println("[Joiner] Flight-Weather Data Join - End")
    println("=" * 80 + "\n")

    joinedData
  }

}
