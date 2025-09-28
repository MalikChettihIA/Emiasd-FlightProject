package com.flightdelay.app

import com.flightdelay.config.ConfigurationLoader
import com.flightdelay.data.preprocessing.FlightPreprocessingPipeline
import com.flightdelay.data.loaders.FlightDataLoader
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

object FlightDelayPredictionApp {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Flight Data Loader App")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    // Réduire les logs pour plus de clarté
    spark.sparkContext.setLogLevel("WARN")

    println("--> FlightDelayPrediction App Starting ...")
    val configuration = ConfigurationLoader.loadConfiguration(args)
    println("--> FlightDelayPrediction App Configuration "+ configuration.environment +" Loaded")

    try {

      val flightData = FlightDataLoader.load(configuration)
      val processedFlightData = FlightPreprocessingPipeline.execute(flightData)



    } catch {
      case ex: Exception =>
        println(s"Erreur dans l'application: ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      spark.stop()
      println("--> FlightDelayPrediction App Stopped ...")
    }
  }
}