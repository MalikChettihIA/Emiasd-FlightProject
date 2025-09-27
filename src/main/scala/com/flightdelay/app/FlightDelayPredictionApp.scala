package com.flightdelay.app

import com.flightdelay.config.ConfigurationLoader
import com.flightdelay.data.preprocessing.FlightDataPreprocessor
import com.flightdelay.data.loaders.FlightDataLoader

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import scala.util.{Success, Failure}

object FlightDelayPredictionApp {

  protected val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Flight Data Loader App")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    // Réduire les logs pour plus de clarté
    spark.sparkContext.setLogLevel("WARN")

    logger.info("--> FlightDelayPrediction App Starting ...")
    val configuration = ConfigurationLoader.loadConfiguration(args)
    logger.info("--> FlightDelayPrediction App Configuration "+ configuration.environment +" Loaded")

    try {

      FlightDataLoader.load(configuration) match {
        case Success(flightData) if !flightData.isEmpty =>
          logger.info(s"Données chargées: ${flightData.count()} lignes")

          val processedFlightData = FlightDataPreprocessor.preprocess(flightData)

          logger.info(s"Preprocessing terminé: ${processedFlightData.count()} lignes traitées")

        // Continuer avec le processedFlightData...

        case Success(flightData) =>
          logger.warn("Dataset de vols vide après chargement")

        case Failure(exception) =>
          logger.error(s"Erreur lors du chargement des données: ${exception.getMessage}")
          throw exception
      }




    } catch {
      case ex: Exception =>
        logger.error(s"Erreur dans l'application: ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      spark.stop()
      logger.info("--> FlightDelayPrediction App Stopped ...")
    }
  }
}