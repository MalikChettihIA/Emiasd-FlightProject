package com.flightdelay.data.preprocessing

import com.flightdelay.config.AppConfiguration
import com.flightdelay.utils.CsvWriter
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlightPreprocessingPipeline {

  /**
   * Exécute le pipeline complet de preprocessing
   * @param configuration Configuration contenant les paramètres de chargement
   * @param spark Session Spark
   * @return DataFrame complètement préprocessé avec labels
   */
  def execute(originalDf: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    println("")
    println("")
    println("----------------------------------------------------------------------------------------------------------")
    println("--> [FlightPreprocessingPipeline] Flight Preprocessing Pipeline - Start ...")
    println("----------------------------------------------------------------------------------------------------------")

    val cleanedFlightData = FlightDataCleaner.preprocess(originalDf)
    val generatedFightData = FlightDataGenerator.preprocess(cleanedFlightData)
    val generatedFightDataWithLabels = FlightLabelGenerator.preprocess(generatedFightData)


    println("")
    println("----------------------------------------------------------------------------------------------------------")
    println("--> [FlightPreprocessingPipeline] Flight Preprocessing Pipeline - End ...")
    println("----------------------------------------------------------------------------------------------------------")

    generatedFightDataWithLabels
  }
}