package com.flightdelay.data.preprocessing.weather

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}

object WeatherPreprocessingPipeline {

  /**
   * Exécute le pipeline complet de preprocessing pour les données météo
   * Charge les données depuis le fichier parquet généré par WeatherDataLoader
   * @param configuration Configuration contenant les paramètres de chargement
   * @param spark Session Spark
   * @return DataFrame complètement préprocessé
   */
  def execute()(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    println("\n" + "=" * 80)
    println("[Preprocessing] Weather Data Preprocessing Pipeline - Start")
    println("=" * 80)

    val processedParquetPath = s"${configuration.common.output.basePath}/common/data/processed_weather.parquet"

    // Load raw data from parquet
    val rawParquetPath = s"${configuration.common.output.basePath}/common/data/raw_weather.parquet"
    println(s"\nLoading raw data from parquet:")
    println(s"  - Path: $rawParquetPath")
    val originalDf = spark.read.parquet(rawParquetPath)
    println(s"  - Loaded ${originalDf.count()} raw records")

    // Pour l'instant, on retourne le dataset tel quel
    val processedWithSkyConditionFeatureDf = originalDf.transform(SkyConditionFeatures.createSkyConditionFeatures)
    val porcessedWithVisibilityFeaturesDf = processedWithSkyConditionFeatureDf.transform(VisibilityFeatures.createVisibilityFeatures)
    val porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf = porcessedWithVisibilityFeaturesDf.transform(WeatherInteractionFeatures.createInteractionFeatures)
    val processedWeatherDf = porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf

    println(s"\nSaving preprocessed data to parquet:")
    println(s"  - Path: $processedParquetPath")
    processedWeatherDf.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(processedParquetPath)
    println(s"  - Saved ${processedWeatherDf.count()} preprocessed records")

    println("\n" + "=" * 80)
    println("[Preprocessing] Weather Data Preprocessing Pipeline - End")
    println("=" * 80 + "\n")

    processedWeatherDf
  }

}
