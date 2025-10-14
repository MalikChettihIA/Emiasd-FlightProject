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

    // Execute preprocessing pipeline (each step creates a new DataFrame)
    val processedWithSkyConditionFeatureDf = originalDf.transform(SkyConditionFeatures.createSkyConditionFeatures)
    val porcessedWithVisibilityFeaturesDf = processedWithSkyConditionFeatureDf.transform(VisibilityFeatures.createVisibilityFeatures)
    val porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf = porcessedWithVisibilityFeaturesDf.transform(WeatherInteractionFeatures.createInteractionFeatures)
    val porcessWithWeatherConditionFeaturesDf = porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf.transform(WeatherTypeFeatureGenerator.createFeatures)
    val processedWeatherDf = WeatherDataCleaner.preprocess(porcessWithWeatherConditionFeaturesDf)


    // OPTIMIZATION: Cache final data because it will be:
    // 1. Counted once
    // 2. Written to parquet
    // 3. Returned and used by DataPipeline (cached again there)
    val cachedProcessedWeatherDf = processedWeatherDf.cache()
    val processedCount = cachedProcessedWeatherDf.count()

    println(s"\nSaving preprocessed data to parquet:")
    println(s"  - Path: $processedParquetPath")
    println(s"  - Records to save: ${processedCount}")

    // OPTIMIZATION: Coalesce and use zstd compression
    cachedProcessedWeatherDf.coalesce(8)
      .write
      .mode("overwrite")
      .option("compression", "zstd")
      .parquet(processedParquetPath)
    println(s"  - Saved ${processedCount} preprocessed records")

    println("\n" + "=" * 80)
    println("[Preprocessing] Weather Data Preprocessing Pipeline - End")
    println("=" * 80 + "\n")

    cachedProcessedWeatherDf
  }

}
