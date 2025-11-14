package com.flightdelay.data.preprocessing.weather

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.flightdelay.utils.DebugUtils._

object WeatherPreprocessingPipeline {

  /**
   * Exécute le pipeline complet de preprocessing pour les données météo
   * Charge les données depuis le fichier parquet généré par WeatherDataLoader
   * @param configuration Configuration contenant les paramètres de chargement
   * @param spark Session Spark
   * @return DataFrame complètement préprocessé
   */
  def execute(flightData:DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("=" * 80)
    info("[DataPipeline][Step 5/7] Weather Data Preprocessing Pipeline - Start")
    info("=" * 80)

    val processedParquetPath = s"${configuration.common.output.basePath}/common/data/processed_weather.parquet"

    // Load raw data from parquet
    val rawParquetPath = s"${configuration.common.output.basePath}/common/data/raw_weather.parquet"
    debug(s"\nLoading raw data from parquet:")
    debug(s"  - Path: $rawParquetPath")
    val originalDf = spark.read.parquet(rawParquetPath)
    debug(s"  - Loaded raw records")

    // Execute preprocessing pipeline (each step creates a new DataFrame)
    val processedWithSkyConditionFeatureDf = originalDf.transform(SkyConditionFeatures.createSkyConditionFeatures)
    val porcessedWithVisibilityFeaturesDf = processedWithSkyConditionFeatureDf.transform(VisibilityFeatures.createVisibilityFeatures)
    val porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf = porcessedWithVisibilityFeaturesDf.transform(WeatherInteractionFeatures.createInteractionFeatures)
    val porcessWithWeatherConditionFeaturesDf = porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf.transform(WeatherTypeFeatureGenerator.createFeatures)
    val processedWithPressureTendencyFeaturesDf = porcessWithWeatherConditionFeaturesDf.transform(WeatherPressionFeatures.createPressureFeatures)
    val processedWeatherDf = WeatherDataCleaner.preprocess(processedWithPressureTendencyFeaturesDf, flightData)

    processedWeatherDf
  }


}