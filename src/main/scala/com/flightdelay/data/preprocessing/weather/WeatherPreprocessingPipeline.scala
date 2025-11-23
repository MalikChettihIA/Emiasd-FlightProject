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
  def execute(rawFlightData: DataFrame, rawWeatherData: DataFrame, rawWBANAirportTimezoneData: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    info("=" * 80)
    info("[DataPipeline][Step 5/7] Weather Data Preprocessing Pipeline - Start")
    info("=" * 80)

    val processedParquetPath = s"${configuration.common.output.basePath}/common/data/processed_weather.parquet"


    // Execute preprocessing pipeline (each step creates a new DataFrame)
    val processedWithSkyConditionFeatureDf = rawWeatherData.transform(SkyConditionFeatures.createSkyConditionFeatures)
    val porcessedWithVisibilityFeaturesDf = processedWithSkyConditionFeatureDf.transform(VisibilityFeatures.createVisibilityFeatures)
    val porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf = porcessedWithVisibilityFeaturesDf.transform(WeatherInteractionFeatures.createInteractionFeatures)
    val porcessWithWeatherConditionFeaturesDf = porcessedWithSkyConditionAndVisibilityIntegrationFeaturesDf.transform(WeatherTypeFeatureGenerator.createFeatures)
    val processedWithPressureTendencyFeaturesDf = porcessWithWeatherConditionFeaturesDf.transform(WeatherPressionFeatures.createPressureFeatures)
    val processedWithTemporalFeaturesDf = WeatherTemporalFeatures.preprocess(processedWithPressureTendencyFeaturesDf)
    val processedWeatherDf = WeatherDataCleaner.preprocess(processedWithTemporalFeaturesDf, rawFlightData)

    processedWeatherDf
  }


}