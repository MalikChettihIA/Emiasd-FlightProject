package com.flightdelay.data

import com.flightdelay.config.AppConfiguration
import com.flightdelay.data.loaders.{FlightDataLoader, WeatherDataLoader, WBANAirportTimezoneLoader}
import com.flightdelay.data.preprocessing.flights.FlightPreprocessingPipeline
import com.flightdelay.data.preprocessing.weather.WeatherPreprocessingPipeline
import com.flightdelay.data.utils.SchemaValidator
import com.flightdelay.utils.ExecutionTimeTracker
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.flightdelay.utils.DebugUtils._

object DataPipeline {

  /**
   * Pipeline complet de traitement des données
   * Charge les données depuis la configuration, preprocesse les données de vols et météo, puis les joint
   * @param spark Session Spark
   * @param configuration Configuration de l'application
   * @param timeTracker Execution time tracker
   * @return Tuple (FlightData, Option[WeatherData]) - Weather is None if no experiments use weather features
   */
  def execute(timeTracker: ExecutionTimeTracker = null)(implicit spark: SparkSession, configuration: AppConfiguration): (DataFrame, DataFrame) = {

    val pipelineStartTime = System.currentTimeMillis()

    info("=" * 80)
    info("[DataPipeline] Complete Data Pipeline - Start")
    info("=" * 80)

    // Chargement des données brutes
    info("[DataPipeline][Step 1/7] Loading raw flight data...")
    if (timeTracker != null) timeTracker.startStep("data_processing.load_flights")
    var stepStartTime = System.currentTimeMillis()
    var originalFlightData = FlightDataLoader.loadFromConfiguration()
    var stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    if (timeTracker != null) timeTracker.endStep("data_processing.load_flights")
    info(s"[DataPipeline][Step 1/7] Completed in ${stepDuration}s")

    // Preprocessing des données de météo
    info("=" * 80)
    info("[DataPipeline][Step 2/7] Loading raw weather data...")
    if (timeTracker != null) timeTracker.startStep("data_processing.load_weather")
    stepStartTime = System.currentTimeMillis()
    var originalWeatherData = WeatherDataLoader.loadFromConfiguration()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    if (timeTracker != null) timeTracker.endStep("data_processing.load_weather")
    info(s"[DataPipeline][Step 2/7] Completed in ${stepDuration}s")

    info("=" * 80)
    info("[DataPipeline][Step 3/7] Loading WBAN-Airport-Timezone mapping...")
    if (timeTracker != null) timeTracker.startStep("data_processing.load_wban")
    stepStartTime = System.currentTimeMillis()
    var originalWBANAirportTimezoneData = WBANAirportTimezoneLoader.loadFromConfiguration()
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    if (timeTracker != null) timeTracker.endStep("data_processing.load_wban")
    info(s"[DataPipeline][Step 3/7] Completed in ${stepDuration}s")

    // Preprocessing des données de vols
    if (timeTracker != null) timeTracker.startStep("data_processing.preprocess_flights")
    stepStartTime = System.currentTimeMillis()
    val processedFlightData = FlightPreprocessingPipeline.execute(originalFlightData, originalWeatherData, originalWBANAirportTimezoneData)
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    if (timeTracker != null) timeTracker.endStep("data_processing.preprocess_flights")

    info(s"[DataPipeline][Step 4/7] Completed in ${stepDuration}s")

    if (timeTracker != null) timeTracker.startStep("data_processing.preprocess_weather")
    stepStartTime = System.currentTimeMillis()
    val processedWeatherData = WeatherPreprocessingPipeline.execute(processedFlightData, originalWeatherData, originalWBANAirportTimezoneData)
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    if (timeTracker != null) timeTracker.endStep("data_processing.preprocess_weather")
    info(s"[DataPipeline][Step 5/7] Completed in ${stepDuration}s")

    // Filter columns based on configuration
    info("=" * 80)
    info("[DataPipeline][Step 6/7] Filtering columns based on configuration...")
    if (timeTracker != null) timeTracker.startStep("data_processing.filter_columns")
    stepStartTime = System.currentTimeMillis()
    val filteredFlightData = filterFlightColumns(processedFlightData)
    val filteredWeatherData = filterWeatherColumns(processedWeatherData)
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    if (timeTracker != null) timeTracker.endStep("data_processing.filter_columns")
    info(s"[DataPipeline][Step 6/7] Completed in ${stepDuration}s")

    // Force materialization
    whenDebug{
      val flightCount = filteredFlightData.count()
      debug(s"  - Cached flight data: ${flightCount} records")
      val weatherCount = filteredWeatherData.count()
      debug(s"  - Cached weather data: ${weatherCount} records")
      stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    }

    // Save to Parquet if configured
    if (configuration.common.storeIntoParquet) {
      info("=" * 80)
      info("[DataPipeline][Step 7/7] Saving processed data to Parquet...")
      if (timeTracker != null) timeTracker.startStep("data_processing.save_parquet")
      stepStartTime = System.currentTimeMillis()

      val flightParquetPath = s"${configuration.common.output.basePath}/common/data/processed_flights.parquet"
      val weatherParquetPath = s"${configuration.common.output.basePath}/common/data/processed_weather.parquet"

      info(s"  - Saving flight data to: $flightParquetPath")
      filteredFlightData.write
        .mode("overwrite")
        .parquet(flightParquetPath)

      info(s"  - Saving weather data to: $weatherParquetPath")
      filteredWeatherData.write
        .mode("overwrite")
        .parquet(weatherParquetPath)

      stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
      if (timeTracker != null) timeTracker.endStep("data_processing.save_parquet")
      info(s"[DataPipeline][Step 7/7] Completed in ${stepDuration}s")
    } else {
      info("[DataPipeline][Step 7/7] Skipping Parquet save (storeIntoParquet=false)")
      if (timeTracker != null) timeTracker.setStepNA("data_processing.save_parquet")
    }

    val totalDuration = (System.currentTimeMillis() - pipelineStartTime) / 1000.0

    // Calculate and set data processing total time
    if (timeTracker != null) {
      val dpTotal = Seq(
        timeTracker.getStepTime("data_processing.load_flights"),
        timeTracker.getStepTime("data_processing.load_weather"),
        timeTracker.getStepTime("data_processing.load_wban"),
        timeTracker.getStepTime("data_processing.preprocess_flights"),
        timeTracker.getStepTime("data_processing.preprocess_weather"),
        timeTracker.getStepTime("data_processing.filter_columns"),
        timeTracker.getStepTime("data_processing.save_parquet")
      ).flatten.filterNot(_.isNaN).sum

      timeTracker.setStepTime("data_processing.total", dpTotal)
    }

    info("=" * 80)
    info(s"[DataPipeline] Complete Data Pipeline - End (Total: ${totalDuration}s)")
    info("=" * 80)

    (filteredFlightData, filteredWeatherData)
  }

  /**
   * Filtre les colonnes de vol en fonction de la configuration de toutes les expériences activées
   * Garde uniquement les colonnes spécifiées dans flightSelectedFeatures
   * + les colonnes essentielles pour le traitement
   * @param df DataFrame des données de vol
   * @param configuration Configuration de l'application
   * @return DataFrame filtré
   */
  private def filterFlightColumns(df: DataFrame)(implicit configuration: AppConfiguration): DataFrame = {
    //Extra columns needed for feature engineering
    val extraColumns = Seq("ARR_DELAY_NEW", "D1", "D2_15", "D2_30", "D2_45", "D2_60", "D2_90", "D3", "D4", "UTC_FL_DATE",
      "feature_utc_departure_hour_rounded", "feature_utc_arrival_hour_rounded", "feature_flight_unique_id",
      "ORIGIN_WBAN","DEST_WBAN","UTC_FL_DATE","UTC_ARR_DATE","UTC_CRS_DEP_TIME","UTC_ARR_TIME",
      "WEATHER_DELAY","NAS_DELAY")

    // Collecter toutes les colonnes nécessaires de toutes les expériences activées
    val baseColumns = configuration.enabledExperiments.flatMap { exp =>
      exp.featureExtraction.flightSelectedFeatures
        .map(_.keys.toSeq)
        .getOrElse(Seq.empty)
    }.distinct  :+ "ARR_DELAY_NEW"

    val selectedColumns = (baseColumns ++ extraColumns).distinct

    if (selectedColumns.isEmpty) {
      info("[DataPipeline] No flight feature selection configured - keeping all columns")
      df
    } else {
      // Colonnes essentielles à toujours garder (pour le join et le traitement)
      val essentialColumns = Seq(
        "FL_DATE", "OP_CARRIER_FL_NUM", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID",
        "CRS_DEP_TIME", "CRS_ARR_TIME", "DEP_DELAY", "ARR_DELAY_NEW",
        "ORIGIN_WBAN", "DEST_WBAN"
      )

      // Union des colonnes sélectionnées et essentielles
      val allRequiredColumns = (selectedColumns ++ essentialColumns).distinct
        .filter(df.columns.contains) // Ne garder que les colonnes qui existent

      info(s"[DataPipeline] Filtering flight columns: keeping ${allRequiredColumns.length} / ${df.columns.length} columns")
      whenDebug {
        info(s"  - Selected columns: ${allRequiredColumns.sorted.mkString(", ")}")
      }

      df.select(allRequiredColumns.head, allRequiredColumns.tail: _*)
    }
  }

  /**
   * Filtre les colonnes météo en fonction de la configuration de toutes les expériences activées
   * Garde uniquement les colonnes spécifiées dans weatherSelectedFeatures
   * + les colonnes essentielles pour le traitement
   * Gère le cas où weatherSelectedFeatures est vide ou inexistant
   * @param df DataFrame des données météo
   * @param configuration Configuration de l'application
   * @return DataFrame filtré
   */
  private def filterWeatherColumns(df: DataFrame)(implicit configuration: AppConfiguration): DataFrame = {
    // Collecter toutes les colonnes nécessaires de toutes les expériences activées
    val selectedColumns = configuration.enabledExperiments.flatMap { exp =>
      exp.featureExtraction.weatherSelectedFeatures
        .map(_.keys.toSeq)
        .getOrElse(Seq.empty)
    }.distinct

    if (selectedColumns.isEmpty) {
      info("[DataPipeline] No weather feature selection configured - keeping only essential WBAN, Date, Time")

      // Colonnes essentielles à toujours garder (pour le join et le traitement)
      val essentialColumns = Seq(
        "WBAN", "Date", "Time"
      )

      // Union des colonnes sélectionnées et essentielles
      val allRequiredColumns = (essentialColumns).distinct
        .filter(df.columns.contains) // Ne garder que les colonnes qui existent

      info(s"[DataPipeline] Filtering weather columns: keeping ${allRequiredColumns.length} / ${df.columns.length} columns")
      whenDebug {
        debug(s"  - Selected columns: ${allRequiredColumns.sorted.mkString(", ")}")
      }

      df.select(allRequiredColumns.head, allRequiredColumns.tail: _*)

    } else {
      // Colonnes essentielles à toujours garder (pour le join et le traitement)
      val essentialColumns = Seq(
        "WBAN", "Date", "Time"
      )

      // Union des colonnes sélectionnées et essentielles
      val allRequiredColumns = (selectedColumns ++ essentialColumns).distinct
        .filter(df.columns.contains) // Ne garder que les colonnes qui existent

      info(s"[DataPipeline] Filtering weather columns: keeping ${allRequiredColumns.length} / ${df.columns.length} columns")
      whenDebug {
        debug(s"  - Selected columns: ${allRequiredColumns.sorted.mkString(", ")}")
      }

      df.select(allRequiredColumns.head, allRequiredColumns.tail: _*)
    }
  }

}
