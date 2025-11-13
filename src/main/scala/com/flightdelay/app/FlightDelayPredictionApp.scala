package com.flightdelay.app

import com.flightdelay.config.{AppConfiguration, ConfigurationLoader, ExperimentConfig}
import com.flightdelay.data.DataPipeline
import com.flightdelay.features.FeaturePipeline
import com.flightdelay.ml.MLPipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.flightdelay.utils.DebugUtils._

/**
 * Flight Delay Prediction Application - Main Entry Point
 *
 * Pipeline:
 * 1. Data Pipeline (once, shared across experiments):
 *    - Load raw flight data
 *    - Load raw weather data
 *    - Load WBAN-Airport-Timezone mapping
 *    - Preprocess flight data (clean, enrich with WBAN, generate features, create labels)
 *    - Preprocess weather data
 * 2. For each enabled experiment:
 *    - Feature Pipeline (join flight/weather data, explode, extract features with optional PCA)
 *    - Train model
 *    - Evaluate model
 */
object FlightDelayPredictionApp {

  def main(args: Array[String]): Unit = {

    val appStartTime = System.currentTimeMillis()
    implicit val configuration: AppConfiguration = ConfigurationLoader.loadConfiguration(args)

    info("=" * 160)
    info("Flight Delay Prediction App Starting...")
    info("=" * 160)

    info(s"Configuration '${configuration.environment}' loaded successfully")


    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Flight Delay Prediction App")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    //Set CheckPoint Dir
    spark.sparkContext.setCheckpointDir(s"${configuration.common.output.basePath}/spark-checkpoints")
    // Réduire les logs pour plus de clarté
    spark.sparkContext.setLogLevel("WARN")


    // Get enabled experiments
    val enabledExperiments = configuration.enabledExperiments
    info("=" * 80)
    info(s"Found ${enabledExperiments.length} enabled experiments:")
    enabledExperiments.foreach { exp =>
      info(s"  - ${exp.name}: ${exp.description}")
    }

    // Parse tasks to execute
    val tasks = if (args.length > 1) {
      args(1).split(",").map(_.trim.toLowerCase).toSet
    } else {
      Set("data-pipeline", "feature-extraction", "train")
    }

    info(s"Tasks to execute: ${tasks.mkString(", ")}")

    try {

      // =====================================================================================
      // STEP 1: Data Pipeline (Load & Preprocess Flight + Weather Data)
      // =====================================================================================
      val (flightData, weatherData) = if (tasks.contains("data-pipeline")) {
        val (flights, weather) = DataPipeline.execute()
        info("[FlightDelayPredictionApp][STEP 1] Data pipeline (load & preprocess)... ")
        debug(f"- Final Flights dataset: ${flights.count()}%,d records with ${flights.columns.length}%3d columns")
        debug(f"- Final Weather dataset: ${weather.count()}%,d records with ${weather.columns.length}%3d columns")

        (flights, weather)
      } else {
        warn("[FlightDelayPredictionApp][STEP 1] Data pipeline (load & preprocess)... SKIPPED")
        warn("- Loading preprocessed data from parquet...")
        val flights = spark.read.parquet(s"${configuration.common.output.basePath}/common/data/processed_flights.parquet")
        val weather = spark.read.parquet(s"${configuration.common.output.basePath}/common/data/processed_weather.parquet")
        debug(f"- Loaded Flights: ${flights.count()}%,d records")
        debug(f"- Loaded Weathers: ${weather.count()}%,d records")
        (flights, weather)
      }

      // =====================================================================================
      // STEP 2: Process each enabled experiment sequentially
      // =====================================================================================
      enabledExperiments.zipWithIndex.foreach { case (experiment, index) =>
        info("=" * 80)
        info(s"EXPERIMENT ${index + 1}/${enabledExperiments.length}: ${experiment.name}")
        info("=" * 80)
        info(s"Description: ${experiment.description}")
        info(s"Model Type: ${experiment.model.modelType}")
        info(s"Feature Extraction: ${experiment.featureExtraction.featureType}")
        info("=" * 80 )

        try {
          runExperiment(experiment, tasks, flightData, weatherData)
        } catch {
          case ex: Exception =>
            error("=" * 80)
            error(s"✗ ERROR in Experiment: ${experiment.name}")
            error("=" * 80)
            error(s"Error message: ${ex.getMessage}")
            ex.printStackTrace()
            error("=" * 80 )
            error("Continuing with next experiment...")
        }
      }

      info("=" * 80)
      info("- Flight Delay Prediction App Completed Successfully!")
      info("=" * 80)

    } catch {
      case ex: Exception =>
        error("=" * 80)
        error("✗ ERROR in Application")
        error("=" * 80)
        error(s"Error message: ${ex.getMessage}")
        ex.printStackTrace()
        error("=" * 80 )
    } finally {
      val totalAppDuration = (System.currentTimeMillis() - appStartTime) / 1000.0

      info("=" * 80)
      info("Flight Delay Prediction App - Execution Summary")
      info("=" * 80)
      info(f"Total execution time: ${totalAppDuration}%.2f seconds (${totalAppDuration / 60}%.2f minutes)")
      info("=" * 80)

      spark.stop()
      info("Spark session stopped.\n")
    }
  }

  /**
   * Run a single experiment
   */
  private def runExperiment(
    experiment: ExperimentConfig,
    tasks: Set[String],
    flightData: DataFrame,
    weatherData: DataFrame
  )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {

    // =====================================================================================
    // STEP 2: Feature Pipeline (Join + Explode + Extract Features)
    // =====================================================================================
    if (tasks.contains("feature-extraction")) {
      info("-" * 80)
      info(s"[STEP 2] Feature Pipeline for ${experiment.name}")
      info("-" * 80)
      info(s"Feature Type: ${experiment.featureExtraction.featureType}")

      FeaturePipeline.execute(flightData, weatherData, experiment)

      info("-" * 80)
    } else {
      warn(s"[STEP 2] Feature pipeline for ${experiment.name}... SKIPPED")
    }

    // =====================================================================================
    // STEP 3: Train Model with K-Fold CV + Hold-out Test
    // =====================================================================================
    if (tasks.contains("train")) {
      info("-" * 80)
      info(s"[FlightDelayPredictionApp][STEP 3] Model Training for ${experiment.name}")
      info("-" * 80)

      // Train model using new MLPipeline (Option B: K-fold + Hold-out)
      val mlResult = MLPipeline.train(experiment)

      // Display summary
      info("-" * 80)
      info("Training Summary")
      info("-" * 80)
      info(f"Accuracy:          ${mlResult.holdOutMetrics.accuracy * 100}%6.2f%%")
      info(f"CV F1-Score:       ${mlResult.cvMetrics.avgF1 * 100}%6.2f%% ± ${mlResult.cvMetrics.stdF1 * 100}%.2f%%")
      info(f"Hold-out F1-Score: ${mlResult.holdOutMetrics.f1Score * 100}%6.2f%%")
      info(f"RECd (Delayed):    ${mlResult.holdOutMetrics.recallDelayed * 100}%6.2f%%")
      info(f"RECo (On-time):    ${mlResult.holdOutMetrics.recallOnTime * 100}%6.2f%%")
      info(f"Training time:     ${mlResult.trainingTimeSeconds}%.2f seconds")
      info("-" * 80)

    } else {
      warn(s"[FlightDelayPredictionApp][STEP 3] Training model for ${experiment.name}... SKIPPED")
    }

  }

}