package com.flightdelay.app

import com.flightdelay.config.{AppConfiguration, ConfigurationLoader, ExperimentConfig}
import com.flightdelay.data.DataPipeline
import com.flightdelay.features.FeaturePipeline
import com.flightdelay.ml.MLPipeline
import com.flightdelay.utils.ExecutionTimeTracker
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

    // Create global execution time tracker for data processing (shared across experiments)
    val globalTimeTracker = ExecutionTimeTracker.create()

    info("=" * 160)
    info("Flight Delay Prediction App Starting...")
    info("=" * 160)

    info(s"Configuration '${configuration.environment}' loaded successfully")


    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Flight Delay Prediction App")
      // .master("local[*]")
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
      // Exécute (ou charge) la pipeline de données et retourne les DataFrames flights et weather
      val (flightData, weatherData) = if (tasks.contains("data-pipeline")) {
        // Si la tâche "data-pipeline" est demandée, lancer la pipeline qui charge et pré-traite les données
        val (flights, weather) = DataPipeline.execute(globalTimeTracker)
        info("[FlightDelayPredictionApp][STEP 1] Data pipeline (load & preprocess)... ")

        // Debug : afficher le nombre d'enregistrements et de colonnes (attention : count() déclenche une action Spark)
        debug(f"- Final Flights dataset: ${flights.count()}%,d records with ${flights.columns.length}%3d columns")
        debug(f"- Final Weather dataset: ${weather.count()}%,d records with ${weather.columns.length}%3d columns")

        (flights, weather)
      } else {
        // Sinon, on saute la pipeline et on charge les données pré-traitées depuis des fichiers parquet
        warn("[FlightDelayPredictionApp][STEP 1] Data pipeline (load & preprocess)... SKIPPED")
        warn("- Loading preprocessed data from parquet...")

        val flights = spark.read.parquet(s"${configuration.common.output.basePath}/common/data/processed_flights.parquet")
        val weather = spark.read.parquet(s"${configuration.common.output.basePath}/common/data/processed_weather.parquet")

        // Debug : compter les enregistrements chargés (également une action Spark)
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
          // Create a new time tracker for this experiment
          val experimentTimeTracker = ExecutionTimeTracker.create()

          // Copy data processing metrics from global tracker to experiment tracker
          globalTimeTracker.getAllTimes.foreach { case (stepName, time) =>
            if (stepName.startsWith("data_processing.")) {
              experimentTimeTracker.setStepTime(stepName, time)
            }
          }

          runExperiment(experiment, tasks, flightData, weatherData, experimentTimeTracker)
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

      // Display global execution time summary table (data processing only)
      info("")
      info("=" * 90)
      info("DISPLAYING GLOBAL EXECUTION TIME SUMMARY (DATA PROCESSING)")
      info("=" * 90)
      globalTimeTracker.displaySummaryTable()

      // Save global execution time metrics to CSV and TXT (common level)
      try {
        val commonMetricsPath = s"${configuration.common.output.basePath}/execution_time_metrics"
        info("=" * 90)
        info("SAVING GLOBAL EXECUTION TIME METRICS (COMMON LEVEL)")
        info("=" * 90)
        info(s"  Saving to: $commonMetricsPath")

        globalTimeTracker.saveToCSV(s"$commonMetricsPath/execution_times.csv")
        info(s"  - CSV file saved: $commonMetricsPath/execution_times.csv")

        globalTimeTracker.saveToText(s"$commonMetricsPath/execution_times.txt")
        info(s"  - TXT file saved: $commonMetricsPath/execution_times.txt")

        info("=" * 90)
      } catch {
        case ex: Exception =>
          error(s"Error saving global execution time metrics: ${ex.getMessage}")
          ex.printStackTrace()
      }

      spark.stop()
      info("Spark session stopped.\n")
    }
  }
  /*Fin du main */

  /**
   * Run a single experiment
   */
  private def runExperiment(
    experiment: ExperimentConfig,
    tasks: Set[String],
    flightData: DataFrame,
    weatherData: DataFrame,
    timeTracker: ExecutionTimeTracker
  )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {

    if (!(tasks.contains("feature-extraction")) && !(tasks.contains("train"))){
      return
    }
    // =====================================================================================
    // STEP 2: Feature Pipeline (Join + Explode + Extract Features)
    // =====================================================================================
    val (trainData, testData) = if (tasks.contains("feature-extraction")) {
      info(s"[STEP 2] Feature Pipeline for ${experiment.name}")
      info("=" * 80)
      info(s"Feature Type: ${experiment.featureExtraction.featureType}")

      val (train, test) = FeaturePipeline.execute(flightData, weatherData, experiment, timeTracker)

      info("Checkpointing prepared data to cut lineage and optimize performance...")
      val trainData = train.checkpoint()
      val testData = test.checkpoint()
      info("Checkpoint completed!")
      info("=" * 80)
      (trainData, testData)
    } else {
      warn(s"[STEP 2] Feature pipeline for ${experiment.name}... SKIPPED")
      val trainPath = s"${configuration.common.output.basePath}/${experiment.name}/data/join_exploded_train_prepared.parquet"
      val testPath = s"${configuration.common.output.basePath}/${experiment.name}/data/join_exploded_test_prepared.parquet"

      warn(s"Loading prepared data:")
      warn(s"  - Train: $trainPath")
      warn(s"  - Test:  $testPath")
      val trainData = spark.read.parquet(trainPath)
      val testData = spark.read.parquet(testPath)
      (trainData, testData)
    }

    // =====================================================================================
    // STEP 3: Train Model with K-Fold CV + Hold-out Test
    // =====================================================================================
    if (tasks.contains("train")) {
      info("-" * 80)
      info(s"[FlightDelayPredictionApp][STEP 3] Model Training for ${experiment.name}")
      info("-" * 80)

      // Train model using new MLPipeline (pre-split balanced datasets)
      val mlResult = MLPipeline.train(trainData, testData, experiment, experiment.train.fast, timeTracker)

      // Display summary
      info("-" * 80)
      info("Training Summary")
      info("-" * 80)
      info(f"Accuracy:          ${mlResult.holdOutMetrics.accuracy * 100}%6.2f%%")
      if (!experiment.train.fast) {
        info(f"CV F1-Score:       ${mlResult.cvMetrics.avgF1 * 100}%6.2f%% ± ${mlResult.cvMetrics.stdF1 * 100}%.2f%%")
      }
      info(f"Hold-out F1-Score: ${mlResult.holdOutMetrics.f1Score * 100}%6.2f%%")
      info(f"RECd (Delayed):    ${mlResult.holdOutMetrics.recallDelayed * 100}%6.2f%%")
      info(f"RECo (On-time):    ${mlResult.holdOutMetrics.recallOnTime * 100}%6.2f%%")
      info(f"Training time:     ${mlResult.trainingTimeSeconds}%.2f seconds")
      info("-" * 80)

    } else {
      warn(s"[FlightDelayPredictionApp][STEP 3] Training model for ${experiment.name}... SKIPPED")
    }

    // =====================================================================================
    // Save execution time metrics at experiment level
    // =====================================================================================
    try {
      val experimentMetricsPath = s"${configuration.common.output.basePath}/${experiment.name}/execution_time"
      info("=" * 90)
      info(s"SAVING EXECUTION TIME METRICS FOR ${experiment.name}")
      info("=" * 90)
      info(s"  Saving to: $experimentMetricsPath")

      timeTracker.saveToCSV(s"$experimentMetricsPath/execution_times.csv")
      info(s"  - CSV file saved: $experimentMetricsPath/execution_times.csv")

      timeTracker.saveToText(s"$experimentMetricsPath/execution_times.txt")
      info(s"  - TXT file saved: $experimentMetricsPath/execution_times.txt")

      info("=" * 90)
    } catch {
      case ex: Exception =>
        error(s"Error saving execution time metrics for ${experiment.name}: ${ex.getMessage}")
        ex.printStackTrace()
    }

  }

}