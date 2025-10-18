package com.flightdelay.app

import com.flightdelay.config.{AppConfiguration, ConfigurationLoader, ExperimentConfig}
import com.flightdelay.data.DataPipeline
import com.flightdelay.features.FeaturePipeline
import com.flightdelay.ml.MLPipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

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

    println("\n" + "=" * 80)
    println("Flight Delay Prediction App Starting...")
    println("=" * 80)

    implicit val configuration: AppConfiguration = ConfigurationLoader.loadConfiguration(args)
    println(s"Configuration '${configuration.environment}' loaded successfully")


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
    println(s"Found ${enabledExperiments.length} enabled experiments:")
    enabledExperiments.foreach { exp =>
      println(s"  - ${exp.name}: ${exp.description}")
    }

    // Parse tasks to execute
    val tasks = if (args.length > 1) {
      args(1).split(",").map(_.trim.toLowerCase).toSet
    } else {
      Set("data-pipeline", "feature-extraction", "train", "evaluate")
    }

    println(s"Tasks to execute: ${tasks.mkString(", ")}")
    println("=" * 80 + "\n")

    try {

      // =====================================================================================
      // STEP 1: Data Pipeline (Load & Preprocess Flight + Weather Data)
      // =====================================================================================
      val (flightData, weatherData) = if (tasks.contains("data-pipeline")) {
        val (flights, weather) = DataPipeline.execute()
        println(f"\n- Final Flights dataset: ${flights.count()}%,d records with ${flights.columns.length}%3d columns")
        weather match {
          case Some(w) =>
            println(f"\n- Final Weather dataset: ${w.count()}%,d records with ${w.columns.length}%3d columns")
          case None =>
            println(f"\n- Weather data: DISABLED (no weather features configured)")
        }
        (flights, weather)
      } else {
        println("\n[STEP 1] Data pipeline (load & preprocess)... SKIPPED")
        println("\n[STEP 1] Loading preprocessed data from parquet...")
        val flights = spark.read.parquet(s"${configuration.common.output.basePath}/common/data/processed_flights.parquet")

        // Check if ANY ENABLED experiment needs weather data
        val isWeatherNeeded = configuration.enabledExperiments.exists(_.featureExtraction.isWeatherEnabled)
        val weather = if (isWeatherNeeded) {
          val w = spark.read.parquet(s"${configuration.common.output.basePath}/common/data/processed_weather.parquet")
          println(f"- Loaded Weather: ${w.count()}%,d records")
          Some(w)
        } else {
          println(f"- Weather data: SKIPPED (no weather features configured)")
          None
        }

        println(f"- Loaded Flights: ${flights.count()}%,d records")
        (flights, weather)
      }

      // =====================================================================================
      // STEP 2: Process each enabled experiment sequentially
      // =====================================================================================
      enabledExperiments.zipWithIndex.foreach { case (experiment, index) =>
        println("\n" + "=" * 80)
        println(s"EXPERIMENT ${index + 1}/${enabledExperiments.length}: ${experiment.name}")
        println("=" * 80)
        println(s"Description: ${experiment.description}")
        println(s"Target: ${experiment.target}")
        println(s"Model Type: ${experiment.model.modelType}")
        println(s"Feature Extraction: ${experiment.featureExtraction.featureType}")
        println("=" * 80 + "\n")

        try {
          runExperiment(experiment, tasks, flightData, weatherData)
        } catch {
          case ex: Exception =>
            println("\n" + "=" * 80)
            println(s"✗ ERROR in Experiment: ${experiment.name}")
            println("=" * 80)
            println(s"Error message: ${ex.getMessage}")
            ex.printStackTrace()
            println("=" * 80 + "\n")
            println("Continuing with next experiment...\n")
        }
      }

      println("\n" + "=" * 80)
      println("- Flight Delay Prediction App Completed Successfully!")
      println("=" * 80 + "\n")

    } catch {
      case ex: Exception =>
        println("\n" + "=" * 80)
        println("✗ ERROR in Application")
        println("=" * 80)
        println(s"Error message: ${ex.getMessage}")
        ex.printStackTrace()
        println("=" * 80 + "\n")
    } finally {
      val totalAppDuration = (System.currentTimeMillis() - appStartTime) / 1000.0

      println("\n" + "=" * 80)
      println("Flight Delay Prediction App - Execution Summary")
      println("=" * 80)
      println(f"Total execution time: ${totalAppDuration}%.2f seconds (${totalAppDuration / 60}%.2f minutes)")
      println("=" * 80 + "\n")

      spark.stop()
      println("Spark session stopped.\n")
    }
  }

  /**
   * Run a single experiment
   */
  private def runExperiment(
    experiment: ExperimentConfig,
    tasks: Set[String],
    flightData: DataFrame,
    weatherData: Option[DataFrame]
  )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {

    // =====================================================================================
    // STEP 2: Feature Pipeline (Join + Explode + Extract Features)
    // =====================================================================================
    if (tasks.contains("feature-extraction")) {
      println("\n" + "-" * 80)
      println(s"[STEP 2] Feature Pipeline for ${experiment.name}")
      println("-" * 80)
      println(s"Feature Type: ${experiment.featureExtraction.featureType}")

      FeaturePipeline.execute(flightData, weatherData, experiment)

      println("-" * 80 + "\n")
    } else {
      println(s"\n[STEP 2] Feature pipeline for ${experiment.name}... SKIPPED")
    }

    // =====================================================================================
    // STEP 3: Train Model with K-Fold CV + Hold-out Test
    // =====================================================================================
    if (tasks.contains("train")) {
      println("\n" + "-" * 80)
      println(s"[STEP 3] Model Training for ${experiment.name}")
      println("-" * 80)

      // Train model using new MLPipeline (Option B: K-fold + Hold-out)
      val mlResult = MLPipeline.train(experiment)

      // Display summary
      println("\n" + "-" * 80)
      println("Training Summary")
      println("-" * 80)
      println(f"CV F1-Score:       ${mlResult.cvMetrics.avgF1 * 100}%6.2f%% ± ${mlResult.cvMetrics.stdF1 * 100}%.2f%%")
      println(f"Hold-out F1-Score: ${mlResult.holdOutMetrics.f1Score * 100}%6.2f%%")
      println(f"Training time:     ${mlResult.trainingTimeSeconds}%.2f seconds")
      println("-" * 80 + "\n")

    } else {
      println(s"\n[STEP 3] Training model for ${experiment.name}... SKIPPED")
    }

    // =====================================================================================
    // STEP 4: Evaluate Model
    // =====================================================================================
    if (tasks.contains("evaluate")) {
      println(s"\n[STEP 4] Evaluating model for ${experiment.name}...")
      println("⚠ Evaluation not yet implemented")
    } else {
      println(s"\n[STEP 4] Evaluating model for ${experiment.name}... SKIPPED")
    }
  }

}