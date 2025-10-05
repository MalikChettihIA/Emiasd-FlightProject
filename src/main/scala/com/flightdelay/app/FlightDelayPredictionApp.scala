package com.flightdelay.app

import com.flightdelay.config.{AppConfiguration, ConfigurationLoader, ExperimentConfig}
import com.flightdelay.data.preprocessing.FlightPreprocessingPipeline
import com.flightdelay.data.loaders.FlightDataLoader
import com.flightdelay.features.FlightFeatureExtractor
import com.flightdelay.ml.MLPipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
 * Flight Delay Prediction Application - Main Entry Point
 *
 * Pipeline per experiment:
 * 1. Load raw flight data (once, shared across experiments)
 * 2. Preprocess and engineer features (once, shared across experiments)
 * 3. For each enabled experiment:
 *    - Extract features with optional PCA
 *    - Train model
 *    - Evaluate model
 */
object FlightDelayPredictionApp {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Flight Delay Prediction App")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    // Réduire les logs pour plus de clarté
    spark.sparkContext.setLogLevel("WARN")

    println("\n" + "=" * 80)
    println("Flight Delay Prediction App Starting...")
    println("=" * 80)

    implicit val configuration: AppConfiguration = ConfigurationLoader.loadConfiguration(args)
    println(s"Configuration '${configuration.environment}' loaded successfully")

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
      Set("load", "preprocess", "feature-extraction", "train", "evaluate")
    }

    println(s"Tasks to execute: ${tasks.mkString(", ")}")
    println("=" * 80 + "\n")

    try {

      // =====================================================================================
      // STEP 1: Load Flight Data (once for all experiments)
      // =====================================================================================
      if (tasks.contains("load")) {
        println("\n" + "=" * 80)
        println("[STEP 1] Loading Flight Data")
        println("=" * 80)
        val flightData = FlightDataLoader.loadFromConfiguration()
        println(f"\n- Loaded ${flightData.count()}%,d flight records")
      } else {
        println("\n[STEP 1] Loading flight data... SKIPPED")
      }

      // =====================================================================================
      // STEP 2: Preprocess and Feature Engineering (once for all experiments)
      // =====================================================================================
      if (tasks.contains("preprocess")) {
        println("\n" + "=" * 80)
        println("[STEP 2] Preprocessing and Feature Engineering")
        println("=" * 80)
        val processedFlightData = FlightPreprocessingPipeline.execute()
        println(f"\n- Generated ${processedFlightData.columns.length}%3d columns (features + labels)")
        println("=" * 80 + "\n")
      } else {
        println("\n[STEP 2] Preprocessing and feature engineering... SKIPPED")
      }

      // =====================================================================================
      // STEP 3: Process each enabled experiment sequentially
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
          runExperiment(experiment, tasks)
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
      spark.stop()
      println("Spark session stopped.\n")
    }
  }

  /**
   * Run a single experiment
   */
  private def runExperiment(
    experiment: ExperimentConfig,
    tasks: Set[String]
  )(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {

    // =====================================================================================
    // STEP 3: Feature Extraction with Optional PCA
    // =====================================================================================
    if (tasks.contains("feature-extraction")) {
      println("\n" + "-" * 80)
      println(s"[STEP 3] Feature Extraction for ${experiment.name}")
      println("-" * 80)
      println(s"Feature Type: ${experiment.featureExtraction.featureType}")

      //Experiment output path
      val processedParquetPath = s"${configuration.common.output.basePath}/common/data/processed_flights.parquet"
      println(s"\nLoading preprocessed data:")
      println(s"  - Path: $processedParquetPath")
      val processedFlightData = spark.read.parquet(processedParquetPath)
      println(f"  - Loaded ${processedFlightData.count()}%,d preprocessed records")

      FlightFeatureExtractor.extract(processedFlightData, experiment)

      println("-" * 80 + "\n")
    } else {
      println(s"\n[STEP 3] Feature extraction for ${experiment.name}... SKIPPED")
    }

    // =====================================================================================
    // STEP 4: Train Model with K-Fold CV + Hold-out Test
    // =====================================================================================
    if (tasks.contains("train")) {
      println("\n" + "-" * 80)
      println(s"[STEP 4] Model Training for ${experiment.name}")
      println("-" * 80)

      // Load extracted features (unified path for both PCA and non-PCA)
      val featuresPath = s"${configuration.common.output.basePath}/${experiment.name}/features/extracted_features"

      println(s"\nLoading features:")
      println(s"  - Path: $featuresPath")
      val featuresData = spark.read.parquet(featuresPath)
      println(f"  - Loaded ${featuresData.count()}%,d feature records")

      // Train model using new MLPipeline (Option B: K-fold + Hold-out)
      val mlResult = MLPipeline.train(featuresData, experiment)

      // Display summary
      println("\n" + "-" * 80)
      println("Training Summary")
      println("-" * 80)
      println(f"CV F1-Score:       ${mlResult.cvMetrics.avgF1 * 100}%6.2f%% ± ${mlResult.cvMetrics.stdF1 * 100}%.2f%%")
      println(f"Hold-out F1-Score: ${mlResult.holdOutMetrics.f1Score * 100}%6.2f%%")
      println(f"Training time:     ${mlResult.trainingTimeSeconds}%.2f seconds")
      println("-" * 80 + "\n")

    } else {
      println(s"\n[STEP 4] Training model for ${experiment.name}... SKIPPED")
    }

    // =====================================================================================
    // STEP 5: Evaluate Model
    // =====================================================================================
    if (tasks.contains("evaluate")) {
      println(s"\n[STEP 5] Evaluating model for ${experiment.name}...")
      println("⚠ Evaluation not yet implemented")
    } else {
      println(s"\n[STEP 5] Evaluating model for ${experiment.name}... SKIPPED")
    }
  }

}