package com.flightdelay.app

import com.flightdelay.config.{AppConfiguration, ConfigurationLoader}
import com.flightdelay.data.preprocessing.FlightPreprocessingPipeline
import com.flightdelay.data.loaders.FlightDataLoader
import com.flightdelay.features.FlightFeatureExtractor
import com.flightdelay.ml.training.ModelTrainer
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
 * Flight Delay Prediction Application - Main Entry Point
 *
 * Pipeline:
 * 1. Load raw flight data
 * 2. Preprocess and engineer features
 * 3. Extract features with optional PCA (60% variance)
 * 4. Save results for ML training
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
      // STEP 1: Load Flight Data
      // =====================================================================================
      if (tasks.contains("load")) {
        println("\n[STEP 1] Loading flight data...")
        val flightData = FlightDataLoader.loadFromConfiguration()
        println(s"✓ Loaded ${flightData.count()} flight records")
      } else {
        println("\n[STEP 1] Loading flight data... SKIPPED")
      }

      // =====================================================================================
      // STEP 2: Preprocess and Feature Engineering
      // =====================================================================================
      if (tasks.contains("preprocess")) {
        println("\n[STEP 2] Preprocessing and feature engineering...")
        val processedFlightData = FlightPreprocessingPipeline.execute()
        println(s"✓ Generated ${processedFlightData.columns.length} columns (features + labels)")
      } else {
        println("\n[STEP 2] Preprocessing and feature engineering... SKIPPED")
      }

      // =====================================================================================
      // STEP 3: Feature Extraction with Optional PCA
      // =====================================================================================
      if (tasks.contains("feature-extraction")) {
        println(s"\n[STEP 3] Feature extraction (PCA: ${if (configuration.featureExtraction.pca) "ENABLED" else "DISABLED"})...")

        // Load preprocessed data from parquet
        val processedParquetPath = s"${configuration.output.data.path}/processed_flights.parquet"
        println(s"Loading preprocessed data from: $processedParquetPath")
        val processedFlightData = spark.read.parquet(processedParquetPath)
        println(s"✓ Loaded ${processedFlightData.count()} preprocessed records")

        if (configuration.featureExtraction.pca) {
          // Extract features with PCA
          val (extractedData, pcaModel, analysis) = FlightFeatureExtractor.extractWithPCA(
            processedFlightData,
            target = configuration.model.target,
            varianceThreshold = configuration.featureExtraction.pcaVarianceThreshold
          )

        } else {
          // Extract features without PCA
          val extractedData = FlightFeatureExtractor.extract(
            processedFlightData,
            target = configuration.model.target
          )


        }
      } else {
        println(s"\n[STEP 3] Feature extraction... SKIPPED")
      }

      // =====================================================================================
      // STEP 4: Train Model
      // =====================================================================================
      if (tasks.contains("train")) {
        println("\n[STEP 4] Training model...")

        // Determine which features to load based on PCA configuration
        val featuresPath = if (configuration.featureExtraction.pca) {
          s"${configuration.output.basePath}/features/pca_features_${configuration.model.target}"
        } else {
          s"${configuration.output.basePath}/features/base_features_${configuration.model.target}"
        }

        println(s"Loading features from: $featuresPath")
        val featuresData = spark.read.parquet(featuresPath)
        println(s"✓ Loaded ${featuresData.count()} feature records")

        // Train model using ModelTrainer
        val trainingResult = ModelTrainer.train(featuresData)

        println(s"\n✓ Model training completed successfully!")
        println(s"   - Model saved to: ${configuration.output.model.path}/${configuration.model.name}_${configuration.model.modelType}")
        println(f"   - Test Accuracy: ${trainingResult.testMetrics.accuracy * 100}%.2f%%")
        println(f"   - Test F1-Score: ${trainingResult.testMetrics.f1Score * 100}%.2f%%")
        println(f"   - Test AUC-ROC: ${trainingResult.testMetrics.areaUnderROC}%.4f")

      } else {
        println("\n[STEP 4] Training model... SKIPPED")
      }

      // =====================================================================================
      // STEP 5: Evaluate Model (TODO)
      // =====================================================================================
      if (tasks.contains("evaluate")) {
        println("\n[STEP 5] Evaluating model...")
        println("⚠ Evaluation not yet implemented")
      } else {
        println("\n[STEP 5] Evaluating model... SKIPPED")
      }

      println("\n" + "=" * 80)
      println("✓ Flight Delay Prediction App Completed Successfully!")
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

}