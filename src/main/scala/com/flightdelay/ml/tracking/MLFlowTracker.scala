package com.flightdelay.ml.tracking

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}

import org.mlflow.tracking.MlflowClient
import org.mlflow.api.proto.Service.RunStatus
import org.apache.spark.ml.{PipelineModel, Model}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, LogisticRegressionModel, GBTClassificationModel}
import org.apache.spark.ml.util.MLWritable

import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}
import java.nio.file.{Files, Paths}

/**
 * MLFlow Tracker - Centralized experiment tracking with MLFlow
 *
 * This object provides methods to:
 * 1. Create and manage MLFlow experiments
 * 2. Start and end runs
 * 3. Log parameters, metrics, and artifacts
 * 4. Log datasets used in training/evaluation
 * 5. Log and register models
 * 6. Handle errors gracefully
 */
object MLFlowTracker {

  private var client: Option[MlflowClient] = None
  private var enabled: Boolean = false
  private var trackingUri: String = "http://localhost:5555"
  private val experimentName = "flight-delay-prediction"

  /**
   * Initialize MLFlow client with tracking URI
   * @param uri MLFlow tracking server URI
   * @param enable Enable/disable tracking
   */
  def initialize(uri: String, enable: Boolean = true): Unit = {
    enabled = enable
    if (enabled) {
      trackingUri = uri
      System.setProperty("MLFLOW_TRACKING_URI", trackingUri)
      client = Some(new MlflowClient(trackingUri))
      println(s"[MLFlow] Initialized with tracking URI: $trackingUri")
    } else {
      println(s"[MLFlow] Tracking disabled")
    }
  }

  /**
   * Get or create MLFlow experiment
   * @return Experiment ID
   */
  def getOrCreateExperiment(): Option[String] = {
    if (!enabled || client.isEmpty) return None

    Try {
      val expOptJava = client.get.getExperimentByName(experimentName)
      val expId: String = if (expOptJava.isPresent) {
        expOptJava.get().getExperimentId
      } else {
        client.get.createExperiment(experimentName)
      }
      println(s"[MLFlow] Using experiment: $experimentName (ID: $expId)")
      expId
    } match {
      case Success(id) => Some(id)
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to get/create experiment: ${e.getMessage}")
        None
    }
  }

  /**
   * Start a new MLFlow run
   * @param experimentId Experiment ID
   * @param runName Run name
   * @param description Optional run description
   * @return Run ID if successful
   */
  def startRun(
                experimentId: String,
                runName: String,
                description: Option[String] = None
              ): Option[String] = {
    if (!enabled || client.isEmpty) return None

    Try {
      val run = client.get.createRun(experimentId)
      val runId = run.getRunId

      // Set run name as a tag
      client.get.setTag(runId, "mlflow.runName", runName)

      // Set description if provided
      description.foreach { desc =>
        setDescription(runId, desc)
      }

      println(s"[MLFlow] Started run: $runName (ID: $runId)")
      description.foreach(desc => println(s"[MLFlow]   Description: ${desc.take(100)}..."))

      runId.toString
    } match {
      case Success(id: String) => Some(id)
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to start run: ${e.getMessage}")
        None
    }
  }

  /**
   * Set or update the description of a run
   *
   * @param runId Run ID
   * @param description Description text (supports markdown)
   */
  def setDescription(runId: String, description: String): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      client.get.setTag(runId, "mlflow.note.content", description)
    } match {
      case Success(_) => // Silent success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to set description: ${e.getMessage}")
    }
  }

  /**
   * Append text to an existing run description
   *
   * @param runId Run ID
   * @param additionalText Text to append
   */
  def appendToDescription(runId: String, additionalText: String): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      val run = client.get.getRun(runId)
      val tags = run.getData.getTagsList.asScala
      val currentDesc = tags
        .find(_.getKey == "mlflow.note.content")
        .map(_.getValue)
        .getOrElse("")

      val newDesc = if (currentDesc.isEmpty) {
        additionalText
      } else {
        currentDesc + "\n\n" + additionalText
      }

      setDescription(runId, newDesc)
    } match {
      case Success(_) => // Silent success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to append to description: ${e.getMessage}")
    }
  }

  /**
   * Log a dataset used in the run
   *
   * @param runId Run ID
   * @param datasetName Name of the dataset
   * @param datasetPath Path or URI to the dataset
   * @param datasetType Type of dataset (e.g., "training", "validation", "test")
   * @param numRows Number of rows in the dataset
   * @param numCols Number of columns in the dataset
   * @param metadata Additional metadata about the dataset
   */
  def logDataset(
                  runId: String,
                  datasetName: String,
                  datasetPath: String,
                  datasetType: String = "training",
                  numRows: Option[Long] = None,
                  numCols: Option[Int] = None,
                  metadata: Map[String, String] = Map.empty
                ): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      val prefix = s"dataset.${datasetType}"

      client.get.setTag(runId, s"${prefix}.name", datasetName)
      client.get.setTag(runId, s"${prefix}.path", datasetPath)

      numRows.foreach(rows =>
        client.get.setTag(runId, s"${prefix}.num_rows", rows.toString)
      )

      numCols.foreach(cols =>
        client.get.setTag(runId, s"${prefix}.num_cols", cols.toString)
      )

      metadata.foreach { case (key, value) =>
        client.get.setTag(runId, s"${prefix}.${key}", value)
      }

      println(s"[MLFlow] Logged dataset: $datasetName ($datasetType)")

    } match {
      case Success(_) => // Silent success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to log dataset: ${e.getMessage}")
    }
  }

  /**
   * Log multiple datasets at once
   */
  def logDatasets(
                   runId: String,
                   datasets: Seq[(String, String, String, Option[Long], Option[Int], Map[String, String])]
                 ): Unit = {
    if (!enabled || client.isEmpty) return

    datasets.foreach { case (name, path, datasetType, numRows, numCols, metadata) =>
      logDataset(runId, name, path, datasetType, numRows, numCols, metadata)
    }
  }

  /**
   * Helper method to log training/validation/test split datasets
   */
  def logDatasetSplit(
                       runId: String,
                       trainPath: String,
                       trainRows: Long,
                       valPath: Option[String] = None,
                       valRows: Option[Long] = None,
                       testPath: Option[String] = None,
                       testRows: Option[Long] = None,
                       numCols: Option[Int] = None
                     ): Unit = {
    if (!enabled || client.isEmpty) return

    logDataset(runId, "training_data", trainPath, "training", Some(trainRows), numCols)
    valPath.foreach(path => logDataset(runId, "validation_data", path, "validation", valRows, numCols))
    testPath.foreach(path => logDataset(runId, "test_data", path, "test", testRows, numCols))

    val splitSummary = s"Train: $trainRows" +
      valRows.map(v => s" | Val: $v").getOrElse("") +
      testRows.map(t => s" | Test: $t").getOrElse("")

    client.get.setTag(runId, "dataset.split_summary", splitSummary)
    println(s"[MLFlow] Logged dataset split: $splitSummary")
  }

  /**
   * Log dataset statistics
   */
  def logDatasetStats(
                       runId: String,
                       datasetType: String,
                       stats: Map[String, Any]
                     ): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      val prefix = s"dataset.${datasetType}.stats"
      stats.foreach { case (key, value) =>
        client.get.setTag(runId, s"${prefix}.${key}", value.toString)
      }
    } match {
      case Success(_) => // Silent success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to log dataset stats: ${e.getMessage}")
    }
  }

  /**
   * Log a Spark ML model to MLFlow
   *
   * @param runId Run ID
   * @param model Spark ML model (must be MLWritable)
   * @param modelName Name of the model
   * @param modelPath Local path where to save the model temporarily
   * @param modelType Type of model (e.g., "RandomForest", "LogisticRegression")
   * @param metadata Additional model metadata
   */
  def logModel[M <: Model[M] with MLWritable](
                                               runId: String,
                                               model: M,
                                               modelName: String,
                                               modelPath: String,
                                               modelType: String,
                                               metadata: Map[String, String] = Map.empty
                                             ): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      println(s"[MLFlow] Logging model: $modelName ($modelType)")

      // Save model to local path
      val localModelPath = s"$modelPath/$modelName"
      model.write.overwrite().save(localModelPath)
      println(s"[MLFlow]   Saved model to: $localModelPath")

      // Log model as artifact
      logArtifact(runId, localModelPath)

      // Log model metadata as tags
      client.get.setTag(runId, "model.name", modelName)
      client.get.setTag(runId, "model.type", modelType)
      client.get.setTag(runId, "model.path", localModelPath)

      // Log model-specific parameters
      extractModelParams(model).foreach { case (key, value) =>
        client.get.setTag(runId, s"model.param.$key", value)
      }

      // Log additional metadata
      metadata.foreach { case (key, value) =>
        client.get.setTag(runId, s"model.metadata.$key", value)
      }

      println(s"[MLFlow] ✓ Model logged successfully")

    } match {
      case Success(_) => // Success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to log model: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  /**
   * Log a PipelineModel (full ML pipeline including preprocessing)
   */
  def logPipelineModel(
                        runId: String,
                        pipelineModel: PipelineModel,
                        modelName: String,
                        modelPath: String,
                        metadata: Map[String, String] = Map.empty
                      ): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      println(s"[MLFlow] Logging pipeline model: $modelName")

      val localModelPath = s"$modelPath/$modelName"
      pipelineModel.write.overwrite().save(localModelPath)

      logArtifact(runId, localModelPath)

      client.get.setTag(runId, "model.name", modelName)
      client.get.setTag(runId, "model.type", "PipelineModel")
      client.get.setTag(runId, "model.path", localModelPath)
      client.get.setTag(runId, "model.stages", pipelineModel.stages.length.toString)

      // Log stages
      pipelineModel.stages.zipWithIndex.foreach { case (stage, idx) =>
        client.get.setTag(runId, s"model.stage.$idx", stage.getClass.getSimpleName)
      }

      metadata.foreach { case (key, value) =>
        client.get.setTag(runId, s"model.metadata.$key", value)
      }

      println(s"[MLFlow] ✓ Pipeline model logged successfully")

    } match {
      case Success(_) => // Success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to log pipeline model: ${e.getMessage}")
    }
  }

  /**
   * Register a model in MLFlow Model Registry
   *
   * @param runId Run ID
   * @param modelName Name to register the model under
   * @param modelPath Path to the model artifacts
   * @param description Model description
   * @param tags Additional tags for the registered model
   */
  def registerModel(
                     runId: String,
                     modelName: String,
                     modelPath: String,
                     description: Option[String] = None,
                     tags: Map[String, String] = Map.empty
                   ): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      println(s"[MLFlow] Registering model: $modelName")

      // Create or get registered model
      val modelUri = s"runs:/$runId/$modelName"

      // Note: Model registry requires MLFlow server with backend store
      // This is a simplified version - full implementation requires MLFlow Registry API

      client.get.setTag(runId, "model.registered", "true")
      client.get.setTag(runId, "model.registry_name", modelName)

      description.foreach { desc =>
        client.get.setTag(runId, "model.registry_description", desc)
      }

      tags.foreach { case (key, value) =>
        client.get.setTag(runId, s"model.registry_tag.$key", value)
      }

      println(s"[MLFlow] ✓ Model registration metadata logged")
      println(s"[MLFlow]   Model URI: $modelUri")

    } match {
      case Success(_) => // Success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to register model: ${e.getMessage}")
    }
  }

  /**
   * Log model performance metrics
   *
   * @param runId Run ID
   * @param metrics Performance metrics (accuracy, f1, etc.)
   * @param stage Stage of evaluation (train, validation, test)
   */
  def logModelMetrics(
                       runId: String,
                       metrics: Map[String, Double],
                       stage: String = "validation"
                     ): Unit = {
    if (!enabled || client.isEmpty) return

    metrics.foreach { case (key, value) =>
      logMetric(runId, s"${stage}_${key}", value)
    }

    println(s"[MLFlow] Logged ${metrics.size} metrics for stage: $stage")
  }

  /**
   * Extract model parameters for logging
   */
  private def extractModelParams(model: Any): Map[String, String] = {
    model match {
      case rf: RandomForestClassificationModel =>
        Map(
          "numTrees" -> rf.getNumTrees.toString,
          "maxDepth" -> rf.getMaxDepth.toString,
          "numFeatures" -> rf.numFeatures.toString,
          "featureImportances" -> rf.featureImportances.toArray.take(10).mkString(",")
        )

      case lr: LogisticRegressionModel =>
        Map(
          "numFeatures" -> lr.numFeatures.toString,
          "numClasses" -> lr.numClasses.toString,
          "threshold" -> lr.getThreshold.toString
        )

      case gbt: GBTClassificationModel =>
        Map(
          "numTrees" -> gbt.getNumTrees.toString,
          "maxDepth" -> gbt.getMaxDepth.toString,
          "numFeatures" -> gbt.numFeatures.toString
        )

      case _ =>
        Map("type" -> model.getClass.getSimpleName)
    }
  }

  /**
   * Log feature importances
   *
   * @param runId Run ID
   * @param featureNames Names of features
   * @param importances Feature importance values
   * @param topN Number of top features to log (default: all)
   */
  def logFeatureImportances(
                             runId: String,
                             featureNames: Array[String],
                             importances: Array[Double],
                             topN: Option[Int] = None
                           ): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      val featuresWithImportances = featureNames.zip(importances)
        .sortBy(-_._2)
        .take(topN.getOrElse(featureNames.length))

      featuresWithImportances.zipWithIndex.foreach { case ((name, importance), idx) =>
        client.get.setTag(runId, s"feature_importance.rank_${idx + 1}", s"$name: ${importance}")
      }

      // Log top 10 as metrics for easy comparison
      featuresWithImportances.take(10).zipWithIndex.foreach { case ((name, importance), idx) =>
        logMetric(runId, s"feature_importance_${idx + 1}_${name}", importance)
      }

      println(s"[MLFlow] Logged feature importances (top ${featuresWithImportances.length})")

    } match {
      case Success(_) => // Success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to log feature importances: ${e.getMessage}")
    }
  }

  /**
   * Log parameters to MLFlow
   */
  def logParams(runId: String, params: Map[String, Any]): Unit = {
    if (!enabled || client.isEmpty) return

    params.foreach { case (key, value) =>
      Try {
        client.get.logParam(runId, key, value.toString)
      } match {
        case Success(_) => // Silent success
        case Failure(e) =>
          println(s"[MLFlow] Warning: Failed to log param $key: ${e.getMessage}")
      }
    }
  }

  /**
   * Log a single metric to MLFlow
   */
  def logMetric(runId: String, key: String, value: Double, step: Long = 0): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      client.get.logMetric(runId, key, value, System.currentTimeMillis(), step)
    } match {
      case Success(_) => // Silent success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to log metric $key: ${e.getMessage}")
    }
  }

  /**
   * Log multiple metrics to MLFlow
   */
  def logMetrics(runId: String, metrics: Map[String, Double], step: Long = 0): Unit = {
    if (!enabled || client.isEmpty) return

    metrics.foreach { case (key, value) =>
      logMetric(runId, key, value, step)
    }
  }

  /**
   * Log an artifact (file or directory) to MLFlow
   */
  def logArtifact(runId: String, localPath: String): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      val file = new java.io.File(localPath)
      if (file.exists()) {
        if (file.isDirectory) {
          client.get.logArtifacts(runId, file)
        } else {
          client.get.logArtifact(runId, file)
        }
      } else {
        println(s"[MLFlow] Warning: Artifact not found: $localPath")
      }
    } match {
      case Success(_) => // Silent success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to log artifact $localPath: ${e.getMessage}")
    }
  }

  /**
   * Log an artifact (file or directory) to MLFlow with a custom artifact path (subdirectory)
   *
   * @param runId Run ID
   * @param localPath Local file or directory path
   * @param artifactPath Subdirectory in artifacts (e.g., "metrics", "configuration")
   */
  def logArtifactWithPath(runId: String, localPath: String, artifactPath: String): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      val file = new java.io.File(localPath)
      if (file.exists()) {
        if (file.isDirectory) {
          client.get.logArtifacts(runId, file, artifactPath)
        } else {
          client.get.logArtifact(runId, file, artifactPath)
        }
      } else {
        println(s"[MLFlow] Warning: Artifact not found: $localPath")
      }
    } match {
      case Success(_) => // Silent success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to log artifact $localPath to $artifactPath: ${e.getMessage}")
    }
  }

  /**
   * Set a tag on a run
   */
  def setTag(runId: String, key: String, value: String): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      client.get.setTag(runId, key, value)
    } match {
      case Success(_) => // Silent success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to set tag $key: ${e.getMessage}")
    }
  }

  /**
   * Set multiple tags on a run
   */
  def setTags(runId: String, tags: Map[String, String]): Unit = {
    if (!enabled || client.isEmpty) return

    tags.foreach { case (key, value) =>
      setTag(runId, key, value)
    }
  }

  /**
   * End a MLFlow run
   */
  def endRun(runId: String, status: RunStatus = RunStatus.FINISHED): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      client.get.setTerminated(runId, status)
      println(s"[MLFlow] Ended run: $runId (status: $status)")
    } match {
      case Success(_) => // Success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to end run: ${e.getMessage}")
    }
  }

  /*
   * Fonction helper pour logger les datasets depuis la config
   */
  def logDatasetsFromConfig(runId: String, config: AppConfiguration): Unit = {

    println("\n[MLFlow] Logging datasets from configuration...")

    // Flight dataset
    MLFlowTracker.logDataset(
      runId = runId,
      datasetName = "flights_raw",
      datasetPath = config.common.data.flight.path,
      datasetType = "raw",
      metadata = Map(
        "source" -> "DOT BTS",
        "format" -> "csv",
        "base_path" -> config.common.data.basePath
      )
    )

    // Weather dataset
    MLFlowTracker.logDataset(
      runId = runId,
      datasetName = "weather_raw",
      datasetPath = config.common.data.weather.path,
      datasetType = "raw",
      metadata = Map(
        "source" -> "NOAA",
        "format" -> "txt",
        "base_path" -> config.common.data.basePath
      )
    )

    // Airport mapping dataset
    MLFlowTracker.logDataset(
      runId = runId,
      datasetName = "airport_mapping",
      datasetPath = config.common.data.airportMapping.path,
      datasetType = "reference",
      metadata = Map(
        "type" -> "WBAN to Airport mapping",
        "format" -> "csv"
      )
    )

    // Processed datasets (if available)
    val processedFlightsPath = s"${config.common.output.basePath}/common/data/processed_flights.parquet"
    MLFlowTracker.logDataset(
      runId = runId,
      datasetName = "flights_processed",
      datasetPath = processedFlightsPath,
      datasetType = "processed",
      metadata = Map(
        "format" -> "parquet",
        "includes_labels" -> "true",
        "includes_features" -> "true"
      )
    )

    val processedWeatherPath = s"${config.common.output.basePath}/common/data/processed_weather.parquet"
    MLFlowTracker.logDataset(
      runId = runId,
      datasetName = "weather_processed",
      datasetPath = processedWeatherPath,
      datasetType = "processed",
      metadata = Map(
        "format" -> "parquet",
        "normalized_time" -> "true"
      )
    )

    println("[MLFlow] ✓ Datasets logged from configuration")
  }

  /**
   * Set special MLFlow tags that appear in UI columns
   */
  def setUIMetadata(
                     runId: String,
                     datasetName: Option[String] = None,
                     modelName: Option[String] = None,
                     description: Option[String] = None
                   ): Unit = {

    // ⭐ Dataset column - use tag "mlflow.datasets"
    datasetName.foreach { name =>
      setTag(runId, "mlflow.datasets", name)
    }

    // ⭐ Models column - use tag "mlflow.log-model.history"
    modelName.foreach { name =>
      setTag(runId, "mlflow.logged_models", name)
    }

    // ⭐ Description column - use tag "mlflow.note.content"
    description.foreach { desc =>
      setDescription(runId, desc)
    }
  }

  /**
   * Set metadata that appears in MLFlow UI columns
   *
   * This method sets special tags that MLFlow UI recognizes:
   * - Dataset column: Shows dataset information
   * - Models column: Shows registered model name
   * - Description column: Shows run description
   *
   * @param runId Run ID
   * @param datasetInfo Dataset information (name, source, etc.)
   * @param modelInfo Model information (name, version, etc.)
   * @param runDescription Short description for the run (shows in Description column)
   */
  def setRunUIMetadata(
                        runId: String,
                        datasetInfo: Option[Map[String, String]] = None,
                        modelInfo: Option[Map[String, String]] = None,
                        runDescription: Option[String] = None
                      ): Unit = {
    if (!enabled || client.isEmpty) return

    Try {
      println(s"[MLFlow] Setting UI metadata for run: $runId")

      // ⭐ DATASET COLUMN
      datasetInfo.foreach { info =>
        val datasetSummary = info.map { case (k, v) => s"$k: $v" }.mkString(", ")

        // MLFlow UI recognizes these tags
        setTag(runId, "mlflow.datasets", datasetSummary)
        setTag(runId, "mlflow.data.context", "training")

        // Additional dataset tags
        info.foreach { case (key, value) =>
          setTag(runId, s"dataset.ui.$key", value)
        }

        println(s"[MLFlow]   Dataset: $datasetSummary")
      }

      // ⭐ MODELS COLUMN
      modelInfo.foreach { info =>
        val modelSummary = info.getOrElse("name", "model")

        // MLFlow UI recognizes these tags
        setTag(runId, "mlflow.logged_models", modelSummary)

        info.get("registered_name").foreach { registeredName =>
          setTag(runId, "mlflow.registered_model_name", registeredName)
        }

        println(s"[MLFlow]   Model: $modelSummary")
      }

      // ⭐ DESCRIPTION COLUMN
      runDescription.foreach { desc =>
        // Use mlflow.note.content for description
        setDescription(runId, desc)

        // Also set as a summary tag (first 200 chars)
        val summary = if (desc.length > 200) desc.take(200) + "..." else desc
        setTag(runId, "mlflow.run.summary", summary)

        println(s"[MLFlow]   Description: ${summary}")
      }

      println(s"[MLFlow] ✓ UI metadata set successfully")

    } match {
      case Success(_) => // Success
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to set UI metadata: ${e.getMessage}")
    }
  }

  /**
   * Set Experiment and configuration data.
   * @param experiment
   * @param configuration
   */
  def setRunWithExperimentUIMetadata(
     experiment: ExperimentConfig,
     configuration: AppConfiguration
   ): Unit = {

    val runId = MLFlowTracker.startRun(
      experimentId = experiment.name,
      runName = experiment.name
    ).get

    // ⭐ Build dataset info from config
    val datasetInfo = Map(
      "flights" -> configuration.common.data.flight.path.split("/").last,
      "weather" -> configuration.common.data.weather.path.split("/").last,
      "airportMapping" -> configuration.common.data.airportMapping.path.split("/").last,
      "target" -> experiment.target,
      "balanced" -> "true"
    )

    // Build model info from config
    val modelInfo = Map(
      "name" -> s"${experiment.model.modelType}_${experiment.name}",
      "type" -> experiment.model.modelType,
      "trees" -> experiment.model.hyperparameters.numTrees.headOption.map(_.toString).getOrElse("?"),
      "depth" -> experiment.model.hyperparameters.maxDepth.headOption.map(_.toString).getOrElse("?")
    )

    // ⭐ Build description from config
    val description = s"${experiment.description} | Target: ${experiment.target} | Model: ${experiment.model.modelType}"

    // Set UI metadata
    MLFlowTracker.setRunUIMetadata(
      runId = runId,
      datasetInfo = Some(datasetInfo),
      modelInfo = Some(modelInfo),
      runDescription = Some(description)
    )

    // Continue with training...
  }
  /**
   * Check if MLFlow tracking is enabled
   */
  def isEnabled: Boolean = enabled

  /**
   * Get tracking URI
   */
  def getTrackingUri: String = trackingUri
}