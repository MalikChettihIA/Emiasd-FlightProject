package com.flightdelay.ml.tracking

import org.mlflow.tracking.MlflowClient
import org.mlflow.api.proto.Service.RunStatus

import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}

/**
 * MLFlow Tracker - Centralized experiment tracking with MLFlow
 *
 * This object provides methods to:
 * 1. Create and manage MLFlow experiments
 * 2. Start and end runs
 * 3. Log parameters, metrics, and artifacts
 * 4. Handle errors gracefully
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
   * @return Run ID if successful
   */
  def startRun(experimentId: String, runName: String): Option[String] = {
    if (!enabled || client.isEmpty) return None

    Try {
      val run = client.get.createRun(experimentId)
      val runId = run.getRunId

      // Set run name as a tag
      client.get.setTag(runId, "mlflow.runName", runName)

      println(s"[MLFlow] Started run: $runName (ID: $runId)")
      runId.toString
    } match {
      case Success(id: String) => Some(id)
      case Failure(e) =>
        println(s"[MLFlow] Warning: Failed to start run: ${e.getMessage}")
        None
    }
  }

  /**
   * Log parameters to MLFlow
   * @param runId Run ID
   * @param params Map of parameter names to values
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
   * @param runId Run ID
   * @param key Metric name
   * @param value Metric value
   * @param step Step number (for time series metrics)
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
   * @param runId Run ID
   * @param metrics Map of metric names to values
   * @param step Step number (for time series metrics)
   */
  def logMetrics(runId: String, metrics: Map[String, Double], step: Long = 0): Unit = {
    if (!enabled || client.isEmpty) return

    metrics.foreach { case (key, value) =>
      logMetric(runId, key, value, step)
    }
  }

  /**
   * Log an artifact (file or directory) to MLFlow
   * @param runId Run ID
   * @param localPath Local file or directory path
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
   * Set a tag on a run
   * @param runId Run ID
   * @param key Tag name
   * @param value Tag value
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
   * End a MLFlow run
   * @param runId Run ID
   * @param status Run status (default: FINISHED)
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

  /**
   * Check if MLFlow tracking is enabled
   * @return true if enabled
   */
  def isEnabled: Boolean = enabled

  /**
   * Get tracking URI
   * @return Tracking URI
   */
  def getTrackingUri: String = trackingUri
}
