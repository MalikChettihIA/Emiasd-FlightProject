package com.flightdelay.ml.tracking

import com.flightdelay.config.{AppConfiguration}

import scala.sys.process._
import scala.util.{Try, Success, Failure}
import java.io._
import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.util.control.NonFatal
import java.util.UUID

/**
 * WandbTracker - Bridge-based W&B tracking from Scala.
 *
 * This implementation starts a lightweight Python process (wandb_bridge.py)
 * and communicates over stdin/stdout with JSON lines. It mirrors the public
 * API used in MLPipeline so it can be dropped-in to replace MLFlowTracker.
 */
object WandbTracker {

  private var enabled: Boolean = false
  private var project: String = "flight-project"
  private var entity: Option[String] = None
  private var mode: String = sys.env.getOrElse("WANDB_MODE", "online")
  private var dir: Option[String] = sys.env.get("WANDB_DIR")

  private var proc: Option[Process] = None
  private var writer: Option[BufferedWriter] = None
  private var reader: Option[BufferedReader] = None

  private def esc(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")

  private def sendRaw(json: String): Unit = synchronized {
    if (!enabled || writer.isEmpty) return
    try {
      val w = writer.get
      w.write(json)
      w.write("\n")
      w.flush()
    } catch {
      case NonFatal(e) => println(s"[W&B] Bridge write error: ${e.getMessage}")
    }
  }

  private def startBridge(): Boolean = {
    val bridgePath = sys.env.get("WANDB_BRIDGE_PATH")
      .getOrElse("work/scripts/wandb_bridge.py")

    val file = new java.io.File(bridgePath)
    if (!file.exists()) {
      println(s"[W&B] Bridge script not found at $bridgePath")
      return false
    }

    try {
      val pb = new ProcessBuilder("python3", "-u", bridgePath)
      val env = pb.environment()
      env.put("WANDB_PROJECT", project)
      entity.foreach(v => env.put("WANDB_ENTITY", v))
      env.put("WANDB_MODE", mode)
      dir.foreach(v => env.put("WANDB_DIR", v))
      sys.env.get("WANDB_API_KEY").foreach(v => env.put("WANDB_API_KEY", v))

      pb.redirectErrorStream(true)
      val p = pb.start()
      proc = Some(p)
      writer = Some(new BufferedWriter(new OutputStreamWriter(p.getOutputStream, StandardCharsets.UTF_8)))
      reader = Some(new BufferedReader(new InputStreamReader(p.getInputStream, StandardCharsets.UTF_8)))

      // wait for ready
      val initMsg = reader.get.readLine()
      if (initMsg != null && initMsg.contains("__wandb_ready__")) {
        println("[W&B] Bridge started")
        true
      } else {
        println("[W&B] Bridge failed to start")
        false
      }
    } catch {
      case NonFatal(e) =>
        println(s"[W&B] Failed to start bridge: ${e.getMessage}")
        false
    }
  }

  /**
   * Initialize tracking. The first parameter is kept for signature parity with MLFlowTracker
   * but is interpreted as an optional project name if it is not a URL.
   */
  def initialize(uriOrProject: String, enable: Boolean = true): Unit = {
    enabled = enable
    if (!enabled) {
      println("[W&B] Tracking disabled")
      return
    }

    // Infer project name: prefer env, else use non-URL arg, else default
    project = sys.env.getOrElse("WANDB_PROJECT", {
      if (uriOrProject.startsWith("http://") || uriOrProject.startsWith("https://")) "flight-project" else uriOrProject
    })
    entity = sys.env.get("WANDB_ENTITY")
    mode = sys.env.getOrElse("WANDB_MODE", mode)
    dir = sys.env.get("WANDB_DIR").orElse(dir)

    if (startBridge()) {
      println(s"[W&B] Initialized | project=$project, entity=${entity.getOrElse("-")}, mode=$mode")
    } else {
      enabled = false
      println("[W&B] Disabled (bridge not available)")
    }
  }

  /**
   * Use project name as "experiment" to maintain API shape.
   */
  def getOrCreateExperiment(): Option[String] = if (enabled) Some(project) else None

  def startRun(experimentId: String, runName: String, description: Option[String] = None): Option[String] = {
    if (!enabled) return None
    val msg = s"{" +
      s"\"op\":\"start_run\",\"project\":\"${esc(experimentId)}\",\"run_name\":\"${esc(runName)}\",\"description\":\"${esc(description.getOrElse(\"\"))}\"}""
    sendRaw(msg)
    Some(UUID.randomUUID().toString)
  }

  def setDescription(runId: String, description: String): Unit = {
    if (!enabled) return
    val msg = s"{\"op\":\"set_description\",\"description\":\"${esc(description)}\"}"
    sendRaw(msg)
    ()
  }

  def appendToDescription(runId: String, additionalText: String): Unit = {
    if (!enabled) return
    val msg = s"{\"op\":\"append_description\",\"text\":\"${esc(additionalText)}\"}"
    sendRaw(msg)
    ()
  }

  def logDataset(runId: String, datasetName: String, datasetPath: String, datasetType: String = "training", numRows: Option[Long] = None, numCols: Option[Int] = None, metadata: Map[String, String] = Map.empty): Unit = {
    if (!enabled) return
    val meta = metadata.map { case (k,v) => s"\"${esc(k)}\":\"${esc(v)}\"" }.mkString(",")
    val nr = numRows.map(_.toString).getOrElse("null")
    val nc = numCols.map(_.toString).getOrElse("null")
    val json = s"{" +
      s"\"op\":\"log_dataset\",\"data\":{\"name\":\"${esc(datasetName)}\",\"path\":\"${esc(datasetPath)}\",\"dtype\":\"${esc(datasetType)}\",\"num_rows\":${nr},\"num_cols\":${nc},\"metadata\":{${meta}}}}"
    sendRaw(json); ()
  }

  def logDatasets(runId: String, datasets: Seq[(String, String, String, Option[Long], Option[Int], Map[String, String])]): Unit = {
    datasets.foreach { case (n,p,t,r,c,m) => logDataset(runId, n,p,t,r,c,m) }
  }

  def logDatasetSplit(runId: String, trainPath: String, trainRows: Long, valPath: Option[String] = None, valRows: Option[Long] = None, testPath: Option[String] = None, testRows: Option[Long] = None, numCols: Option[Int] = None): Unit = {
    logDataset(runId, "training_data", trainPath, "training", Some(trainRows), numCols)
    valPath.foreach(p => logDataset(runId, "validation_data", p, "validation", valRows, numCols))
    testPath.foreach(p => logDataset(runId, "test_data", p, "test", testRows, numCols))
  }

  def logDatasetStats(runId: String, datasetType: String, stats: Map[String, Any]): Unit = {
    if (!enabled) return
    val payload = stats.map { case (k,v) => s"\"${esc(s"dataset.${datasetType}.stats.$k")}\":\"${esc(v.toString)}\"" }.mkString(",")
    sendRaw(s"{\"op\":\"set_tags\",\"tags\":{${payload}}}"); ()
  }

  def logModel[M](runId: String, model: M, modelName: String, modelPath: String, modelType: String, metadata: Map[String, String] = Map.empty): Unit = {
    // In Scala side we only log the saved directory as an artifact
    val localModelPath = s"$modelPath/$modelName"
    sendRaw(s"{\"op\":\"log_artifact\",\"path\":\"${esc(localModelPath)}\",\"artifact_path\":\"${esc(s"models/$modelName")}\"}"); ()
    setTags(runId, metadata.map { case (k,v) => s"model.metadata.$k" -> v })
  }

  def logPipelineModel(runId: String, pipelineModel: Any, modelName: String, modelPath: String, metadata: Map[String, String] = Map.empty): Unit = {
    val localModelPath = s"$modelPath/$modelName"
    sendRaw(s"{\"op\":\"log_artifact\",\"path\":\"${esc(localModelPath)}\",\"artifact_path\":\"${esc(s"models/$modelName")}\"}"); ()
    setTag(runId, "model.name", modelName)
    setTag(runId, "model.type", "PipelineModel")
  }

  def registerModel(runId: String, modelName: String, modelPath: String, description: Option[String] = None, tags: Map[String, String] = Map.empty): Unit = {
    setTag(runId, "model.registered", "true")
    setTag(runId, "model.registry_name", modelName)
    description.foreach(d => setTag(runId, "model.registry_description", d))
    setTags(runId, tags.map { case (k,v) => s"model.registry_tag.$k" -> v })
  }

  def logModelMetrics(runId: String, metrics: Map[String, Double], stage: String = "validation"): Unit = {
    logMetrics(runId, metrics.map { case (k,v) => s"${stage}_${k}" -> v })
  }

  def logFeatureImportances(runId: String, featureNames: Array[String], importances: Array[Double], topN: Option[Int] = None): Unit = {
    val feats = featureNames.zip(importances).sortBy(-_._2).take(topN.getOrElse(featureNames.length))
    feats.take(10).zipWithIndex.foreach { case ((name, score), idx) =>
      logMetric(runId, s"feature_importance_${idx+1}_${name}", score, 0)
    }
  }

  def logParams(runId: String, params: Map[String, Any]): Unit = {
    if (!enabled) return
    val ps = params.map { case (k,v) => s"\"${esc(k)}\":\"${esc(v.toString)}\"" }.mkString(",")
    sendRaw(s"{\"op\":\"log_params\",\"params\":{${ps}}}"); ()
  }

  def logMetric(runId: String, key: String, value: Double, step: Long = 0): Unit = {
    if (!enabled) return
    sendRaw(s"{\"op\":\"log_metric\",\"key\":\"${esc(key)}\",\"value\":${value},\"step\":${step}}"); ()
  }

  def logMetrics(runId: String, metrics: Map[String, Double], step: Long = 0): Unit = {
    if (!enabled) return
    val ms = metrics.map { case (k,v) => s"\"${esc(k)}\":${v}" }.mkString(",")
    sendRaw(s"{\"op\":\"log_metrics\",\"metrics\":{${ms}},\"step\":${step}}"); ()
  }

  def logArtifact(runId: String, localPath: String): Unit = {
    if (!enabled) return
    sendRaw(s"{\"op\":\"log_artifact\",\"path\":\"${esc(localPath)}\"}"); ()
  }

  def logArtifactWithPath(runId: String, localPath: String, artifactPath: String): Unit = {
    if (!enabled) return
    sendRaw(s"{\"op\":\"log_artifact\",\"path\":\"${esc(localPath)}\",\"artifact_path\":\"${esc(artifactPath)}\"}"); ()
  }

  def setTag(runId: String, key: String, value: String): Unit = {
    if (!enabled) return
    sendRaw(s"{\"op\":\"set_tag\",\"key\":\"${esc(key)}\",\"value\":\"${esc(value)}\"}"); ()
  }

  def setTags(runId: String, tags: Map[String, String]): Unit = {
    if (!enabled) return
    val ts = tags.map { case (k,v) => s"\"${esc(k)}\":\"${esc(v)}\"" }.mkString(",")
    sendRaw(s"{\"op\":\"set_tags\",\"tags\":{${ts}}}"); ()
  }

  def endRun(runId: String): Unit = {
    if (!enabled) return
    sendRaw(s"{\"op\":\"finish\"}"); ()
  }

  def logDatasetsFromConfig(runId: String, config: AppConfiguration): Unit = {
    println("[W&B] Logging datasets from configuration...")
    logDataset(runId, "flights_raw", config.common.data.flight.path, "raw", None, None, Map(
      "source" -> "DOT BTS", "format" -> "csv", "base_path" -> config.common.data.basePath
    ))
    logDataset(runId, "weather_raw", config.common.data.weather.path, "raw", None, None, Map(
      "source" -> "NOAA", "format" -> "txt", "base_path" -> config.common.data.basePath
    ))
    logDataset(runId, "airport_mapping", config.common.data.airportMapping.path, "reference", None, None, Map(
      "type" -> "WBAN to Airport mapping", "format" -> "csv"
    ))
    val processedFlightsPath = s"${config.common.output.basePath}/common/data/processed_flights.parquet"
    logDataset(runId, "flights_processed", processedFlightsPath, "processed", None, None, Map(
      "format" -> "parquet", "includes_labels" -> "true", "includes_features" -> "true"
    ))
    val processedWeatherPath = s"${config.common.output.basePath}/common/data/processed_weather.parquet"
    logDataset(runId, "weather_processed", processedWeatherPath, "processed", None, None, Map(
      "format" -> "parquet", "normalized_time" -> "true"
    ))
  }
}
