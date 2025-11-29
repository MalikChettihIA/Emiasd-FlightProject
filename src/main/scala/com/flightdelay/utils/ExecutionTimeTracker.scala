package com.flightdelay.utils

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import scala.collection.mutable

/**
 * Tracks execution time for different pipeline steps
 * Supports hierarchical tracking (main steps and substeps)
 */
class ExecutionTimeTracker {

  // Store execution times in seconds for each step
  private val executionTimes = mutable.Map[String, Double]()
  private val stepStartTimes = mutable.Map[String, Long]()

  /**
   * Start tracking a step
   */
  def startStep(stepName: String): Unit = {
    stepStartTimes(stepName) = System.currentTimeMillis()
  }

  /**
   * End tracking a step and record its duration
   */
  def endStep(stepName: String): Double = {
    stepStartTimes.get(stepName) match {
      case Some(startTime) =>
        val duration = (System.currentTimeMillis() - startTime) / 1000.0
        executionTimes(stepName) = duration
        stepStartTimes.remove(stepName)
        duration
      case None =>
        executionTimes(stepName) = Double.NaN
        Double.NaN
    }
  }

  /**
   * Set a step as not applicable (NA)
   */
  def setStepNA(stepName: String): Unit = {
    executionTimes(stepName) = Double.NaN
  }

  /**
   * Set execution time for a step directly
   */
  def setStepTime(stepName: String, time: Double): Unit = {
    executionTimes(stepName) = time
  }

  /**
   * Get execution time for a step
   */
  def getStepTime(stepName: String): Option[Double] = {
    executionTimes.get(stepName)
  }

  /**
   * Execute a block of code and track its execution time
   */
  def trackStep[T](stepName: String)(block: => T): T = {
    startStep(stepName)
    try {
      block
    } finally {
      endStep(stepName)
    }
  }

  /**
   * Get all execution times
   */
  def getAllTimes: Map[String, Double] = executionTimes.toMap

  /**
   * Get total execution time (sum of main pipeline totals only to avoid double counting)
   */
  def getTotalTime: Double = {
    // Only sum the three main pipeline totals to avoid double counting
    val mainPipelines = Seq(
      "data_processing.total",
      "balancing.total",
      "join.total",
      "explode.total",
      "post_processing.total",
      "ml.total"
    )

    mainPipelines.flatMap(executionTimes.get).filterNot(_.isNaN).sum
  }

  /**
   * Format time value for display (handles NaN as "NA")
   */
  private def formatTime(time: Double): String = {
    if (time.isNaN) "NA" else f"$time%.2f"
  }

  /**
   * Display a summary table in the console
   */
  def displaySummaryTable(): Unit = {
    val separator = "=" * 90
    val lineSeparator = "-" * 90

    println(separator)
    println("EXECUTION TIME SUMMARY")
    println(separator)
    println(f"${"Step"}%-50s ${"Time (s)"}%15s ${"Time (min)"}%15s")
    println(lineSeparator)

    // Define step order and labels
    val stepOrder = Seq(
      ("data_processing.load_flights", "Data Processing - Load Flights"),
      ("data_processing.load_weather", "Data Processing - Load Weather"),
      ("data_processing.load_wban", "Data Processing - Load WBAN Mapping"),
      ("data_processing.preprocess_flights", "Data Processing - Preprocess Flights"),
      ("data_processing.preprocess_weather", "Data Processing - Preprocess Weather"),
      ("data_processing.filter_columns", "Data Processing - Filter Columns"),
      ("data_processing.save_parquet", "Data Processing - Save to Parquet"),
      ("data_processing.total", "DATA PROCESSING - TOTAL"),
      ("balancing.label_flights", "Balancing - Label Flights"),
      ("balancing.sample_flights", "Balancing - Sample Flights"),
      ("balancing.total", "BALANCING - TOTAL"),
      ("join.train", "Join - Train Dataset"),
      ("join.test", "Join - Test Dataset"),
      ("join.total", "JOIN - TOTAL"),
      ("explode.train", "Explode - Train Dataset"),
      ("explode.test", "Explode - Test Dataset"),
      ("explode.total", "EXPLODE - TOTAL"),
      ("post_processing.train", "Post Processing - Train Dataset"),
      ("post_processing.test", "Post Processing - Test Dataset"),
      ("post_processing.total", "POST PROCESSING - TOTAL"),
      ("ml_feature_extraction.dev", "ML Feature Extraction - Dev Set"),
      ("ml_feature_extraction.test", "ML Feature Extraction - Test Set"),
      ("ml_feature_extraction.total", "ML FEATURE EXTRACTION - TOTAL"),
      ("ml_grid_search", "ML Grid Search (included in CV)"),
      ("ml_kfold_cv", "ML K-Fold Cross Validation + Grid Search"),
      ("ml_train", "ML Train Final Model"),
      ("ml_evaluation", "ML Evaluation"),
      ("ml_save_metrics", "ML Save Metrics"),
      ("ml.total", "ML PIPELINE - TOTAL")
    )

    stepOrder.foreach { case (stepKey, stepLabel) =>
      val time = executionTimes.getOrElse(stepKey, Double.NaN)
      val timeStr = formatTime(time)
      val timeMinStr = if (time.isNaN) "NA" else f"${time / 60}%.2f"

      // Highlight total rows
      if (stepLabel.contains("TOTAL")) {
        println(lineSeparator)
        println(f"$stepLabel%-50s ${timeStr}%15s ${timeMinStr}%15s")
        println(lineSeparator)
      } else {
        println(f"$stepLabel%-50s ${timeStr}%15s ${timeMinStr}%15s")
      }
    }

    val totalTime = getTotalTime
    println(separator)
    println(f"${"TOTAL EXECUTION TIME"}%-50s ${formatTime(totalTime)}%15s ${f"${totalTime / 60}%.2f"}%15s")
    println(separator)
  }

  /**
   * Save execution times to CSV file
   */
  def saveToCSV(path: String)(implicit spark: SparkSession): Unit = {
    val csvContent = new StringBuilder()
    csvContent.append("step,time_seconds,time_minutes,status\n")

    // Define step order
    val stepOrder = Seq(
      ("data_processing.load_flights", "Data Processing - Load Flights"),
      ("data_processing.load_weather", "Data Processing - Load Weather"),
      ("data_processing.load_wban", "Data Processing - Load WBAN Mapping"),
      ("data_processing.preprocess_flights", "Data Processing - Preprocess Flights"),
      ("data_processing.preprocess_weather", "Data Processing - Preprocess Weather"),
      ("data_processing.filter_columns", "Data Processing - Filter Columns"),
      ("data_processing.save_parquet", "Data Processing - Save to Parquet"),
      ("data_processing.total", "DATA PROCESSING - TOTAL"),
      ("balancing.label_flights", "Balancing - Label Flights"),
      ("balancing.sample_flights", "Balancing - Sample Flights"),
      ("balancing.total", "BALANCING - TOTAL"),
      ("join.train", "Join - Train Dataset"),
      ("join.test", "Join - Test Dataset"),
      ("join.total", "JOIN - TOTAL"),
      ("explode.train", "Explode - Train Dataset"),
      ("explode.test", "Explode - Test Dataset"),
      ("explode.total", "EXPLODE - TOTAL"),
      ("post_processing.train", "Post Processing - Train Dataset"),
      ("post_processing.test", "Post Processing - Test Dataset"),
      ("post_processing.total", "POST PROCESSING - TOTAL"),
      ("ml_feature_extraction.dev", "ML Feature Extraction - Dev Set"),
      ("ml_feature_extraction.test", "ML Feature Extraction - Test Set"),
      ("ml_feature_extraction.total", "ML FEATURE EXTRACTION - TOTAL"),
      ("ml_grid_search", "ML Grid Search (included in CV)"),
      ("ml_kfold_cv", "ML K-Fold Cross Validation + Grid Search"),
      ("ml_train", "ML Train Final Model"),
      ("ml_evaluation", "ML Evaluation"),
      ("ml_save_metrics", "ML Save Metrics"),
      ("ml.total", "ML PIPELINE - TOTAL")
    )

    stepOrder.foreach { case (stepKey, stepLabel) =>
      val time = executionTimes.getOrElse(stepKey, Double.NaN)
      val timeStr = if (time.isNaN) "NA" else f"$time%.6f"
      val timeMinStr = if (time.isNaN) "NA" else f"${time / 60}%.6f"
      val status = if (time.isNaN) "NA" else "completed"
      csvContent.append(s"$stepLabel,$timeStr,$timeMinStr,$status\n")
    }

    // Write using Hadoop FileSystem (HDFS-compatible)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val csvPath = new Path(path)

    // Ensure the path is properly qualified for the current filesystem (local or HDFS)
    val qualifiedPath = fs.makeQualified(csvPath)

    // Create parent directories if they don't exist
    val parentPath = qualifiedPath.getParent
    if (parentPath != null && !fs.exists(parentPath)) {
      fs.mkdirs(parentPath)
    }

    val out = fs.create(qualifiedPath, true)
    val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
    try {
      writer.write(csvContent.toString())
    } finally {
      writer.close()
    }
  }

  /**
   * Save execution times to TXT file (human-readable format)
   */
  def saveToText(path: String)(implicit spark: SparkSession): Unit = {
    val txtContent = new StringBuilder()
    val separator = "=" * 90
    val lineSeparator = "-" * 90

    txtContent.append(separator).append("\n")
    txtContent.append("EXECUTION TIME SUMMARY\n")
    txtContent.append(separator).append("\n")
    txtContent.append(f"${"Step"}%-50s ${"Time (s)"}%15s ${"Time (min)"}%15s\n")
    txtContent.append(lineSeparator).append("\n")

    // Define step order
    val stepOrder = Seq(
      ("data_processing.load_flights", "Data Processing - Load Flights"),
      ("data_processing.load_weather", "Data Processing - Load Weather"),
      ("data_processing.load_wban", "Data Processing - Load WBAN Mapping"),
      ("data_processing.preprocess_flights", "Data Processing - Preprocess Flights"),
      ("data_processing.preprocess_weather", "Data Processing - Preprocess Weather"),
      ("data_processing.filter_columns", "Data Processing - Filter Columns"),
      ("data_processing.save_parquet", "Data Processing - Save to Parquet"),
      ("data_processing.total", "DATA PROCESSING - TOTAL"),
      ("balancing.label_flights", "Balancing - Label Flights"),
      ("balancing.sample_flights", "Balancing - Sample Flights"),
      ("balancing.total", "BALANCING - TOTAL"),
      ("join.train", "Join - Train Dataset"),
      ("join.test", "Join - Test Dataset"),
      ("join.total", "JOIN - TOTAL"),
      ("explode.train", "Explode - Train Dataset"),
      ("explode.test", "Explode - Test Dataset"),
      ("explode.total", "EXPLODE - TOTAL"),
      ("post_processing.train", "Post Processing - Train Dataset"),
      ("post_processing.test", "Post Processing - Test Dataset"),
      ("post_processing.total", "POST PROCESSING - TOTAL"),
      ("ml_feature_extraction.dev", "ML Feature Extraction - Dev Set"),
      ("ml_feature_extraction.test", "ML Feature Extraction - Test Set"),
      ("ml_feature_extraction.total", "ML FEATURE EXTRACTION - TOTAL"),
      ("ml_grid_search", "ML Grid Search (included in CV)"),
      ("ml_kfold_cv", "ML K-Fold Cross Validation + Grid Search"),
      ("ml_train", "ML Train Final Model"),
      ("ml_evaluation", "ML Evaluation"),
      ("ml_save_metrics", "ML Save Metrics"),
      ("ml.total", "ML PIPELINE - TOTAL")
    )

    stepOrder.foreach { case (stepKey, stepLabel) =>
      val time = executionTimes.getOrElse(stepKey, Double.NaN)
      val timeStr = formatTime(time)
      val timeMinStr = if (time.isNaN) "NA" else f"${time / 60}%.2f"

      // Highlight total rows
      if (stepLabel.contains("TOTAL")) {
        txtContent.append(lineSeparator).append("\n")
        txtContent.append(f"$stepLabel%-50s ${timeStr}%15s ${timeMinStr}%15s\n")
        txtContent.append(lineSeparator).append("\n")
      } else {
        txtContent.append(f"$stepLabel%-50s ${timeStr}%15s ${timeMinStr}%15s\n")
      }
    }

    val totalTime = getTotalTime
    txtContent.append(separator).append("\n")
    txtContent.append(f"${"TOTAL EXECUTION TIME"}%-50s ${formatTime(totalTime)}%15s ${f"${totalTime / 60}%.2f"}%15s\n")
    txtContent.append(separator).append("\n")
    txtContent.append(s"\nGenerated: ${java.time.LocalDateTime.now()}\n")
    txtContent.append(separator).append("\n")

    // Write using Hadoop FileSystem (HDFS-compatible)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val txtPath = new Path(path)

    // Ensure the path is properly qualified for the current filesystem (local or HDFS)
    val qualifiedPath = fs.makeQualified(txtPath)

    // Create parent directories if they don't exist
    val parentPath = qualifiedPath.getParent
    if (parentPath != null && !fs.exists(parentPath)) {
      fs.mkdirs(parentPath)
    }

    val out = fs.create(qualifiedPath, true)
    val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
    try {
      writer.write(txtContent.toString())
    } finally {
      writer.close()
    }
  }

  /**
   * Clear all tracked times
   */
  def clear(): Unit = {
    executionTimes.clear()
    stepStartTimes.clear()
  }
}

object ExecutionTimeTracker {
  /**
   * Create a new tracker instance
   */
  def create(): ExecutionTimeTracker = new ExecutionTimeTracker()
}
