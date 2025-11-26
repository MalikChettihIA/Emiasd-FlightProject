package com.flightdelay.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import java.io.{BufferedWriter, File, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import scala.util.{Try, Success, Failure}

/**
 * Utility object for writing metrics to CSV files for visualization in Python.
 *
 * This allows exporting Scala ML metrics to files that can be easily loaded
 * and visualized using Python libraries (matplotlib, seaborn, plotly).
 *
 * Supports both HDFS and local file systems.
 */
object MetricsWriter {

  /**
   * Write metrics to CSV file (supports both HDFS and local paths)
   * @param headers Column headers
   * @param rows Data rows
   * @param outputPath Path to output CSV file (HDFS or local)
   * @param spark SparkSession for Hadoop configuration
   */
  def writeCsv(headers: Seq[String], rows: Seq[Seq[Any]], outputPath: String)
              (implicit spark: SparkSession): Try[Unit] = Try {

    // Check if path is HDFS
    if (outputPath.startsWith("hdfs://")) {
      // HDFS path - use Hadoop FileSystem
      writeToHDFS(headers, rows, outputPath)
    } else {
      // Local path - use standard Java IO
      writeToLocal(headers, rows, outputPath)
    }

    println(s"  - Metrics saved to: $outputPath")
  }

  /**
   * Write CSV to HDFS using Hadoop FileSystem
   */
  private def writeToHDFS(headers: Seq[String], rows: Seq[Seq[Any]], outputPath: String)
                         (implicit spark: SparkSession): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val path = new Path(outputPath)

    // Create parent directory if it doesn't exist
    val parentDir = path.getParent
    if (parentDir != null && !fs.exists(parentDir)) {
      fs.mkdirs(parentDir)
    }

    // Write CSV file
    val out = fs.create(path, true) // true = overwrite
    val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))

    try {
      // Write header
      writer.write(headers.mkString(","))
      writer.newLine()

      // Write rows
      rows.foreach { row =>
        writer.write(row.mkString(","))
        writer.newLine()
      }
    } finally {
      writer.close()
    }
  }

  /**
   * Write CSV to local file system using Java IO
   */
  private def writeToLocal(headers: Seq[String], rows: Seq[Seq[Any]], outputPath: String): Unit = {
    val file = new File(outputPath)
    val parentDir = file.getParentFile
    if (parentDir != null) {
      parentDir.mkdirs()
    }

    val writer = new PrintWriter(file)
    try {
      // Write header
      writer.println(headers.mkString(","))

      // Write rows
      rows.foreach { row =>
        writer.println(row.mkString(","))
      }
    } finally {
      writer.close()
    }
  }

  /**
   * Write confusion matrix to CSV
   */
  def writeConfusionMatrix(
    tp: Long,
    tn: Long,
    fp: Long,
    fn: Long,
    outputPath: String
  )(implicit spark: SparkSession): Try[Unit] = {
    val headers = Seq("", "Predicted_Positive", "Predicted_Negative")
    val rows = Seq(
      Seq("Actual_Positive", tp, fn),
      Seq("Actual_Negative", fp, tn)
    )
    writeCsv(headers, rows, outputPath)
  }

  /**
   * Write feature importance to CSV
   */
  def writeFeatureImportance(
    importances: Array[(Int, Double)],
    outputPath: String
  )(implicit spark: SparkSession): Try[Unit] = {
    val headers = Seq("feature_index", "importance")
    val rows = importances.map { case (idx, imp) => Seq(idx, imp) }
    writeCsv(headers, rows, outputPath)
  }

  /**
   * Write training/test metrics comparison to CSV
   */
  def writeTrainTestMetrics(
    trainMetrics: Map[String, Double],
    testMetrics: Map[String, Double],
    outputPath: String
  )(implicit spark: SparkSession): Try[Unit] = {
    val metricNames = trainMetrics.keys.toSeq.sorted
    val headers = Seq("metric", "train", "test", "gap")
    val rows = metricNames.map { metric =>
      val train = trainMetrics(metric)
      val test = testMetrics(metric)
      val gap = train - test
      Seq(metric, f"$train%.6f", f"$test%.6f", f"$gap%.6f")
    }
    writeCsv(headers, rows, outputPath)
  }
}
