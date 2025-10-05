package com.flightdelay.utils

import java.io.{File, PrintWriter}
import scala.util.{Try, Success, Failure}

/**
 * Utility object for writing metrics to JSON/CSV files for visualization in Python.
 *
 * This allows exporting Scala ML metrics to files that can be easily loaded
 * and visualized using Python libraries (matplotlib, seaborn, plotly).
 */
object MetricsWriter {

  /**
   * Write metrics to CSV file
   * @param headers Column headers
   * @param rows Data rows
   * @param outputPath Path to output CSV file
   */
  def writeCsv(headers: Seq[String], rows: Seq[Seq[Any]], outputPath: String): Try[Unit] = Try {
    val file = new File(outputPath)
    file.getParentFile.mkdirs()

    val writer = new PrintWriter(file)
    try {
      // Write header
      writer.println(headers.mkString(","))

      // Write rows
      rows.foreach { row =>
        writer.println(row.mkString(","))
      }

      println(s"  - Metrics saved to: $outputPath")
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
  ): Try[Unit] = {
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
  ): Try[Unit] = {
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
  ): Try[Unit] = {
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
