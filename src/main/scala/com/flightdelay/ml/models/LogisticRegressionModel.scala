package com.flightdelay.ml.models

import com.flightdelay.config.ExperimentConfig
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel => SparkLRModel}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets

/**
 * Logistic Regression model implementation for flight delay prediction.
 *
 * Logistic Regression is a linear model for binary classification that
 * estimates the probability of a binary response based on features.
 *
 * Advantages for flight delay prediction:
 * - Fast training and inference
 * - Interpretable coefficients
 * - Works well with linearly separable data
 * - Lower memory footprint than ensemble methods
 *
 * @param experiment Experiment configuration with model type and hyperparameters
 */
class LogisticRegressionModel(experiment: ExperimentConfig) extends MLModel {

  /**
   * Train Logistic Regression classifier on flight delay data
   * @param data Training data with "features" and "label" columns
   * @param featureImportancePath Optional path to save feature coefficients
   * @return Trained LogisticRegression model wrapped in a Pipeline
   */
  def train(data: DataFrame, featureImportancePath: Option[String] = None): Transformer = {
    val hp = experiment.model.hyperparameters

    // Use first value from arrays for single training
    val maxIter = hp.maxIter.getOrElse(Seq(100)).head
    val regParam = hp.regParam.getOrElse(Seq(0.0)).head
    val elasticNetParam = hp.elasticNetParam.getOrElse(Seq(0.0)).head

    println(s"[LogisticRegression] Training with hyperparameters:")
    println(s"  - Max iterations: $maxIter")
    println(s"  - Regularization parameter: $regParam")
    println(s"  - ElasticNet parameter: $elasticNetParam")
    println(s"  - Family: binomial")

    // Configure Logistic Regression classifier
    val lr = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability")
      .setRawPredictionCol("rawPrediction")
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam) // 0.0 = L2 (Ridge), 1.0 = L1 (Lasso), 0.5 = ElasticNet
      .setFamily("binomial")
      .setThreshold(0.5)

    // Create pipeline with the classifier
    val pipeline = new Pipeline().setStages(Array(lr))

    println("Starting training...")
    val startTime = System.currentTimeMillis()

    val model = pipeline.fit(data)

    val endTime = System.currentTimeMillis()
    val trainingTime = (endTime - startTime) / 1000.0

    println(f"- Training completed in $trainingTime%.2f seconds")

    // Extract and display feature coefficients
    val lrModel = model.stages(0).asInstanceOf[SparkLRModel]
    displayFeatureCoefficients(lrModel)

    // Save feature coefficients if path provided
    featureImportancePath.foreach { path =>
      saveFeatureCoefficients(lrModel, path)
    }

    println("=" * 80)

    model
  }

  /**
   * Override train from MLModel trait to call our extended version
   */
  override def train(data: DataFrame): Transformer = {
    train(data, None)
  }

  /**
   * Display top feature coefficients from the trained model
   */
  private def displayFeatureCoefficients(model: SparkLRModel): Unit = {
    val coefficients = model.coefficients.toArray
    val intercept = model.intercept
    val topN = 20

    // Try to load feature names from file
    val featureNames = loadFeatureNames()

    println(f"Model Intercept: $intercept%.6f")
    println(f"Top $topN Feature Coefficients (Absolute Value):")
    println("-" * 70)

    coefficients.zipWithIndex
      .map { case (coef, idx) => (coef, math.abs(coef), idx) }
      .sortBy(-_._2)
      .take(topN)
      .foreach { case (coef, absCoef, idx) =>
        val featureName = featureNames.lift(idx).getOrElse(s"Feature_$idx")
        val sign = if (coef >= 0) "+" else "-"
        println(f"[$idx%3d] $featureName%-40s: $sign%.1s ${absCoef}%8.6f (raw: ${coef}%8.6f)")
      }

    println("-" * 70)
  }

  /**
   * Load feature names from the selected_features.txt file
   * Returns empty array if file doesn't exist or can't be read
   */
  private def loadFeatureNames(): Array[String] = {
    try {
      val featureNamesPath = s"${experiment.name}/features/selected_features.txt"

      // Try to read from multiple possible locations
      val possiblePaths = Seq(
        s"/output/$featureNamesPath",
        s"output/$featureNamesPath",
        s"work/output/$featureNamesPath",
        featureNamesPath
      )

      possiblePaths.find(path => {
        val file = new java.io.File(path)
        file.exists() && file.canRead()
      }).map { foundPath =>
        val source = scala.io.Source.fromFile(foundPath)
        try {
          val names = source.getLines().toArray
          println(s" Loaded ${names.length} feature names from: $foundPath")
          names
        } finally {
          source.close()
        }
      }.getOrElse {
        println(s" Could not load feature names (tried ${possiblePaths.length} locations)")
        Array.empty[String]
      }
    } catch {
      case ex: Exception =>
        println(s"\n⚠ Error loading feature names: ${ex.getMessage}")
        Array.empty[String]
    }
  }

  /**
   * Save feature coefficients to CSV file with feature names
   */
  private def saveFeatureCoefficients(model: SparkLRModel, outputPath: String): Unit = {
    val coefficients = model.coefficients.toArray
    val intercept = model.intercept
    val featureNames = loadFeatureNames()

    // Build CSV content with header
    val header = "feature_index,feature_name,coefficient,abs_coefficient"
    val rows = coefficients.zipWithIndex
      .map { case (coef, idx) =>
        val featureName = featureNames.lift(idx).getOrElse(s"Feature_$idx")
        s"$idx,$featureName,$coef,${math.abs(coef)}"
      }
      .sortBy(row => -row.split(",")(3).toDouble) // Sort by abs coefficient descending

    val interceptRow = s"-1,INTERCEPT,$intercept,${math.abs(intercept)}"
    val csvContent = (header +: (interceptRow +: rows)).mkString("\n")

    // Write to file using Hadoop FileSystem (HDFS-compatible)
    try {
      val spark = org.apache.spark.sql.SparkSession.active
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val outputPathObj = new Path(outputPath)
      val parentDir = outputPathObj.getParent
      if (parentDir != null && !fs.exists(parentDir)) {
        fs.mkdirs(parentDir)
      }
      val out = fs.create(outputPathObj, true)
      val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
      try {
        writer.write(csvContent)
        println(s" Feature coefficients saved to: $outputPath")
      } finally {
        writer.close()
      }
    } catch {
      case ex: Exception =>
        println(s" Failed to save feature coefficients: ${ex.getMessage}")
    }
  }
}

/**
 * Companion object for LogisticRegressionModel factory methods
 */
object LogisticRegressionModel {

  /**
   * Create a LogisticRegressionModel from experiment configuration
   * @param experiment Experiment configuration
   * @return New LogisticRegressionModel instance
   */
  def apply(experiment: ExperimentConfig): LogisticRegressionModel = {
    new LogisticRegressionModel(experiment)
  }
}
