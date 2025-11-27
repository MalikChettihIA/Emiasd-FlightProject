package com.flightdelay.ml.models

import com.flightdelay.config.ExperimentConfig
import org.apache.spark.ml.{Pipeline, Transformer}
import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets

/**
 * XGBoost model implementation for flight delay prediction.
 *
 * XGBoost (Extreme Gradient Boosting) is an optimized distributed gradient boosting
 * library that is highly efficient, flexible and portable.
 *
 * Advantages over GBT Spark:
 * - Better performance (speed and accuracy)
 * - Advanced regularization (L1, L2, gamma)
 * - Better handling of missing values
 * - Built-in cross-validation
 * - GPU support (if available)
 * - More hyperparameter control
 *
 * Advantages for flight delay prediction:
 * - State-of-the-art accuracy
 * - Handles complex interactions
 * - Robust to overfitting with proper regularization
 * - Faster training than GBT Spark
 *
 * @param experiment Experiment configuration with model type and hyperparameters
 */
class XGBoostModel(experiment: ExperimentConfig) extends MLModel {

  /**
   * Train XGBoost classifier on flight delay data
   * @param data Training data with "features" and "label" columns
   * @param featureImportancePath Optional path to save feature importances
   * @return Trained XGBoost model wrapped in a Pipeline
   */
  def train(data: DataFrame, featureImportancePath: Option[String] = None): Transformer = {
    val hp = experiment.model.hyperparameters

    // Use first value from arrays for single training
    val numRound = hp.maxIter.getOrElse(Seq(100)).head  // Number of boosting rounds (trees)
    val maxDepth = hp.maxDepth.getOrElse(Seq(6)).head
    val eta = hp.stepSize.getOrElse(Seq(0.1)).head  // Learning rate
    val subsample = hp.subsamplingRate.getOrElse(Seq(1.0)).head
    val colsampleBytree = hp.colsampleBytree.getOrElse(Seq(1.0)).head
    val minChildWeight = hp.minInstancesPerNode.getOrElse(Seq(1)).head
    val alpha = hp.alpha.getOrElse(Seq(0.0)).head  // L1 regularization
    val lambda = hp.lambda.getOrElse(Seq(1.0)).head  // L2 regularization
    val gamma = hp.gamma.getOrElse(Seq(0.0)).head  // Minimum loss reduction

    println(s"[XGBoost] Training with hyperparameters:")
    println(s"  - Num rounds (trees):    $numRound")
    println(s"  - Max depth:             $maxDepth")
    println(s"  - Eta (learning rate):   $eta")
    println(s"  - Subsample:             $subsample")
    println(s"  - Colsample by tree:     $colsampleBytree")
    println(s"  - Min child weight:      $minChildWeight")
    println(s"  - Alpha (L1 reg):        $alpha")
    println(s"  - Lambda (L2 reg):       $lambda")
    println(s"  - Gamma (min loss red):  $gamma")

    // Configure XGBoost classifier
    val xgbParams = Map(
      "eta" -> eta,
      "max_depth" -> maxDepth,
      "subsample" -> subsample,
      "colsample_bytree" -> colsampleBytree,
      "min_child_weight" -> minChildWeight,
      "alpha" -> alpha,
      "lambda" -> lambda,
      "gamma" -> gamma,
      "objective" -> "binary:logistic",
      "eval_metric" -> "logloss",
      "seed" -> experiment.name.hashCode.toLong,
      "nthread" -> 4  // Can be adjusted based on cluster config
    )

    val xgb = new XGBoostClassifier(xgbParams)
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability")
      .setRawPredictionCol("rawPrediction")
      .setNumRound(numRound)
      .setNumWorkers(2)  // Adjust based on cluster size

    // Create pipeline with the classifier
    val pipeline = new Pipeline().setStages(Array(xgb))

    println("Starting training...")
    val startTime = System.currentTimeMillis()

    val model = pipeline.fit(data)

    val endTime = System.currentTimeMillis()
    val trainingTime = (endTime - startTime) / 1000.0

    println(f"- Training completed in $trainingTime%.2f seconds")

    // Extract and display feature importances
    val xgbModel = model.stages(0).asInstanceOf[XGBoostClassificationModel]
    displayFeatureImportances(xgbModel)

    // Save feature importances if path provided
    featureImportancePath.foreach { path =>
      saveFeatureImportances(xgbModel, path)
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
   * Display top feature importances from the trained model
   */
  private def displayFeatureImportances(model: XGBoostClassificationModel): Unit = {
    val importances = model.nativeBooster.getScore("", "gain") // Use "gain" importance type
    val topN = 20

    // Try to load feature names from file
    val featureNames = loadFeatureNames()

    println(f"Top $topN Feature Importances (Gain):")
    println("=" * 90)
    println(f"${"Rank"}%-6s ${"Feature Name"}%-60s ${"Importance"}%12s")
    println("=" * 90)

    importances.toSeq
      .sortBy(-_._2)
      .take(topN)
      .zipWithIndex
      .foreach { case ((featureName, importance), rank) =>
        val displayName = if (featureNames.nonEmpty && featureName.startsWith("f")) {
          // XGBoost uses "f0", "f1", etc. as feature names
          val idx = featureName.drop(1).toInt
          featureNames.lift(idx).getOrElse(featureName)
        } else {
          featureName
        }

        val importancePercent = importance * 100

        // Visual indicator for importance level
        val indicator = if (importancePercent >= 10) "█"
                       else if (importancePercent >= 5) "▓"
                       else if (importancePercent >= 1) "▒"
                       else "░"

        println(f"${rank + 1}%-6d ${displayName}%-60s ${indicator}  ${importancePercent}%5.2f%%")
      }

    println("=" * 90)
    println("Importance Levels:  █≥10% ▓≥5% ▒≥1% ░<1%")
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
   * Save feature importances to CSV file with feature names
   */
  private def saveFeatureImportances(model: XGBoostClassificationModel, outputPath: String): Unit = {
    val importances = model.nativeBooster.getScore("", "gain")
    val featureNames = loadFeatureNames()

    // Build CSV content with header
    val header = "feature_name,importance"
    val rows = importances.toSeq
      .map { case (featureName, importance) =>
        val displayName = if (featureNames.nonEmpty && featureName.startsWith("f")) {
          val idx = featureName.drop(1).toInt
          featureNames.lift(idx).getOrElse(featureName)
        } else {
          featureName
        }
        s"$displayName,$importance"
      }
      .sortBy(row => -row.split(",")(1).toDouble)

    val csvContent = (header +: rows).mkString("\n")

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
        println(s" Feature importances saved to: $outputPath")
      } finally {
        writer.close()
      }
    } catch {
      case ex: Exception =>
        println(s" Failed to save feature importances: ${ex.getMessage}")
    }
  }
}

/**
 * Companion object for XGBoostModel factory methods
 */
object XGBoostModel {

  /**
   * Create an XGBoostModel from experiment configuration
   * @param experiment Experiment configuration
   * @return New XGBoostModel instance
   */
  def apply(experiment: ExperimentConfig): XGBoostModel = {
    new XGBoostModel(experiment)
  }
}
