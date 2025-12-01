package com.flightdelay.ml.models

import com.flightdelay.utils.DebugUtils._
import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets

/**
 * Gradient Boosted Trees model implementation for flight delay prediction.
 *
 * GBT is an ensemble learning method that builds trees sequentially,
 * where each tree tries to correct the errors of the previous ones.
 *
 * Advantages for flight delay prediction:
 * - Better accuracy than Random Forest on many datasets
 * - Handles non-linear relationships and interactions
 * - Less prone to overfitting with proper tuning
 * - Feature importance available
 *
 * Disadvantages:
 * - Slower training than Random Forest (sequential)
 * - More sensitive to hyperparameters
 * - Can overfit if not properly regularized
 *
 * @param experiment Experiment configuration with model type and hyperparameters
 */
class GradientBoostedTreesModel(experiment: ExperimentConfig) extends MLModel {

  /**
   * Train GBT classifier on flight delay data
   * @param data Training data with "features" and "label" columns
   * @param featureImportancePath Optional path to save feature importances
   * @return Trained GBT model wrapped in a Pipeline
   */
  def train(data: DataFrame, featureImportancePath: Option[String] = None)(implicit spark: SparkSession, configuration: AppConfiguration): Transformer = {
    val hp = experiment.model.hyperparameters

    // Use first value from arrays for single training
    val maxIter = hp.maxIter.getOrElse(Seq(100)).head
    val maxDepth = hp.maxDepth.getOrElse(Seq(5)).head
    val maxBins = hp.maxBins.getOrElse(Seq(32)).head
    val minInstancesPerNode = hp.minInstancesPerNode.getOrElse(Seq(1)).head
    val subsamplingRate = hp.subsamplingRate.getOrElse(Seq(1.0)).head
    val stepSize = hp.stepSize.getOrElse(Seq(0.1)).head

    info(s"[GradientBoostedTrees] Training with hyperparameters:")
    info(s"  - Max iterations (trees): $maxIter")
    info(s"  - Max depth: $maxDepth")
    info(s"  - Max bins: $maxBins")
    info(s"  - Min instances per node: $minInstancesPerNode")
    info(s"  - Subsampling rate: $subsamplingRate")
    info(s"  - Step size (learning rate): $stepSize")

    // Configure GBT classifier
    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability")
      .setRawPredictionCol("rawPrediction")
      .setMaxIter(maxIter)
      .setMaxDepth(maxDepth)
      .setMaxBins(maxBins)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setSubsamplingRate(subsamplingRate)
      .setStepSize(stepSize)
      .setSeed(experiment.name.hashCode.toLong) // Use experiment name as seed for reproducibility

    // Create pipeline with the classifier
    val pipeline = new Pipeline().setStages(Array(gbt))

    info("Starting training...")
    val startTime = System.currentTimeMillis()

    val model = pipeline.fit(data)

    val endTime = System.currentTimeMillis()
    val trainingTime = (endTime - startTime) / 1000.0

    info(f"- Training completed in $trainingTime%.2f seconds")

    // Extract and display feature importance
    val gbtModel = model.stages(0).asInstanceOf[GBTClassificationModel]
    displayFeatureImportance(gbtModel)

    // Save feature importance if path provided
    featureImportancePath.foreach { path =>
      saveFeatureImportance(gbtModel, path)
    }

    info("=" * 80)

    model
  }

  /**
   * Override train from MLModel trait to call our extended version
   */
  override def train(data: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): Transformer = {
    train(data, None)
  }

  /**
   * Display top feature importances from the trained model
   */
  private def displayFeatureImportance(model: GBTClassificationModel)(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    val importances = model.featureImportances.toArray
    val topN = 20

    // Try to load feature names from file
    val featureNames = loadFeatureNames()

    info(f"Top $topN Feature Importances:")
    info("-" * 50)

    importances.zipWithIndex
      .sortBy(-_._1)
      .take(topN)
      .foreach { case (importance, idx) =>
        val featureName = featureNames.lift(idx).getOrElse(s"Feature_$idx")
        info(f"[$idx%3d] $featureName%-50s: ${importance * 100}%6.2f%%")
      }

    info("-" * 50)
  }

  /**
   * Load feature names from the selected_features.txt file
   * Returns empty array if file doesn't exist or can't be read
   */
  private def loadFeatureNames()(implicit spark: SparkSession, configuration: AppConfiguration): Array[String] = {
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
          info(s" Loaded ${names.length} feature names from: $foundPath")
          names
        } finally {
          source.close()
        }
      }.getOrElse {
        error(s" Could not load feature names (tried ${possiblePaths.length} locations)")
        Array.empty[String]
      }
    } catch {
      case ex: Exception =>
        error(s" Error loading feature names: ${ex.getMessage}")
        Array.empty[String]
    }
  }

  /**
   * Save feature importances to CSV file with feature names
   */
  private def saveFeatureImportance(model: GBTClassificationModel, outputPath: String)(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    val importances = model.featureImportances.toArray
    val featureNames = loadFeatureNames()

    // Build CSV content with header
    val header = "feature_index,feature_name,importance"
    val rows = importances.zipWithIndex
      .sortBy(-_._1) // Sort by importance descending
      .map { case (imp, idx) =>
        val featureName = featureNames.lift(idx).getOrElse(s"Feature_$idx")
        s"$idx,$featureName,$imp"
      }

    val csvContent = (header +: rows).mkString("\n")

    // Write to file using Hadoop FileSystem (GCS/HDFS-compatible)
    try {
      val spark = org.apache.spark.sql.SparkSession.active
      val outputPathObj = new Path(outputPath)
      // Get the filesystem that matches the path URI (GCS, HDFS, or local)
      val fs = FileSystem.get(outputPathObj.toUri, spark.sparkContext.hadoopConfiguration)
      val parentDir = outputPathObj.getParent
      if (parentDir != null && !fs.exists(parentDir)) {
        fs.mkdirs(parentDir)
      }
      val out = fs.create(outputPathObj, true)
      val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
      try {
        writer.write(csvContent)
        info(s" Feature importances saved to: $outputPath")
      } finally {
        writer.close()
      }
    } catch {
      case ex: Exception =>
        error(s" Failed to save feature importances: ${ex.getMessage}")
    }
  }
}

/**
 * Companion object for GradientBoostedTreesModel factory methods
 */
object GradientBoostedTreesModel {

  /**
   * Create a GradientBoostedTreesModel from experiment configuration
   * @param experiment Experiment configuration
   * @return New GradientBoostedTreesModel instance
   */
  def apply(experiment: ExperimentConfig): GradientBoostedTreesModel = {
    new GradientBoostedTreesModel(experiment)
  }
}
