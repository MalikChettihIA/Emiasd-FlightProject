package com.flightdelay.ml.models

import com.flightdelay.config.ExperimentConfig
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.sql.DataFrame

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
  def train(data: DataFrame, featureImportancePath: Option[String] = None): Transformer = {
    val hp = experiment.train.hyperparameters

    // Use first value from arrays for single training
    val maxIter = hp.maxIter.getOrElse(Seq(100)).head
    val maxDepth = hp.maxDepth.getOrElse(Seq(5)).head
    val maxBins = hp.maxBins.getOrElse(Seq(32)).head
    val minInstancesPerNode = hp.minInstancesPerNode.getOrElse(Seq(1)).head
    val subsamplingRate = hp.subsamplingRate.getOrElse(Seq(1.0)).head
    val stepSize = hp.stepSize.getOrElse(Seq(0.1)).head

    println(s"\n[GradientBoostedTrees] Training with hyperparameters:")
    println(s"  - Max iterations (trees): $maxIter")
    println(s"  - Max depth: $maxDepth")
    println(s"  - Max bins: $maxBins")
    println(s"  - Min instances per node: $minInstancesPerNode")
    println(s"  - Subsampling rate: $subsamplingRate")
    println(s"  - Step size (learning rate): $stepSize")

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

    println("\nStarting training...")
    val startTime = System.currentTimeMillis()

    val model = pipeline.fit(data)

    val endTime = System.currentTimeMillis()
    val trainingTime = (endTime - startTime) / 1000.0

    println(f"\n- Training completed in $trainingTime%.2f seconds")

    // Extract and display feature importance
    val gbtModel = model.stages(0).asInstanceOf[GBTClassificationModel]
    displayFeatureImportance(gbtModel)

    // Save feature importance if path provided
    featureImportancePath.foreach { path =>
      saveFeatureImportance(gbtModel, path)
    }

    println("=" * 80 + "\n")

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
  private def displayFeatureImportance(model: GBTClassificationModel): Unit = {
    val importances = model.featureImportances.toArray
    val topN = 20

    // Try to load feature names from file
    val featureNames = loadFeatureNames()

    println(f"\nTop $topN Feature Importances:")
    println("-" * 50)

    importances.zipWithIndex
      .sortBy(-_._1)
      .take(topN)
      .foreach { case (importance, idx) =>
        val featureName = featureNames.lift(idx).getOrElse(s"Feature_$idx")
        println(f"[$idx%3d] $featureName%-50s: ${importance * 100}%6.2f%%")
      }

    println("-" * 50)
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
          println(s"\n✓ Loaded ${names.length} feature names from: $foundPath")
          names
        } finally {
          source.close()
        }
      }.getOrElse {
        println(s"\n⚠ Could not load feature names (tried ${possiblePaths.length} locations)")
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
  private def saveFeatureImportance(model: GBTClassificationModel, outputPath: String): Unit = {
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

    // Write to file
    try {
      val writer = new java.io.PrintWriter(new java.io.File(outputPath))
      try {
        writer.write(csvContent)
        println(s"\n✓ Feature importances saved to: $outputPath")
      } finally {
        writer.close()
      }
    } catch {
      case ex: Exception =>
        println(s"\n⚠ Failed to save feature importances: ${ex.getMessage}")
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
