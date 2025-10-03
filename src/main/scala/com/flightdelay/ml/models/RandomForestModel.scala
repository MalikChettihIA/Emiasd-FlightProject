package com.flightdelay.ml.models

import com.flightdelay.config.ModelConfig
import com.flightdelay.utils.MetricsWriter
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.DataFrame

/**
 * Random Forest model implementation for flight delay prediction.
 *
 * Random Forest is an ensemble learning method that constructs multiple decision trees
 * during training and outputs the class that is the mode of the classes of individual trees.
 *
 * Advantages for flight delay prediction:
 * - Handles non-linear relationships well
 * - Robust to outliers
 * - Provides feature importance
 * - Works well with high-dimensional data
 *
 * @param config Model configuration with hyperparameters
 */
class RandomForestModel(config: ModelConfig) extends MLModel {

  /**
   * Train Random Forest classifier on flight delay data
   * @param data Training data with "features" and "label" columns
   * @param featureImportancePath Optional path to save feature importances
   * @return Trained RandomForest model wrapped in a Pipeline
   */
  def train(data: DataFrame, featureImportancePath: Option[String] = None): Transformer = {
    println("\n" + "=" * 80)
    println("Training Random Forest Model")
    println("=" * 80)
    println(s"Model: ${config.name}")
    println(s"Target: ${config.target}")
    println(s"Number of trees: ${config.numTrees}")
    println(s"Max depth: ${config.maxDepth}")
    println(s"Max bins: ${config.maxBins}")
    println(s"Min instances per node: ${config.minInstancesPerNode}")
    println(s"Random seed: ${config.seed}")
    println("=" * 80)

    // Configure Random Forest classifier
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability")
      .setRawPredictionCol("rawPrediction")
      .setNumTrees(config.numTrees)
      .setMaxDepth(config.maxDepth)
      .setMaxBins(config.maxBins)
      .setMinInstancesPerNode(config.minInstancesPerNode)
      .setSeed(config.seed)
      .setFeatureSubsetStrategy("auto")  // Let Spark decide optimal features per split
      .setImpurity("gini")               // Gini impurity for classification
      .setSubsamplingRate(1.0)           // Use all data for each tree

    // Create pipeline with the classifier
    val pipeline = new Pipeline().setStages(Array(rf))

    println("\nStarting training...")
    val startTime = System.currentTimeMillis()

    val model = pipeline.fit(data)

    val endTime = System.currentTimeMillis()
    val trainingTime = (endTime - startTime) / 1000.0

    println(f"\n✓ Training completed in $trainingTime%.2f seconds")

    // Extract and display feature importance
    val rfModel = model.stages(0).asInstanceOf[RandomForestClassificationModel]
    displayFeatureImportance(rfModel)

    // Save feature importance if path provided
    featureImportancePath.foreach { path =>
      saveFeatureImportance(rfModel, path)
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
  private def displayFeatureImportance(model: RandomForestClassificationModel): Unit = {
    val importances = model.featureImportances.toArray
    val topN = 20

    println(f"\nTop $topN Feature Importances:")
    println("-" * 50)

    importances.zipWithIndex
      .sortBy(-_._1)
      .take(topN)
      .foreach { case (importance, idx) =>
        println(f"Feature $idx%3d: ${importance * 100}%6.2f%%")
      }

    println("-" * 50)
  }

  /**
   * Save feature importances to CSV file
   */
  private def saveFeatureImportance(model: RandomForestClassificationModel, outputPath: String): Unit = {
    val importances = model.featureImportances.toArray
    val importancesWithIndex = importances.zipWithIndex.map { case (imp, idx) => (idx, imp) }

    MetricsWriter.writeFeatureImportance(importancesWithIndex, outputPath) match {
      case scala.util.Success(_) => // Already logged by MetricsWriter
      case scala.util.Failure(ex) => println(s"  ✗ Failed to save feature importances: ${ex.getMessage}")
    }
  }
}

/**
 * Companion object for RandomForestModel factory methods
 */
object RandomForestModel {

  /**
   * Create a RandomForestModel from configuration
   * @param config Model configuration
   * @return New RandomForestModel instance
   */
  def apply(config: ModelConfig): RandomForestModel = {
    new RandomForestModel(config)
  }
}
