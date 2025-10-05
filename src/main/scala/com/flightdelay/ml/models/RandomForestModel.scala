package com.flightdelay.ml.models

import com.flightdelay.config.ExperimentConfig
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
 * @param experiment Experiment configuration with model type and hyperparameters
 */
class RandomForestModel(experiment: ExperimentConfig) extends MLModel {

  /**
   * Train Random Forest classifier on flight delay data
   * @param data Training data with "features" and "label" columns
   * @param featureImportancePath Optional path to save feature importances
   * @return Trained RandomForest model wrapped in a Pipeline
   */
  def train(data: DataFrame, featureImportancePath: Option[String] = None): Transformer = {
    val hp = experiment.train.hyperparameters

    // Use first value from arrays for single training
    // (Grid Search will iterate over all combinations)
    val numTrees = hp.numTrees.head
    val maxDepth = hp.maxDepth.head

    println(s"\n[RandomForest] Training with hyperparameters:")
    println(s"  - Number of trees: $numTrees")
    println(s"  - Max depth: $maxDepth")
    println(s"  - Max bins: ${hp.maxBins}")
    println(s"  - Min instances per node: ${hp.minInstancesPerNode}")
    println(s"  - Subsampling rate: ${hp.subsamplingRate}")
    println(s"  - Feature subset strategy: ${hp.featureSubsetStrategy}")
    println(s"  - Impurity: ${hp.impurity}")

    // Configure Random Forest classifier
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability")
      .setRawPredictionCol("rawPrediction")
      .setNumTrees(numTrees)
      .setMaxDepth(maxDepth)
      .setMaxBins(hp.maxBins)
      .setMinInstancesPerNode(hp.minInstancesPerNode)
      .setFeatureSubsetStrategy(hp.featureSubsetStrategy)
      .setImpurity(hp.impurity)
      .setSubsamplingRate(hp.subsamplingRate)

    // Create pipeline with the classifier
    val pipeline = new Pipeline().setStages(Array(rf))

    println("\nStarting training...")
    val startTime = System.currentTimeMillis()

    val model = pipeline.fit(data)

    val endTime = System.currentTimeMillis()
    val trainingTime = (endTime - startTime) / 1000.0

    println(f"\n- Training completed in $trainingTime%.2f seconds")

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
      case scala.util.Failure(ex) => println(s"  - Failed to save feature importances: ${ex.getMessage}")
    }
  }
}

/**
 * Companion object for RandomForestModel factory methods
 */
object RandomForestModel {

  /**
   * Create a RandomForestModel from experiment configuration
   * @param experiment Experiment configuration
   * @return New RandomForestModel instance
   */
  def apply(experiment: ExperimentConfig): RandomForestModel = {
    new RandomForestModel(experiment)
  }
}
