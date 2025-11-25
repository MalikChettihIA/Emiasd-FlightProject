package com.flightdelay.ml.models

import com.flightdelay.config.ExperimentConfig
import com.flightdelay.utils.MetricsWriter
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets

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
    val hp = experiment.model.hyperparameters

    // Use first value from arrays for single training
    // (Grid Search will iterate over all combinations)
    val numTrees = hp.numTrees.getOrElse(Seq(100)).head
    val maxDepth = hp.maxDepth.getOrElse(Seq(5)).head
    val maxBins = hp.maxBins.getOrElse(Seq(32)).head
    val minInstancesPerNode = hp.minInstancesPerNode.getOrElse(Seq(1)).head
    val subsamplingRate = hp.subsamplingRate.getOrElse(Seq(1.0)).head
    val featureSubsetStrategy = hp.featureSubsetStrategy.getOrElse(Seq("auto")).head
    val impurity = hp.impurity.getOrElse("gini")

    println(s"[RandomForest] Training with hyperparameters:")
    println(s"  - Number of trees: $numTrees")
    println(s"  - Max depth: $maxDepth")
    println(s"  - Max bins: $maxBins")
    println(s"  - Min instances per node: $minInstancesPerNode")
    println(s"  - Subsampling rate: $subsamplingRate")
    println(s"  - Feature subset strategy: $featureSubsetStrategy")
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
      .setMaxBins(maxBins)
      .setMinInstancesPerNode(minInstancesPerNode)
      .setFeatureSubsetStrategy(featureSubsetStrategy)
      .setImpurity(impurity)
      .setSubsamplingRate(subsamplingRate)
      // OPTIMISATIONS CRITIQUES
      .setCacheNodeIds(true)             // Active le cache (améliore perf)
      .setCheckpointInterval(5)         // CRITIQUE : checkpoint tous les 5 arbres
      .setMaxMemoryInMB(2048)            // 2 GB (au lieu de 512 MB)

    // Create pipeline with the classifier
    val pipeline = new Pipeline().setStages(Array(rf))

    println("Starting training...")
    val startTime = System.currentTimeMillis()

    val model = pipeline.fit(data)

    val endTime = System.currentTimeMillis()
    val trainingTime = (endTime - startTime) / 1000.0

    println(f"- Training completed in $trainingTime%.2f seconds")

    // Extract and display feature importance
    val rfModel = model.stages(0).asInstanceOf[RandomForestClassificationModel]
    displayFeatureImportance(rfModel)

    // Save feature importance if path provided
    featureImportancePath.foreach { path =>
      saveFeatureImportance(rfModel, path)

      // Also save text report
      val reportPath = path.replace(".csv", "_report.txt")
      saveFeatureImportanceReport(rfModel, reportPath)
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
   * Helper function to shorten feature names for display
   */
  private def shortenFeatureName(name: String, maxLen: Int = 55): String = {
    if (name.length <= maxLen) {
      name
    } else {
      // Smart truncation: keep the most important parts
      val patterns = Map(
        "indexed_" -> "idx_",
        "origin_weather_" -> "org_w_",
        "destination_weather_" -> "dst_w_",
        "feature_" -> "f_",
        "_operations_risk_level" -> "_opr_risk",
        "_weather_severity_index" -> "_wsev_idx",
        "_is_ifr_conditions" -> "_ifr",
        "_is_vfr_conditions" -> "_vfr",
        "_requires_cat_ii" -> "_cat2"
      )

      var shortened = name
      patterns.foreach { case (long, short) =>
        shortened = shortened.replace(long, short)
      }

      if (shortened.length <= maxLen) {
        shortened
      } else {
        shortened.take(maxLen - 3) + "..."
      }
    }
  }

  /**
   * Display top feature importances from the trained model
   * Enhanced formatting with feature name abbreviation and grouping
   */
  private def displayFeatureImportance(model: RandomForestClassificationModel): Unit = {
    val importances = model.featureImportances.toArray
    val topN = 20

    // Try to load feature names from file
    val featureNames = loadFeatureNames()

    println(f"Top $topN Feature Importances:")
    println("=" * 90)
    println(f"${"Rank"}%-6s ${"Index"}%-7s ${"Feature Name"}%-60s ${"Importance"}%12s")
    println("=" * 90)

    importances.zipWithIndex
      .sortBy(-_._1)
      .take(topN)
      .zipWithIndex
      .foreach { case ((importance, featureIdx), rank) =>
        val featureName = featureNames.lift(featureIdx).getOrElse(s"Feature_$featureIdx")
        val shortName = shortenFeatureName(featureName, 60)
        val importancePercent = importance * 100

        // Visual indicator for importance level
        val indicator = if (importancePercent >= 10) "█"
                       else if (importancePercent >= 5) "▓"
                       else if (importancePercent >= 1) "▒"
                       else "░"

        println(f"${rank + 1}%-6d [${featureIdx}%3d]  ${shortName}%-60s ${indicator} ${importancePercent}%6.2f%%")
      }

    println("=" * 90)

    // Print legend
    println("Importance Levels:  ≥10% ≥5% ≥1% <1%")

    // Print abbreviations used
    println("Abbreviations:")
    println("  idx_    = indexed_")
    println("  org_w_  = origin_weather_")
    println("  dst_w_  = destination_weather_")
    println("  f_      = feature_")
    println("  wsev    = weather_severity")
    println("  opr_risk = operations_risk_level")
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
        println(s" Error loading feature names: ${ex.getMessage}")
        Array.empty[String]
    }
  }

  /**
   * Save feature importances report to text file
   */
  def saveFeatureImportanceReport(model: RandomForestClassificationModel, outputPath: String): Unit = {
    val importances = model.featureImportances.toArray
    val featureNames = loadFeatureNames()

    // Build report content
    val report = new StringBuilder
    report.append("=" * 90).append("\n")
    report.append("Top 20 Feature Importances\n")
    report.append("=" * 90).append("\n")
    report.append(f"Rank   Index   Feature Name${" " * 48}Importance\n")
    report.append("=" * 90).append("\n")

    importances.zipWithIndex
      .sortBy(-_._1)
      .take(20)
      .zipWithIndex
      .foreach { case ((importance, featureIdx), rank) =>
        val featureName = featureNames.lift(featureIdx).getOrElse(s"Feature_$featureIdx")
        val shortName = shortenFeatureName(featureName, 60)
        val importancePercent = importance * 100

        // Visual indicator for importance level
        val indicator = if (importancePercent >= 10) "█"
                       else if (importancePercent >= 5) "▓"
                       else if (importancePercent >= 1) "▒"
                       else "░"

        report.append(f"${rank + 1}%-6d [${featureIdx}%3d]  ${shortName}%-60s ${indicator}  ${importancePercent}%5.2f%%\n")
      }

    report.append("=" * 90).append("\n")
    report.append("Importance Levels:  █≥10% ▓≥5% ▒≥1% ░<1%\n")

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
        writer.write(report.toString)
        println(s" Feature importance report saved to: $outputPath")
      } finally {
        writer.close()
      }
    } catch {
      case ex: Exception =>
        println(s" Failed to save feature importance report: ${ex.getMessage}")
    }
  }

  /**
   * Save feature importances to CSV file with feature names
   */
  private def saveFeatureImportance(model: RandomForestClassificationModel, outputPath: String): Unit = {
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
