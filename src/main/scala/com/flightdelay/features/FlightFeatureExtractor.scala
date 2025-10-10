package com.flightdelay.features

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.PCAModel

import com.flightdelay.data.utils.DataQualityMetrics
import com.flightdelay.features.pipelines.{BasicFlightFeaturePipeline, EnhancedFlightFeaturePipeline}
import com.flightdelay.features.pca.{PCAFeatureExtractor, VarianceAnalysis}
import com.flightdelay.features.leakage.DataLeakageProtection


import scala.sys.process._
/**
 * Flight Feature Extractor - Main entry point for feature engineering pipeline.
 *
 * This object provides feature extraction with optional PCA dimensionality reduction:
 * 1. Automatic column type detection (text vs numeric)
 * 2. Feature vectorization and indexing
 * 3. Optional PCA with variance-based component selection
 *
 * @example
 * {{{
 *   // Without PCA (default)
 *   val data = FlightFeatureExtractor.extract(df, target = "label_is_delayed_15min")
 *
 *   // With PCA (60% variance)
 *   val (data, pcaModel, analysis) = FlightFeatureExtractor.extractWithPCA(
 *     df,
 *     target = "label_is_delayed_15min",
 *     varianceThreshold = 0.60
 *   )
 * }}}
 */
object FlightFeatureExtractor {

  private val maxCat = 32
  private val handleInvalid = "skip"
  private val defaultVarianceThreshold = 0.70 // 70% minimum variance

  private val _featuresVec = "featuresVec"
  private val _featuresVecIndex = "features"
  private val _pcaFeatures = "pcaFeatures"

  /**
   * Extract features without PCA (original behavior)
   * @param data Input DataFrame with flight data
   * @param target Target column name
   * @return DataFrame with features and label columns
   */
  def extract(data: DataFrame, experiment: ExperimentConfig)(implicit configuration: AppConfiguration): DataFrame = {

    val target = experiment.target

    // Step 1: Drop Data leakage
    val flightData = DataLeakageProtection.clean(data, target)

    println("\n" + "=" * 80)
    println(s"[STEP 3][FeatureExtractor] Feature Extraction - Start")
    println("=" * 80)
    println(s"\nTarget column: $target")

    // Step 2: Detect column types using DataQualityMetrics
    val flightDataMetric = DataQualityMetrics.metrics(flightData)
    val allTextCols = flightDataMetric
      .filter(col("colType").contains(DataQualityMetrics._text))
      .select("name")
      .rdd.flatMap(x => x.toSeq).map(x => x.toString).collect

    val allNumericCols = flightDataMetric
      .filter(col("colType").contains(DataQualityMetrics._numeric))
      .filter(!col("name").contains(target))
      .select("name")
      .rdd.flatMap(x => x.toSeq).map(x => x.toString).collect

    println(s"  - Detected ${allTextCols.length} text columns and ${allNumericCols.length} numeric columns")

    // Step 3: Apply feature selection if enabled
    val (textCols, numericCols) = if (experiment.featureExtraction.isFeatureSelectionEnabled) {
      experiment.featureExtraction.selectedFeatures match {
        case Some(selectedFeatureNames) =>
          val selectedSet = selectedFeatureNames.toSet
          val filteredTextCols = allTextCols.filter(selectedSet.contains)
          val filteredNumericCols = allNumericCols.filter(selectedSet.contains)

          println(s"\n  [Feature Selection] Enabled")
          println(s"  - Selected features: ${selectedFeatureNames.length}")
          println(s"  - Filtered to ${filteredTextCols.length} text columns and ${filteredNumericCols.length} numeric columns")
          println(s"  - Selected text columns: ${filteredTextCols.take(5).mkString(", ")}${if (filteredTextCols.length > 5) "..." else ""}")
          println(s"  - Selected numeric columns: ${filteredNumericCols.take(5).mkString(", ")}${if (filteredNumericCols.length > 5) "..." else ""}")

          (filteredTextCols, filteredNumericCols)
        case None =>
          println(s"  - Feature selection enabled but no selectedFeatures specified, using all features")
          (allTextCols, allNumericCols)
      }
    } else {
      (allTextCols, allNumericCols)
    }

    val (baseFeatures, featureNames) = {
      // Determine scaler type based on feature extraction type
      val scalerType = if (experiment.featureExtraction.isPcaEnabled) {
        Some("standard")  // Critical for PCA
      } else {
        None  // No scaling for feature_selection or other types
      }

      val pipelineMode = if (experiment.featureExtraction.isPcaEnabled) "PCA mode"
                         else if (experiment.featureExtraction.isFeatureSelectionEnabled) "Feature Selection mode"
                         else "Standard mode"

      println(s"\n  - Using EnhancedFlightFeaturePipeline${if (scalerType.isDefined) " with StandardScaler" else ""} ($pipelineMode)")

      val enhancedPipeline = new EnhancedFlightFeaturePipeline(
        textCols = textCols,
        numericCols = numericCols,
        target = target,
        maxCat = maxCat,
        handleInvalid = handleInvalid,
        scalerType = scalerType
      )
      val (model, transformed) = enhancedPipeline.fitTransform(flightData)

      // Build feature names: indexed text columns + numeric columns
      val names = textCols.map("indexed_" + _) ++ numericCols
      (transformed, names)
    }

    println(s"  - Feature pipeline completed")
    println(s"  - Feature count: ${featureNames.length} features")
    println(s"  - Sample feature names: ${featureNames.take(5).mkString(", ")}...")

    val (transformedDF, pcaModel, varianceAnalysis) : (DataFrame, Option[PCAModel], Option[VarianceAnalysis])
    = if (experiment.featureExtraction.isPcaEnabled) {
      // Extract features with PCA
      val (df, model, analysis) = FlightFeatureExtractor.applyPCA(
        baseFeatures,
        featureNames,
        experiment
      )
      (df, Some(model), Some(analysis))
    } else {
      // Return Extracted Features Without PCA
      (baseFeatures, None, None)
    }

    // Display sample data
    println("\nSample of extracted features:")
    transformedDF.show(5, truncate = false)

    // Display summary based on extraction type
    if (experiment.featureExtraction.isFeatureSelectionEnabled) {
      println("\n" + "=" * 80)
      println("[STEP 3] Feature Extraction Summary (Feature Selection)")
      println("=" * 80)
      println(f"Total Features Available : ${allTextCols.length + allNumericCols.length}")
      println(f"Selected Features        : ${textCols.length + numericCols.length}")
      println(f"Text Features            : ${textCols.length}")
      println(f"Numeric Features         : ${numericCols.length}")
      println("=" * 80)
    } else if (!experiment.featureExtraction.isPcaEnabled) {
      println("\n" + "=" * 80)
      println("[STEP 3] Feature Extraction Summary (All Features)")
      println("=" * 80)
      println(f"Text Features            : ${textCols.length}")
      println(f"Numeric Features         : ${numericCols.length}")
      println(f"Total Features           : ${textCols.length + numericCols.length}")
      println("=" * 80)
    }

    saveResult(transformedDF,
      pcaModel,
      experiment,
      featureNames)
    transformedDF
  }

  /**
   * Extract features with PCA dimensionality reduction
   * @param data Input DataFrame with flight data
   * @param target Target column name
   * @param varianceThreshold Minimum cumulative variance to retain (default: 0.60 for 60%)
   * @return Tuple of (transformed DataFrame, PCAModel, VarianceAnalysis)
   */
  def applyPCA(
    data: DataFrame,
    featureNames: Array[String],
    experiment: ExperimentConfig)(implicit configuration: AppConfiguration): (DataFrame, PCAModel, VarianceAnalysis) = {

    val varianceThreshold = experiment.featureExtraction.pcaVarianceThreshold

    println(s"\n- PCA enabled with ${varianceThreshold * 100}% variance threshold")

    val (df, Some(pcaModel), Some(analysis)) = {
      val pca = PCAFeatureExtractor.varianceBased(
        threshold = varianceThreshold,
        outputCol = _pcaFeatures
      )

      val (pcaModel, pcaData, analysis) = pca.fitTransform(data)

      // Print PCA summary
      println(s"\n" + "=" * 50)
      println("[STEP 3] PCA Summary")
      println("=" * 50)
      println(f"Original features:    ${analysis.originalDimension}%4d")
      println(f"PCA components:       ${analysis.numComponents}%4d")
      println(f"Variance explained:   ${analysis.totalVarianceExplained * 100}%6.2f%%")
      println(f"Dimensionality reduction: ${(1 - analysis.numComponents.toDouble / analysis.originalDimension) * 100}%6.2f%%")
      println("=" * 50)

      // Save PCA metrics for visualization
      val pcaMetricsPath = s"${configuration.common.output.basePath}/${experiment.name}/metrics/pca_analysis"
      println(s"\n[Saving PCA Metrics]")
      println(s"  - Metrics path: $pcaMetricsPath")
      println(s"  - Feature names count: ${featureNames.length}")

      pca.saveVarianceAnalysis(analysis, s"${configuration.common.output.basePath}/${experiment.name}/metrics/pca_variance.csv")
      pca.savePCAProjections(pcaData, s"${configuration.common.output.basePath}/${experiment.name}/metrics/pca_projections.csv",
        labelCol = Some("label"), maxSamples = 5000)
      pca.savePCALoadings(pcaModel, s"${configuration.common.output.basePath}/${experiment.name}/metrics/pca_loadings.csv",
        topN = Some(10), featureNames = Some(featureNames))

      // Also save feature names separately for reference
      pca.saveFeatureNames(featureNames, s"${configuration.common.output.basePath}/${experiment.name}/features/feature_names.csv")

      // Select final columns: pcaFeatures -> features, label
      val finalData = pcaData
        .select(col(_pcaFeatures).alias("features"), col("label"))

      println("\n" + "=" * 80)
      println("[STEP 3][Visualization Command]")
      println("=" * 80)

      println(s"  To visualize PCA analysis, run:")
      println(s"  python work/scripts/visualize_pca.py $pcaMetricsPath")
      println("")

      (finalData, Some(pcaModel), Some(analysis))
    }

    println("\n" + "=" * 80)
    println("[STEP 3] Feature Extraction Summary (with PCA)")
    println("=" * 80)
    println(f"Original Features    : ${analysis.originalDimension}")
    println(f"PCA Components       : ${analysis.numComponents}")
    println(f"Variance Explained   : ${analysis.totalVarianceExplained * 100}%.2f%%")
    println(f"Dimensionality Reduction: ${(1 - analysis.numComponents.toDouble / analysis.originalDimension) * 100}%.1f%%")
    println("=" * 80)

    (df, pcaModel, analysis)
  }


  /**
   * Save Extraction Result to CSV and Parquet files
   */
  private def saveResult(
    data: DataFrame,
    pcaModel: Option[PCAModel],
    experiment: ExperimentConfig,
    featureNames: Array[String]
  )(implicit configuration: AppConfiguration): (DataFrame) = {

    //Experiment OutputPath:
    val experimentOutputPath = s"${configuration.common.output.basePath}/${experiment.name}"

    // Save PCA model if present
    pcaModel.foreach { model =>
      val pcaModelPath = s"${experimentOutputPath}/models/pca_model"
      println(s"\nSaving PCA model to: $pcaModelPath")
      model.write.overwrite().save(pcaModelPath)
      println("- PCA model saved")
    }

    // Save feature names if feature selection is enabled
    if (experiment.featureExtraction.isFeatureSelectionEnabled) {
      val featureNamesPath = s"${experimentOutputPath}/features/selected_features.txt"
      import java.io.PrintWriter
      import java.io.File

      println(s"\nSaving selected feature names to: $featureNamesPath")
      val outputDir = new File(s"${experimentOutputPath}/features")
      if (!outputDir.exists()) {
        outputDir.mkdirs()
      }

      val writer = new PrintWriter(new File(featureNamesPath))
      try {
        featureNames.foreach(writer.println)
        println(s"  - Saved ${featureNames.length} feature names")
      } finally {
        writer.close()
      }
    }

    // Save extracted features
    val featuresPath = s"${experimentOutputPath}/features/extracted_features"
    println(s"\nSaving extracted features:")
    println(s"  - Path: $featuresPath")
    data.write.mode("overwrite").parquet(featuresPath)
    println(s"  - Saved ${data.count()} records with extracted features")

    data
  }

}
