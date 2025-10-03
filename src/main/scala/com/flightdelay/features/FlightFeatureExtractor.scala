package com.flightdelay.features

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import com.flightdelay.data.utils.DataQualityMetrics
import com.flightdelay.features.pipelines.{BasicFlightFeaturePipeline, EnhancedFlightFeaturePipeline}
import com.flightdelay.features.pca.{PCAFeatureExtractor, VarianceAnalysis}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.PCAModel

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
  def extract(data: DataFrame, target: String)(implicit configuration: AppConfiguration): DataFrame = {
    extractInternal(data, target, usePCA = false, None)._1
  }

  /**
   * Extract features without PCA and save to parquet
   * @param data Input DataFrame with flight data
   * @param target Target column name
   * @param outputPath Path to save the extracted features
   * @return DataFrame with features and label columns
   */
  def extractAndSave(data: DataFrame, target: String, outputPath: String)(implicit configuration: AppConfiguration): DataFrame = {
    val extracted = extract(data, target)

    println(s"\n[FlightFeatureExtractor] Saving extracted features to: $outputPath")
    extracted.write.mode("overwrite").parquet(outputPath)
    println(s"[FlightFeatureExtractor] ✓ Features saved successfully")

    extracted
  }

  /**
   * Extract features with PCA dimensionality reduction
   * @param data Input DataFrame with flight data
   * @param target Target column name
   * @param varianceThreshold Minimum cumulative variance to retain (default: 0.60 for 60%)
   * @return Tuple of (transformed DataFrame, PCAModel, VarianceAnalysis)
   */
  def extractWithPCA(
    data: DataFrame,
    target: String,
    varianceThreshold: Double = defaultVarianceThreshold
  )(implicit configuration: AppConfiguration): (DataFrame, PCAModel, VarianceAnalysis) = {
    require(
      varianceThreshold > 0.0 && varianceThreshold <= 1.0,
      s"Variance threshold must be between 0.0 and 1.0, got $varianceThreshold"
    )

    println(s"\n[FlightFeatureExtractor] PCA enabled with ${varianceThreshold * 100}% variance threshold")

    val (df, Some(pcaModel), Some(analysis)) = extractInternal(
      data,
      target,
      usePCA = true,
      Some(varianceThreshold)
    )

    println("\n" + "=" * 80)
    println("Feature Extraction Summary (with PCA)")
    println("=" * 80)
    println(f"Original Features    : ${analysis.originalDimension}")
    println(f"PCA Components       : ${analysis.numComponents}")
    println(f"Variance Explained   : ${analysis.totalVarianceExplained * 100}%.2f%%")
    println(f"Dimensionality Reduction: ${(1 - analysis.numComponents.toDouble / analysis.originalDimension) * 100}%.1f%%")
    println("=" * 80)

    // Display sample data
    println("\nSample of extracted features:")
    df.show(5, truncate = false)

    // Save PCA model
    val pcaModelPath = s"${configuration.output.basePath}/models/pca_${configuration.featureExtraction.pcaVarianceThreshold}_${configuration.model.target}"
    println(s"\nSaving PCA model to: $pcaModelPath")
    pcaModel.write.overwrite().save(pcaModelPath)
    println("✓ PCA model saved")

    // Save extracted features
    val featuresPath = s"${configuration.output.basePath}/features/pca_features_${configuration.model.target}"
    println(s"Saving extracted features to: $featuresPath")
    df.write.mode("overwrite").parquet(featuresPath)
    println("✓ Features saved")

    (df, pcaModel, analysis)
  }

  /**
   * Extract features with PCA and save both features and model
   * @param data Input DataFrame with flight data
   * @param target Target column name
   * @param varianceThreshold Minimum cumulative variance to retain
   * @param featuresOutputPath Path to save the extracted features
   * @param modelOutputPath Path to save the PCA model
   * @return Tuple of (transformed DataFrame, PCAModel, VarianceAnalysis)
   */
  def extractWithPCAAndSave(
    data: DataFrame,
    target: String,
    varianceThreshold: Double = defaultVarianceThreshold,
    featuresOutputPath: String,
    modelOutputPath: String
  )(implicit configuration: AppConfiguration): (DataFrame, PCAModel, VarianceAnalysis) = {

    val (extracted, pcaModel, analysis) = extractWithPCA(data, target, varianceThreshold)

    // Save features
    println(s"\n[FlightFeatureExtractor] Saving PCA features to: $featuresOutputPath")
    extracted.write.mode("overwrite").parquet(featuresOutputPath)
    println(s"[FlightFeatureExtractor] ✓ PCA features saved successfully")

    // Save PCA model
    println(s"[FlightFeatureExtractor] Saving PCA model to: $modelOutputPath")
    pcaModel.write.overwrite().save(modelOutputPath)
    println(s"[FlightFeatureExtractor] ✓ PCA model saved successfully")

    (extracted, pcaModel, analysis)
  }

  /**
   * Internal extraction method with optional PCA
   */
  private def extractInternal(
    data: DataFrame,
    target: String,
    usePCA: Boolean,
    varianceThreshold: Option[Double]
  )(implicit configuration: AppConfiguration): (DataFrame, Option[PCAModel], Option[VarianceAnalysis]) = {

    println("\n" + "=" * 80)
    println(s"[FeatureExtractor] Feature Extraction - Start")
    println("=" * 80)
    println(s"\nTarget column: $target")

    // Step 1: Drop unused labels (keep only target)
    val labelsToDrop = data.columns
      .filter(colName => colName.startsWith("label_") && colName != target)
    val flightData = data.drop(labelsToDrop: _*)

    println(s"  → Dropped ${labelsToDrop.length} unused label columns")

    // Step 2: Detect column types using DataQualityMetrics
    val flightDataMetric = DataQualityMetrics.metrics(flightData)
    val textCols = flightDataMetric
      .filter(col("colType").contains(DataQualityMetrics._text))
      .select("name")
      .rdd.flatMap(x => x.toSeq).map(x => x.toString).collect

    val numericCols = flightDataMetric
      .filter(col("colType").contains(DataQualityMetrics._numeric))
      .filter(!col("name").contains(target))
      .select("name")
      .rdd.flatMap(x => x.toSeq).map(x => x.toString).collect

    println(s"  → Detected ${textCols.length} text columns and ${numericCols.length} numeric columns")

    // Step 3: Apply feature pipeline (with or without scaling based on PCA usage)
    val baseFeatures = if (usePCA) {
      // Use EnhancedFlightFeaturePipeline with StandardScaler for PCA
      println(s"\n  → Using EnhancedFlightFeaturePipeline with StandardScaler (PCA mode)")
      val enhancedPipeline = new EnhancedFlightFeaturePipeline(
        textCols = textCols,
        numericCols = numericCols,
        target = target,
        maxCat = maxCat,
        handleInvalid = handleInvalid,
        scalerType = Some("standard")  // Critical for PCA
      )
      val (model, transformed) = enhancedPipeline.fitTransform(flightData)
      transformed
    } else {
      // Use BasicFlightFeaturePipeline without scaling for tree-based models
      println(s"\n  → Using BasicFlightFeaturePipeline (tree-based models)")
      val basicPipeline = new BasicFlightFeaturePipeline(textCols, numericCols, target, maxCat, handleInvalid)
      basicPipeline.fit(flightData)
    }

    println(s"  ✓ Feature pipeline completed")

    // Step 4: Apply PCA if enabled
    if (usePCA && varianceThreshold.isDefined) {
      val pca = PCAFeatureExtractor.varianceBased(
        threshold = varianceThreshold.get,
        inputCol = "features",
        outputCol = _pcaFeatures
      )

      val (pcaModel, pcaData, analysis) = pca.fitTransform(baseFeatures)

      // Print PCA summary
      println(s"\n" + "=" * 50)
      println("PCA Summary")
      println("=" * 50)
      println(f"Original features:    ${analysis.originalDimension}%4d")
      println(f"PCA components:       ${analysis.numComponents}%4d")
      println(f"Variance explained:   ${analysis.totalVarianceExplained * 100}%6.2f%%")
      println(f"Dimensionality reduction: ${(1 - analysis.numComponents.toDouble / analysis.originalDimension) * 100}%6.2f%%")
      println("=" * 50)

      // Select final columns: pcaFeatures -> features, label
      val finalData = pcaData
        .select(col(_pcaFeatures).alias("features"), col("label"))

      // Save extracted features
      val featuresPath = s"${configuration.output.basePath}/features/pca_features_${configuration.model.target}"
      println(s"\nSaving extracted features:")
      println(s"  → Path: $featuresPath")
      finalData.write.mode("overwrite").parquet(featuresPath)
      println(s"  ✓ Saved ${finalData.count()} records with PCA features")

      println("\n" + "=" * 80)
      println("[FeatureExtractor] Feature Extraction - End")
      println("=" * 80 + "\n")

      (finalData, Some(pcaModel), Some(analysis))
    } else {

      // Save extracted features
      val featuresPath = s"${configuration.output.basePath}/features/base_features_${configuration.model.target}"
      println(s"\nSaving extracted features:")
      println(s"  → Path: $featuresPath")
      baseFeatures.write.mode("overwrite").parquet(featuresPath)
      println(s"  ✓ Saved ${baseFeatures.count()} records with base features")

      println("\n" + "=" * 80)
      println("[FeatureExtractor] Feature Extraction - End")
      println("=" * 80 + "\n")

      (baseFeatures, None, None)
    }
  }

  /**
   * Explore optimal PCA variance threshold
   * @param data Input DataFrame
   * @param target Target column
   * @param maxK Maximum number of components to test (default: 50)
   * @return VarianceAnalysis for exploration
   */
  def explorePCAVariance(
    data: DataFrame,
    target: String,
    maxK: Int = 50
  )(implicit configuration: AppConfiguration): VarianceAnalysis = {
    println(s"\n[FlightFeatureExtractor] Exploring PCA variance with maxK=$maxK")

    // Get base features without PCA
    val (baseFeatures, _, _) = extractInternal(data, target, usePCA = false, None)

    // Perform variance analysis
    val analysis = PCAFeatureExtractor.exploreVariance(
      baseFeatures,
      inputCol = "features",
      maxK = maxK
    )

    // Print recommendations
    println(s"\nPCA Variance Exploration Results:")
    println(s"  - For 60% variance: ${analysis.getMinComponentsForVariance(0.60)} components")
    println(s"  - For 75% variance: ${analysis.getMinComponentsForVariance(0.75)} components")
    println(s"  - For 90% variance: ${analysis.getMinComponentsForVariance(0.90)} components")
    println(s"  - For 95% variance: ${analysis.getMinComponentsForVariance(0.95)} components")

    analysis
  }
}
