package com.flightdelay.features

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.PCAModel
import com.flightdelay.data.utils.ColumnTypeDetector
import com.flightdelay.features.pipelines.{ConfigurationBasedFeatureExtractorPipeline, EnhancedDataFeatureExtractorPipeline}
import com.flightdelay.features.pca.{PCAFeatureExtractor, VarianceAnalysis}
import com.flightdelay.features.leakage.DataLeakageProtection
import com.flightdelay.utils.DebugUtils._

import scala.sys.process._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.DoubleType

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets

/**
 * Stores all fitted models from feature extraction pipeline
 * to enable transform on new data without refitting (avoiding data leakage)
 */
case class FeatureExtractionModels(
  basePipelineModel: PipelineModel,
  pcaModel: Option[PCAModel],
  featureNames: Array[String],
  varianceAnalysis: Option[VarianceAnalysis]
)

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
object FeatureExtractor {

  private val defaultVarianceThreshold = 0.70 // 70% minimum variance

  private val _featuresVec = "featuresVec"
  private val _featuresVecIndex = "features"
  private val _pcaFeatures = "pcaFeatures"

  /**
   * Extract features from training data (fits transformers)
   *
   * This method should be used ONLY on training data. It fits all transformers
   * (StringIndexer, Scaler, PCA, etc.) on the provided data and returns both
   * the transformed data and the fitted models.
   *
   * @param data Input DataFrame with flight data (training set)
   * @param experiment Experiment configuration
   * @return Tuple of (transformed DataFrame, fitted models for reuse on test set)
   */
  def extract(data: DataFrame, experiment: ExperimentConfig)(implicit configuration: AppConfiguration, spark: SparkSession): (DataFrame, FeatureExtractionModels) = {

    val dataWithLabel = data.withColumn("label", col(experiment.target).cast(DoubleType))
    val extractionStartTime = System.currentTimeMillis()
    val target = experiment.target

    var stepStartTime = System.currentTimeMillis()
    val cleaData = DataLeakageProtection.clean(dataWithLabel, target)
    var stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    info(s"  - Data leakage protection completed in ${stepDuration}s")

    // Step 2: Apply feature extraction pipeline
    stepStartTime = System.currentTimeMillis()
    val (baseFeatures, featureNames, basePipelineModel) = {
      // Check if using configuration-based pipeline (when transformations are specified)
      val useConfigBasedPipeline = experiment.featureExtraction.flightSelectedFeatures.isDefined ||
                                     experiment.featureExtraction.weatherSelectedFeatures.isDefined

      if (useConfigBasedPipeline) {
        info(s"  - Using ConfigurationBasedFeatureExtractorPipeline (transformations from config)")

        val configPipeline = ConfigurationBasedFeatureExtractorPipeline(
          featureConfig = experiment.featureExtraction,
          target = target,
          handleInvalid = experiment.featureExtraction.handleInvalid
        )

        // Print pipeline configuration summary
        configPipeline.printSummary()

        val (model, transformed) = configPipeline.fitTransform(cleaData, experiment)

        // OPTIMIZATION: Cache transformed features before any further operations
        info("  - Caching transformed features...")
        val cachedTransformed = transformed.cache()

        // Force materialization with a single count
        debug(s"  - Transformed ${cachedTransformed.count()} records")

        //  Get transformed feature names (after StringIndexer, OneHotEncoder, explosion)
        // These names correspond to the actual indices in the feature vector
        val transformedNames = configPipeline.getTransformedFeatureNames(cleaData)
        info(s"  - Transformed feature count: ${transformedNames.length} features")

        (cachedTransformed, transformedNames, model)

      } else {
        // Fallback to automatic type detection with EnhancedDataFeatureExtractorPipeline
        info(s"  - Using EnhancedDataFeatureExtractorPipeline (automatic type detection)")

        //  Column type detection is ONLY needed for EnhancedDataFeatureExtractorPipeline
        info(s"  - Detecting column types...")
        val detectionStart = System.currentTimeMillis()

        val (allNumericCols, allTextCols, allBooleanCols, allDateCols) =
          ColumnTypeDetector.detectColumnTypesWithHeuristics(
            cleaData,
            excludeColumns = Seq(target),
            maxCardinalityForCategorical = experiment.featureExtraction.maxCategoricalCardinality,
            sampleFraction = 0.01
          )

        ColumnTypeDetector.printSummary(allNumericCols, allTextCols, allBooleanCols, allDateCols)
        ColumnTypeDetector.printDateTypeDetails(cleaData, allDateCols)

        val detectionDuration = (System.currentTimeMillis() - detectionStart) / 1000.0
        info(s"  - Column type detection completed in ${detectionDuration}s")

        // Determine scaler type based on feature extraction type
        val scalerType = if (experiment.featureExtraction.isPcaEnabled) {
          Some("standard")  // Critical for PCA
        } else {
          Some("standard")  // No scaling for feature_selection or other types
        }

        val pipelineMode = if (experiment.featureExtraction.isPcaEnabled) "PCA mode"
                           else if (experiment.featureExtraction.isFeatureSelectionEnabled) "Feature Selection mode"
                           else "Standard mode"

        info(s"  - Mode: $pipelineMode${if (scalerType.isDefined) " with StandardScaler" else ""}")

        val enhancedPipeline = new EnhancedDataFeatureExtractorPipeline(
          textCols = allTextCols,
          numericCols = allNumericCols,
          booleanCols = allBooleanCols,
          dateCols = allDateCols,
          target = target,
          maxCat = experiment.featureExtraction.maxCategoricalCardinality,
          handleInvalid = experiment.featureExtraction.handleInvalid,
          scalerType = scalerType
        )

        // Print pipeline configuration summary
        enhancedPipeline.printSummary()

        val (model, transformed) = enhancedPipeline.fitTransform(cleaData)

        // OPTIMIZATION: Cache transformed features before any further operations
        info("  - Caching transformed features...")
        val cachedTransformed = transformed.cache()

        // Force materialization with a single count
        val transformedCount = cachedTransformed.count()
        info(s"  - Transformed ${transformedCount} records")

        // Build feature names: indexed text columns + numeric columns + boolean + date-derived features
        val dateFeatureNames = buildDateFeatureNames(allDateCols, cleaData)

        info(s"  - Date columns (${allDateCols.length}): ${allDateCols.mkString(", ")}")
        info(s"  - Date-derived features (${dateFeatureNames.length}): ${dateFeatureNames.mkString(", ")}")

        val names = allTextCols.map("indexed_" + _) ++ allNumericCols ++ allBooleanCols ++ dateFeatureNames
        (cachedTransformed, names, model)
      }
    }

    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    info(s"  - Feature pipeline completed in ${stepDuration}s")
    info(s"  - Feature count: ${featureNames.length} features")
    if (featureNames.length <= 5) {
      info(s"  - Feature names: ${featureNames.mkString(", ")}")
    } else {
      info(s"  - Sample features: ${featureNames.take(5).mkString(", ")} ...")
    }

    stepStartTime = System.currentTimeMillis()
    val (transformedDF, pcaModel, varianceAnalysis) : (DataFrame, Option[PCAModel], Option[VarianceAnalysis])
    = if (experiment.featureExtraction.isPcaEnabled) {
      // Extract features with PCA
      val (df, model, analysis) = FeatureExtractor.applyPCA(
        baseFeatures,
        featureNames,
        experiment
      )
      (df, Some(model), Some(analysis))
    } else {
      // No PCA: check if we need to rename featuresVec -> features
      // (StandardScaler in pipeline already does this if present)
      val finalFeatures = if (baseFeatures.columns.contains("features")) {
        info(s"  - Column 'features' already exists (created by StandardScaler)")
        baseFeatures.select(col("features"), col("label"))
      } else {
        info(s"  - Renaming '${_featuresVec}' to 'features' for ML model compatibility")
        baseFeatures.select(col(_featuresVec).alias("features"), col("label"))
      }
      (finalFeatures, None, None)
    }
    if (experiment.featureExtraction.isPcaEnabled) {
      stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
      info(s"  - PCA transformation completed in ${stepDuration}s")
    }

    // Display summary based on extraction type
    info("=" * 80)
    info("[STEP 3] Feature Extraction Summary")
    info("=" * 80)
    info(f"Total Features Extracted : ${featureNames.length}")
    if (experiment.featureExtraction.isPcaEnabled) {
      info(f"Extraction Type          : PCA")
    } else if (experiment.featureExtraction.isFeatureSelectionEnabled) {
      info(f"Extraction Type          : Feature Selection")
    } else {
      info(f"Extraction Type          : Standard")
    }
    info("=" * 80)


    stepStartTime = System.currentTimeMillis()
    saveResult(transformedDF,
      pcaModel,
      experiment,
      featureNames)
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    info(s"  - Results saved in ${stepDuration}s")

    val totalDuration = (System.currentTimeMillis() - extractionStartTime) / 1000.0
    info(s"[STEP 3][FeatureExtractor] Feature Extraction - Completed in ${totalDuration}s")

    // Create models package for reuse on test set (avoid data leakage)
    val models = FeatureExtractionModels(
      basePipelineModel = basePipelineModel,
      pcaModel = pcaModel,
      featureNames = featureNames,
      varianceAnalysis = varianceAnalysis
    )

    (transformedDF, models)
  }

  /**
   * Transform new data using pre-fitted models (for test set)
   *
   * This method should be used on test/validation data. It transforms the data
   * using models that were fitted on the training set, avoiding data leakage.
   *
   * @param data Input DataFrame with flight data (test/validation set)
   * @param models Pre-fitted models from extract() call on training data
   * @param experiment Experiment configuration
   * @return Transformed DataFrame with features and label columns
   */
  def transform(data: DataFrame, models: FeatureExtractionModels, experiment: ExperimentConfig)(implicit configuration: AppConfiguration, spark: SparkSession): DataFrame = {

    val dataWithLabel = data.withColumn("label", col(experiment.target).cast(DoubleType))

    val transformStartTime = System.currentTimeMillis()
    val target = experiment.target

    info("[FeatureExtractor.transform] Transforming new data with pre-fitted models")
    info("   No refitting - using models from training set to avoid data leakage")

    // Step 1: Clean data (remove leakage columns)
    var stepStartTime = System.currentTimeMillis()
    val cleanData = DataLeakageProtection.clean(dataWithLabel, target)
    var stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    info(s"  - Data leakage protection completed in ${stepDuration}s")

    // Step 2: Transform using base pipeline model (NO FIT)
    stepStartTime = System.currentTimeMillis()
    val baseTransformed = models.basePipelineModel.transform(cleanData)

    // Cache transformed features
    info("  - Caching transformed features...")
    val cachedTransformed = baseTransformed.cache()
    val transformedCount = cachedTransformed.count()
    info(s"  - Transformed ${transformedCount} records")

    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    info(s"  - Base pipeline transformation completed in ${stepDuration}s")
    info(s"  - Feature count: ${models.featureNames.length} features")

    // Step 3: Apply PCA if present (NO FIT)
    stepStartTime = System.currentTimeMillis()
    val finalTransformed = models.pcaModel match {
      case Some(pcaModel) =>
        info(s"  - Applying PCA transformation (${models.varianceAnalysis.get.numComponents} components)...")
        val pcaTransformed = pcaModel.transform(cachedTransformed)

        // Select final columns: pcaFeatures -> features, label
        val finalData = pcaTransformed
          .select(col(_pcaFeatures).alias("features"), col("label"))

        stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
        info(s"  - PCA transformation completed in ${stepDuration}s")

        finalData

      case None =>
        // No PCA: check if we need to rename featuresVec -> features
        // (StandardScaler in pipeline already does this if present)
        if (cachedTransformed.columns.contains("features")) {
          info(s"  - Column 'features' already exists (created by StandardScaler)")
          cachedTransformed.select(col("features"), col("label"))
        } else {
          info(s"  - Renaming '${_featuresVec}' to 'features' for ML model compatibility")
          cachedTransformed.select(col(_featuresVec).alias("features"), col("label"))
        }
    }

    val totalDuration = (System.currentTimeMillis() - transformStartTime) / 1000.0
    info(s"   Transform completed in ${totalDuration}s")

    finalTransformed
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
    experiment: ExperimentConfig)(implicit configuration: AppConfiguration, spark: SparkSession): (DataFrame, PCAModel, VarianceAnalysis) = {

    val varianceThreshold = experiment.featureExtraction.pcaVarianceThreshold

    info(s"- PCA enabled with ${varianceThreshold * 100}% variance threshold")

    val (df, Some(pcaModel), Some(analysis)) = {
      val pca = PCAFeatureExtractor.varianceBased(
        threshold = varianceThreshold,
        outputCol = _pcaFeatures
      )

      val (pcaModel, pcaData, analysis) = pca.fitTransform(data)

      // Print PCA summary
      info("=" * 50)
      info("[STEP 3] PCA Summary")
      info("=" * 50)
      info(f"Original features:    ${analysis.originalDimension}%4d")
      info(f"PCA components:       ${analysis.numComponents}%4d")
      info(f"Variance explained:   ${analysis.totalVarianceExplained * 100}%6.2f%%")
      info(f"Dimensionality reduction: ${(1 - analysis.numComponents.toDouble / analysis.originalDimension) * 100}%6.2f%%")
      info("=" * 50)

      // Save PCA metrics for visualization
      val pcaMetricsPath = s"${configuration.common.output.basePath}/${experiment.name}/metrics/pca_analysis"
      info(s"[Saving PCA Metrics]")
      info(s"  - Metrics path: $pcaMetricsPath")
      info(s"  - Feature names count: ${featureNames.length}")

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

      info("=" * 80)
      info("[STEP 3][Visualization Command]")
      info("=" * 80)

      info(s"  To visualize PCA analysis, run:")
      info(s"  python work/scripts/visualize_pca.py $pcaMetricsPath")
      info("")

      (finalData, Some(pcaModel), Some(analysis))
    }

    info("=" * 80)
    info("[STEP 3] Feature Extraction Summary (with PCA)")
    info("=" * 80)
    info(f"Original Features    : ${analysis.originalDimension}")
    info(f"PCA Components       : ${analysis.numComponents}")
    info(f"Variance Explained   : ${analysis.totalVarianceExplained * 100}%.2f%%")
    info(f"Dimensionality Reduction: ${(1 - analysis.numComponents.toDouble / analysis.originalDimension) * 100}%.1f%%")
    info("=" * 80)

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
  )(implicit configuration: AppConfiguration, spark: SparkSession): (DataFrame) = {

    //Experiment OutputPath:
    val experimentOutputPath = s"${configuration.common.output.basePath}/${experiment.name}"

    // Save PCA model if present
    pcaModel.foreach { model =>
      val pcaModelPath = s"${experimentOutputPath}/models/pca_model"
      info(s"Saving PCA model to: $pcaModelPath")
      model.write.overwrite().save(pcaModelPath)
      info("- PCA model saved")
    }

    // Save feature names if feature selection is enabled
    if (experiment.featureExtraction.isFeatureSelectionEnabled) {
      val featuresDirPath = new Path(s"${experimentOutputPath}/features")
      // Get the filesystem that matches the path URI (GCS, HDFS, or local)
      val fs = FileSystem.get(featuresDirPath.toUri, spark.sparkContext.hadoopConfiguration)
      if (!fs.exists(featuresDirPath)) {
        fs.mkdirs(featuresDirPath)
      }

      // Save transformed feature names (these correspond to vector indices)
      val transformedNamesPath = new Path(s"${experimentOutputPath}/features/selected_features.txt")
      info(s"Saving transformed feature names to: ${transformedNamesPath.toString}")
      val transformedOut = fs.create(transformedNamesPath, true)
      val transformedWriter = new BufferedWriter(new OutputStreamWriter(transformedOut, StandardCharsets.UTF_8))
      try {
        featureNames.foreach { name =>
          transformedWriter.write(name)
          transformedWriter.newLine()
        }
        info(s"  - Saved ${featureNames.length} transformed feature names")
      } finally {
        transformedWriter.close()
      }

      // Also save original feature names (from configuration) for reference
      val originalNamesPath = new Path(s"${experimentOutputPath}/features/original_feature_names.txt")
      info(s"Saving original feature names to: ${originalNamesPath.toString}")
      val originalNames = experiment.featureExtraction.getAllFeatureNames
      val originalOut = fs.create(originalNamesPath, true)
      val originalWriter = new BufferedWriter(new OutputStreamWriter(originalOut, StandardCharsets.UTF_8))
      try {
        originalNames.foreach { name =>
          originalWriter.write(name)
          originalWriter.newLine()
        }
        info(s"  - Saved ${originalNames.size} original feature names")
      } finally {
        originalWriter.close()
      }
    }

    // Save extracted features
    val featuresPath = s"${experimentOutputPath}/features/extracted_features.parquet"
    info(s"Saving extracted features:")
    info(s"  - Path: $featuresPath")

    // OPTIMIZATION: Count before save to avoid double materialization
    val recordCount = data.count()
    info(s"  - Records to save: ${recordCount}")

    // OPTIMIZATION: Coalesce to reduce number of output files (improves write performance)
    // Use 100 partitions for balance between parallelism and file count
    // OPTIMIZATION: Use zstd compression (better than snappy)
    data.coalesce(100)
      .write
      .mode("overwrite")
      .option("compression", "zstd")
      .parquet(featuresPath)
    info(s"  - Saved ${recordCount} records with extracted features")

    data
  }

  /**
   * Build feature names for date-derived columns
   * Replicates the logic from EnhancedDataFeatureExtractorPipeline.getDateNumericCols()
   *
   * Each date column generates these features:
   * - date_<colName>_year
   * - date_<colName>_month
   * - date_<colName>_day
   * - date_<colName>_dayofweek
   * - date_<colName>_unix
   *
   * Timestamp columns additionally generate:
   * - date_<colName>_hour
   * - date_<colName>_minute
   */
  private def buildDateFeatureNames(dateCols: Array[String], data: DataFrame): Array[String] = {
    import org.apache.spark.sql.types.{DateType, TimestampType}

    if (dateCols.isEmpty) {
      Array.empty
    } else {
      dateCols.flatMap { colName =>
        // Check if column type is Date or Timestamp
        val colType = data.schema(colName).dataType

        val baseFeatures = Seq(
          s"date_${colName}_year",
          s"date_${colName}_month",
          s"date_${colName}_day",
          s"date_${colName}_dayofweek",
          s"date_${colName}_unix"
        )

        // Add hour and minute for Timestamp columns
        if (colType == TimestampType) {
          baseFeatures ++ Seq(
            s"date_${colName}_hour",
            s"date_${colName}_minute"
          )
        } else {
          baseFeatures
        }
      }
    }
  }

}
