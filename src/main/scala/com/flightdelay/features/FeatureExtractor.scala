package com.flightdelay.features

import com.flightdelay.config.{AppConfiguration, ExperimentConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.PCAModel
import com.flightdelay.data.utils.ColumnTypeDetector
import com.flightdelay.features.pipelines.EnhancedDataFeatureExtractorPipeline
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
object FeatureExtractor {

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
  def extract(data: DataFrame, experiment: ExperimentConfig)(implicit configuration: AppConfiguration, spark: SparkSession): DataFrame = {

    val extractionStartTime = System.currentTimeMillis()
    val target = experiment.target

    var stepStartTime = System.currentTimeMillis()
    val cleaData = DataLeakageProtection.clean(data, target)
    var stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"  - Data leakage protection completed in ${stepDuration}s")

    // Step 2: Detect column types using schema-based detection (ultra-fast)
    stepStartTime = System.currentTimeMillis()

    // Détecter tous les types avec heuristiques
    val (allNumericCols, allTextCols, allBooleanCols, allDateCols) =
      ColumnTypeDetector.detectColumnTypesWithHeuristics(
        cleaData,
        excludeColumns = Seq(target),
        maxCardinalityForCategorical = experiment.featureExtraction.maxCategoricalCardinality,
        sampleFraction = 0.01
      )

    // Afficher le résumé
    ColumnTypeDetector.printSummary(allNumericCols, allTextCols, allBooleanCols, allDateCols)

    // Afficher les détails des dates
    ColumnTypeDetector.printDateTypeDetails(cleaData, allDateCols)

    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"  - Column type detection completed in ${stepDuration}s")
    println(s"  - Detected ${allTextCols.length} categorical columns and ${allNumericCols.length} numeric columns")

    // Step 3: Apply feature selection if enabled

    stepStartTime = System.currentTimeMillis()
    val (baseFeatures, featureNames) = {
      // Determine scaler type based on feature extraction type
      val scalerType = if (experiment.featureExtraction.isPcaEnabled) {
        Some("standard")  // Critical for PCA
      } else {
        Some("standard")  // No scaling for feature_selection or other types
      }

      val pipelineMode = if (experiment.featureExtraction.isPcaEnabled) "PCA mode"
                         else if (experiment.featureExtraction.isFeatureSelectionEnabled) "Feature Selection mode"
                         else "Standard mode"

      println(s"\n  - Using EnhancedFlightFeaturePipeline${if (scalerType.isDefined) " with StandardScaler" else ""} ($pipelineMode)")

      val enhancedPipeline = new EnhancedDataFeatureExtractorPipeline(
        textCols = allTextCols,
        numericCols = allNumericCols,
        booleanCols = allBooleanCols,
        dateCols = allDateCols,
        target = target,
        maxCat = experiment.featureExtraction.maxCategoricalCardinality,
        handleInvalid = handleInvalid,
        scalerType = scalerType
      )

      // Print pipeline configuration summary
      enhancedPipeline.printSummary()

      val (model, transformed) = enhancedPipeline.fitTransform(cleaData)

      // OPTIMIZATION: Cache transformed features before any further operations
      println("  - Caching transformed features...")
      val cachedTransformed = transformed.cache()

      // Force materialization with a single count
      val transformedCount = cachedTransformed.count()
      println(s"  - Transformed ${transformedCount} records")

      // Build feature names: indexed text columns + numeric columns + boolean + date-derived features
      val dateFeatureNames = buildDateFeatureNames(allDateCols, cleaData)

      println(s"\n  - Date columns (${allDateCols.length}): ${allDateCols.mkString(", ")}")
      println(s"  - Date-derived features (${dateFeatureNames.length}): ${dateFeatureNames.mkString(", ")}")

      val names = allTextCols.map("indexed_" + _) ++ allNumericCols ++ allBooleanCols ++ dateFeatureNames
      (cachedTransformed, names)
    }

    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"  - Feature pipeline completed in ${stepDuration}s")
    println(s"  - Feature count: ${featureNames.length} features")
    println(s"  - Sample feature names: ${featureNames.take(5).mkString(", ")}...")

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
      // Return Extracted Features Without PCA
      (baseFeatures, None, None)
    }
    if (experiment.featureExtraction.isPcaEnabled) {
      stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
      println(s"  - PCA transformation completed in ${stepDuration}s")
    }

    // Display summary based on extraction type
    println("\n" + "=" * 80)
    println("[STEP 3] Feature Extraction Summary (Feature Selection)")
    println("=" * 80)
    println(f"Total Features Available : ${allTextCols.length + allNumericCols.length + allBooleanCols.length + allDateCols.length}")
    println(f"Text Features            : ${allTextCols.length}")
    println(f"Boolean Features         : ${allBooleanCols.length}")
    println(f"Date Features            : ${allDateCols.length}")
    println(f"Numeric Features         : ${allNumericCols.length}")
    println("=" * 80)


    stepStartTime = System.currentTimeMillis()
    saveResult(transformedDF,
      pcaModel,
      experiment,
      featureNames)
    stepDuration = (System.currentTimeMillis() - stepStartTime) / 1000.0
    println(s"\n  - Results saved in ${stepDuration}s")

    val totalDuration = (System.currentTimeMillis() - extractionStartTime) / 1000.0
    println(s"\n[STEP 3][FeatureExtractor] Feature Extraction - Completed in ${totalDuration}s")

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
    val featuresPath = s"${experimentOutputPath}/features/extracted_features.parquet"
    println(s"\nSaving extracted features:")
    println(s"  - Path: $featuresPath")

    // OPTIMIZATION: Count before save to avoid double materialization
    val recordCount = data.count()
    println(s"  - Records to save: ${recordCount}")

    // OPTIMIZATION: Coalesce to reduce number of output files (improves write performance)
    // Use 8 partitions for balance between parallelism and file count
    // OPTIMIZATION: Use zstd compression (better than snappy)
    data.coalesce(8)
      .write
      .mode("overwrite")
      .option("compression", "zstd")
      .parquet(featuresPath)
    println(s"  - Saved ${recordCount} records with extracted features")

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
