package com.flightdelay.features.pipelines

import com.flightdelay.config.{AppConfiguration, FeatureExtractionConfig, FeatureTransformationConfig}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{BooleanType, DoubleType}
import com.flightdelay.utils.DebugUtils._
import com.flightdelay.utils.MetricsUtils


/**
 * Configuration-based feature pipeline that reads transformations from config
 * Supports:
 * - Categorical: StringIndexer, OneHotEncoder
 * - Numeric: None, StandardScaler, MinMaxScaler, RobustScaler
 * - Boolean: automatic conversion to 0.0/1.0
 * - Automatic label indexing
 *
 * @param featureConfig Feature extraction configuration from YAML
 * @param target Target column name
 * @param handleInvalid Strategy for invalid values
 */
class ConfigurationBasedFeatureExtractorPipeline(
  featureConfig: FeatureExtractionConfig,
  target: String,
  handleInvalid: String = "keep"
) {

  // Column naming constants
  private val _label = "label"
  private val _prefix = "indexed_"
  private val _ohe_prefix = "ohe_"
  private val _featuresVec = "featuresVec"
  private val _features = "features"

  // Extract feature configurations from config
  private val featureTransformations: Map[String, FeatureTransformationConfig] = {
    val flight = featureConfig.flightSelectedFeatures.getOrElse(Map.empty)
    val weather = featureConfig.weatherSelectedFeatures.getOrElse(Map.empty)
    flight ++ weather
  }

  /**
   * Group features by their transformation type
   * ✅ Enhanced to match exploded weather features (e.g., origin_weather_feature_*-2)
   */
  private def groupFeaturesByTransformation(data: DataFrame): (Array[String], Array[String], Array[String]) = {
    val availableFeatures = data.columns.toSet - target

    // Separate depth configurations for origin and destination weather features
    val weatherOriginDepthHours: Int = featureConfig.weatherOriginDepthHours
    val weatherDestinationDepthHours: Int = featureConfig.weatherDestinationDepthHours

    /**
     * Finds all columns that match a given feature name pattern.
     *
     * Logic:
     * 1) Try an exact match first (for standard flight-level features).
     * 2) If not found, try exploded weather features, which can appear as:
     *      - origin_weather_<featureName>-<index>
     *      - destination_weather_<featureName>-<index>
     *    where the index ranges depend on weatherOriginDepthHours and weatherDestinationDepthHours.
     *
     * Example:
     *   featureName = "feature_weather_severity_index" matches:
     *     - origin_weather_feature_weather_severity_index-0
     *     - origin_weather_feature_weather_severity_index-1
     *     - destination_weather_feature_weather_severity_index-0
     *     - destination_weather_feature_weather_severity_index-1
     */
    def findMatchingColumns(featureName: String): Seq[String] = {
      // 1) Try exact match first
      if (availableFeatures.contains(featureName)) {
        return Seq(featureName)
      }

      // 2) Generate exploded weather feature patterns
      val safeOriginDepth = math.max(0, weatherOriginDepthHours)
      val safeDestDepth   = math.max(0, weatherDestinationDepthHours)

      val originPatterns =
        (0 to safeOriginDepth).map(i => s"origin_weather_${featureName}-$i")

      val destPatterns =
        (0 to safeDestDepth).map(i => s"destination_weather_${featureName}-$i")

      val candidates = originPatterns ++ destPatterns
      val matching   = candidates.filter(availableFeatures.contains).sorted

      // 3) Return all matching columns (empty sequence if none found)
      matching
    }

    // Filter and group features with pattern matching
    val stringIndexerFeatures = featureTransformations
      .filter { case (_, config) => config.transformation == "StringIndexer" }
      .flatMap { case (name, _) => findMatchingColumns(name) }
      .toArray

    val oneHotEncoderFeatures = featureTransformations
      .filter { case (_, config) => config.transformation == "OneHotEncoder" }
      .flatMap { case (name, _) => findMatchingColumns(name) }
      .toArray

    val numericFeatures = featureTransformations
      .filter { case (_, config) =>
        config.transformation == "None" || config.transformation.endsWith("Scaler")
      }
      .flatMap { case (name, _) => findMatchingColumns(name) }
      .toArray

    (stringIndexerFeatures, oneHotEncoderFeatures, numericFeatures)
  }

  /**
   * Preprocessing: Convert boolean columns to numeric (0.0/1.0)
   */
  private def preprocessBooleans(df: DataFrame): DataFrame = {
    val booleanCols = df.schema.fields
      .filter(_.dataType == BooleanType)
      .map(_.name)
      .filterNot(_ == target)

    if (booleanCols.isEmpty) {
      df
    } else {
      var result = df
      booleanCols.foreach { colName =>
        result = result.withColumn(
          colName,
          when(col(colName) === true, 1.0)
            .when(col(colName) === false, 0.0)
            .otherwise(null)
            .cast(DoubleType)
        )
      }
      result
    }
  }

  /**
   * Build pipeline stages based on configuration
   */
  private def buildPipelineStages(data: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): Array[PipelineStage] = {
    val (stringIndexerCols, oneHotEncoderCols, numericCols) = groupFeaturesByTransformation(data)

    info(s"[ConfigurationBasedFeatureExtractorPipeline] Feature Distribution:")
    info(s"  - StringIndexer features: ${stringIndexerCols.length}")
    if (stringIndexerCols.length > 0 && stringIndexerCols.length <= 5) {
      info(s"    ${stringIndexerCols.mkString(", ")}")
    } else if (stringIndexerCols.length > 5) {
      info(s"    ${stringIndexerCols.take(5).mkString(", ")}, ... (${stringIndexerCols.length - 5} more)")
    }

    whenDebug{
      // DIAGNOSTIC: Print distinct value counts for categorical features
      debug(s"[ConfigurationBasedFeatureExtractorPipeline] Categorical Features Cardinality:")
      stringIndexerCols.zipWithIndex.foreach { case (colName, idx) =>
        val distinctCount = data.select(colName).distinct().count()
        debug(f"  [$idx%2d] $colName%-50s : $distinctCount%,6d distinct values")
      }
      debug(s"  - OneHotEncoder features: ${oneHotEncoderCols.length}")
      if (oneHotEncoderCols.length > 0 && oneHotEncoderCols.length <= 5) {
        debug(s"    ${oneHotEncoderCols.mkString(", ")}")
      } else if (oneHotEncoderCols.length > 5) {
        debug(s"    ${oneHotEncoderCols.take(5).mkString(", ")}, ... (${oneHotEncoderCols.length - 5} more)")
      }
      debug(s"  - Numeric features: ${numericCols.length}")
      if (numericCols.length > 0 && numericCols.length <= 5) {
        debug(s"    ${numericCols.mkString(", ")}")
      } else if (numericCols.length > 5) {
        debug(s"    ${numericCols.take(5).mkString(", ")}, ... (${numericCols.length - 5} more)")
      }
    }

    var stages = Array.empty[PipelineStage]

    // Stage 1: StringIndexer for label
    // CRITICAL: Always use "skip" for label to ensure binary classification (2 classes only)
    // Using "keep" would create a 3rd class for unknowns, causing evaluation errors
    val labelIndexer = new StringIndexer()
      .setInputCol(target)
      .setOutputCol(_label)
      .setHandleInvalid("skip")  // Always skip for label, regardless of handleInvalid parameter
    stages = stages :+ labelIndexer

    // Stage 2: StringIndexer for categorical features
    if (stringIndexerCols.nonEmpty) {
      val categoricalIndexer = new StringIndexer()
        .setInputCols(stringIndexerCols)
        .setOutputCols(stringIndexerCols.map(_prefix + _))
        .setHandleInvalid(handleInvalid)
      stages = stages :+ categoricalIndexer
    }

    // Stage 3: OneHotEncoder for categorical features that need it
    val oneHotInputCols = oneHotEncoderCols
    val oneHotIndexedCols = oneHotInputCols.map(_prefix + _)
    val oneHotOutputCols = oneHotInputCols.map(_ohe_prefix + _)

    if (oneHotInputCols.nonEmpty) {
      // First index them
      val oneHotIndexer = new StringIndexer()
        .setInputCols(oneHotInputCols)
        .setOutputCols(oneHotIndexedCols)
        .setHandleInvalid(handleInvalid)
      stages = stages :+ oneHotIndexer

      // Then one-hot encode
      val oneHotEncoder = new OneHotEncoder()
        .setInputCols(oneHotIndexedCols)
        .setOutputCols(oneHotOutputCols)
        .setHandleInvalid(handleInvalid)
      stages = stages :+ oneHotEncoder
    }

    // Stage 4: Imputer to replace NaN/null values in numeric columns
    // This is critical because VectorAssembler and ML models cannot handle NaN
    if (numericCols.nonEmpty) {
      val imputer = new Imputer()
        .setInputCols(numericCols)
        .setOutputCols(numericCols.map("imputed_" + _))
        .setStrategy("mean")  // Replace NaN with mean (alternatives: "median", "mode")
        .setMissingValue(Double.NaN)
      stages = stages :+ imputer

      info(s"  - Added Imputer for ${numericCols.length} numeric columns (strategy: mean)")
    }

    // Stage 5: VectorAssembler to combine all features
    val imputedNumericCols = if (numericCols.nonEmpty) numericCols.map("imputed_" + _) else Array.empty[String]
    val allInputCols = stringIndexerCols.map(_prefix + _) ++
                       oneHotOutputCols ++
                       imputedNumericCols

    val vectorAssembler = new VectorAssembler()
      .setInputCols(allInputCols)
      .setOutputCol(_featuresVec)
      .setHandleInvalid(handleInvalid)
    stages = stages :+ vectorAssembler

    // Stage 6: Apply scalers if configured for numeric features
    val scalerConfig = numericCols.flatMap { colName =>
      featureTransformations.get(colName).map(config => (colName, config.transformation))
    }.groupBy(_._2)

    if (scalerConfig.contains("StandardScaler") ||
        scalerConfig.contains("MinMaxScaler") ||
        scalerConfig.contains("RobustScaler")) {

      // Apply StandardScaler to the entire feature vector
      // Note: In a real scenario, you might want to apply different scalers to different subsets
      val scaler = new StandardScaler()
        .setInputCol(_featuresVec)
        .setOutputCol(_features)
        .setWithMean(true)
        .setWithStd(true)
      stages = stages :+ scaler
    } else {
      // No scaling, just rename the column
      // We'll handle this in postProcess
    }

    stages
  }

  /**
   * Fit the pipeline on training data
   */
  def fit(data: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): PipelineModel = {
    val preprocessed = preprocessBooleans(data)
    val stages = buildPipelineStages(preprocessed)
    val pipeline = new Pipeline().setStages(stages)

    info("[ConfigurationBasedFeatureExtractorPipeline] Fitting transformation pipeline...")
    pipeline.fit(preprocessed)
  }

  /**
   * Fit and transform training data
   */
  def fitTransform(data: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): (PipelineModel, DataFrame) = {
    val preprocessed = preprocessBooleans(data)
    val stages = buildPipelineStages(preprocessed)
    val pipeline = new Pipeline().setStages(stages)

    info("[ConfigurationBasedFeatureExtractorPipeline] Fitting transformation pipeline...")
    val model = pipeline.fit(preprocessed)

    val transformed = postProcess(model.transform(preprocessed))
    (model, transformed)
  }

  /**
   * Get the names of features after transformation (as they appear in the final vector)
   * This returns the exact feature names that correspond to indices in the feature vector
   *
   * @param data Input DataFrame to analyze
   * @return Array of feature names in the order they appear in the feature vector
   */
  def getTransformedFeatureNames(data: DataFrame): Array[String] = {
    val (stringIndexerCols, oneHotEncoderCols, numericCols) = groupFeaturesByTransformation(data)

    // Build the same order as VectorAssembler uses
    val imputedNumericCols = if (numericCols.nonEmpty) numericCols.map("imputed_" + _) else Array.empty[String]
    val allInputCols = stringIndexerCols.map(_prefix + _) ++
                       oneHotEncoderCols.map(_ohe_prefix + _) ++
                       imputedNumericCols

    allInputCols
  }

  /**
   * Transform new data using fitted model
   */
  def transform(model: PipelineModel, data: DataFrame): DataFrame = {
    val preprocessed = preprocessBooleans(data)
    postProcess(model.transform(preprocessed))
  }

  /**
   * Post-process: Select final columns (features, label)
   */
  private def postProcess(df: DataFrame): DataFrame = {
    val finalFeaturesCol = if (df.columns.contains(_features)) {
      _features
    } else {
      _featuresVec
    }

    df.select(
      col(finalFeaturesCol).as("features"),
      col(_label).as("label")
    )
  }

  /**
   * Print summary of the pipeline configuration
   */
  def printSummary()(implicit spark: SparkSession, configuration: AppConfiguration): Unit = {
    info("=" * 80)
    info("[ConfigurationBasedFeatureExtractorPipeline] Pipeline Configuration")
    info("=" * 80)
    info(s"Target: $target")
    info(s"Total features configured: ${featureTransformations.size}")
    info(s"Handle invalid: $handleInvalid")

    // Group by transformation type
    val byTransformation = featureTransformations.groupBy(_._2.transformation)
    byTransformation.foreach { case (transformation, features) =>
      info(s"${transformation}: ${features.size} features")
      features.keys.foreach(name => info(s"  - $name"))
    }
    println("=" * 80)
  }
}

/**
 * Companion object with builder pattern
 */
object ConfigurationBasedFeatureExtractorPipeline {

  def apply(
    featureConfig: FeatureExtractionConfig,
    target: String,
    handleInvalid: String = "keep"
  ): ConfigurationBasedFeatureExtractorPipeline = {
    new ConfigurationBasedFeatureExtractorPipeline(featureConfig, target, handleInvalid)
  }
}
