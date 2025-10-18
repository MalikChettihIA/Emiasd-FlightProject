package com.flightdelay.features.pipelines

import com.flightdelay.config.{FeatureExtractionConfig, FeatureTransformationConfig}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{BooleanType, DoubleType}

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
  handleInvalid: String = "skip"
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
    val weatherDepthHours = featureConfig.weatherDepthHours

    /**
     * Find all columns that match a feature name pattern
     * Handles both exact matches and exploded weather features
     *
     * Example: "feature_weather_severity_index" matches:
     *   - origin_weather_feature_weather_severity_index-0
     *   - origin_weather_feature_weather_severity_index-1
     *   - origin_weather_feature_weather_severity_index-2
     *   - destination_weather_feature_weather_severity_index-0
     *   - destination_weather_feature_weather_severity_index-1
     *   - destination_weather_feature_weather_severity_index-2
     */
    def findMatchingColumns(featureName: String): Seq[String] = {
      // First try exact match (for flight features)
      if (availableFeatures.contains(featureName)) {
        Seq(featureName)
      } else {
        // Try pattern matching for exploded weather features
        // Pattern: (origin|destination)_weather_${featureName}-${index}
        // Index range: 0 to (weatherDepthHours - 1)

        val indices = 0 until weatherDepthHours
        val patterns = for {
          prefix <- Seq("origin_weather", "destination_weather")
          index <- indices
        } yield s"${prefix}_${featureName}-${index}"

        val matchingCols = patterns.filter(availableFeatures.contains).sorted

        if (matchingCols.nonEmpty) {
          matchingCols
        } else {
          // Feature not found in data - will be silently skipped
          Seq.empty
        }
      }
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
  private def buildPipelineStages(data: DataFrame): Array[PipelineStage] = {
    val (stringIndexerCols, oneHotEncoderCols, numericCols) = groupFeaturesByTransformation(data)

    println(s"\n[ConfigurationBasedPipeline] Feature Distribution:")
    println(s"  - StringIndexer features: ${stringIndexerCols.length}")
    if (stringIndexerCols.length > 0 && stringIndexerCols.length <= 5) {
      println(s"    ${stringIndexerCols.mkString(", ")}")
    } else if (stringIndexerCols.length > 5) {
      println(s"    ${stringIndexerCols.take(5).mkString(", ")}, ... (${stringIndexerCols.length - 5} more)")
    }

    // DIAGNOSTIC: Print distinct value counts for categorical features
    println(s"\n[ConfigurationBasedPipeline] Categorical Features Cardinality:")
    stringIndexerCols.zipWithIndex.foreach { case (colName, idx) =>
      val distinctCount = data.select(colName).distinct().count()
      println(f"  [$idx%2d] $colName%-50s : $distinctCount%,6d distinct values")
    }
    println(s"  - OneHotEncoder features: ${oneHotEncoderCols.length}")
    if (oneHotEncoderCols.length > 0 && oneHotEncoderCols.length <= 5) {
      println(s"    ${oneHotEncoderCols.mkString(", ")}")
    } else if (oneHotEncoderCols.length > 5) {
      println(s"    ${oneHotEncoderCols.take(5).mkString(", ")}, ... (${oneHotEncoderCols.length - 5} more)")
    }
    println(s"  - Numeric features: ${numericCols.length}")
    if (numericCols.length > 0 && numericCols.length <= 5) {
      println(s"    ${numericCols.mkString(", ")}")
    } else if (numericCols.length > 5) {
      println(s"    ${numericCols.take(5).mkString(", ")}, ... (${numericCols.length - 5} more)")
    }

    var stages = Array.empty[PipelineStage]

    // Stage 1: StringIndexer for label
    val labelIndexer = new StringIndexer()
      .setInputCol(target)
      .setOutputCol(_label)
      .setHandleInvalid(handleInvalid)
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

    // Stage 4: VectorAssembler to combine all features
    val allInputCols = stringIndexerCols.map(_prefix + _) ++
                       oneHotOutputCols ++
                       numericCols

    val vectorAssembler = new VectorAssembler()
      .setInputCols(allInputCols)
      .setOutputCol(_featuresVec)
      .setHandleInvalid(handleInvalid)
    stages = stages :+ vectorAssembler

    // Stage 5: Apply scalers if configured for numeric features
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
  def fit(data: DataFrame): PipelineModel = {
    val preprocessed = preprocessBooleans(data)
    val stages = buildPipelineStages(preprocessed)
    val pipeline = new Pipeline().setStages(stages)

    println("\n[ConfigurationBasedPipeline] Fitting transformation pipeline...")
    pipeline.fit(preprocessed)
  }

  /**
   * Fit and transform training data
   */
  def fitTransform(data: DataFrame): (PipelineModel, DataFrame) = {
    val preprocessed = preprocessBooleans(data)
    val stages = buildPipelineStages(preprocessed)
    val pipeline = new Pipeline().setStages(stages)

    println("\n[ConfigurationBasedPipeline] Fitting transformation pipeline...")
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
    val allInputCols = stringIndexerCols.map(_prefix + _) ++
                       oneHotEncoderCols.map(_ohe_prefix + _) ++
                       numericCols

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
  def printSummary(): Unit = {
    println("=" * 80)
    println("[ConfigurationBasedPipeline] Pipeline Configuration")
    println("=" * 80)
    println(s"Target: $target")
    println(s"Total features configured: ${featureTransformations.size}")
    println(s"Handle invalid: $handleInvalid")

    // Group by transformation type
    val byTransformation = featureTransformations.groupBy(_._2.transformation)
    byTransformation.foreach { case (transformation, features) =>
      println(s"\n${transformation}: ${features.size} features")
      if (features.size <= 10) {
        features.keys.foreach(name => println(s"  - $name"))
      } else {
        features.keys.take(10).foreach(name => println(s"  - $name"))
        println(s"  ... and ${features.size - 10} more")
      }
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
    handleInvalid: String = "skip"
  ): ConfigurationBasedFeatureExtractorPipeline = {
    new ConfigurationBasedFeatureExtractorPipeline(featureConfig, target, handleInvalid)
  }
}
