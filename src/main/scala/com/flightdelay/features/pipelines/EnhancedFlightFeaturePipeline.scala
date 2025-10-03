package com.flightdelay.features.pipelines

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * Enhanced feature pipeline with advanced capabilities:
 * - Optional feature scaling (standard, minmax, robust)
 * - Feature selection (chi-squared, ANOVA)
 * - Custom stage injection
 * - Reusable PipelineModel for train/test/production
 *
 * This class addresses the limitations of BasicFlightFeaturePipeline by providing:
 * 1. Feature scaling for non-tree-based models (SVM, Logistic Regression, Neural Networks)
 * 2. Feature selection to reduce dimensionality and improve model performance
 * 3. Extensibility through custom pipeline stages
 * 4. Model reusability for consistent transformations across datasets
 *
 * @constructor Creates an enhanced pipeline with configurable preprocessing stages.
 * @param textCols Categorical columns to encode
 * @param numericCols Numeric columns to include in features
 * @param target Target column name (will be indexed and renamed to "label")
 * @param maxCat Max categories for VectorIndexer (default: 32)
 * @param handleInvalid Strategy for invalid values: "skip", "error", "keep" (default: "skip")
 * @param scalerType Optional scaler: "standard", "minmax", "robust", or None
 * @param featureSelector Optional feature selection: (type, numFeatures) where type is "chi2" or "anova"
 * @param customStages Additional custom pipeline stages to inject
 *
 * @example
 * {{{
 *   // Example 1: With standard scaling
 *   val pipeline = new EnhancedFlightFeaturePipeline(
 *     textCols = Array("carrier", "route_id"),
 *     numericCols = Array("departure_hour", "distance_score"),
 *     target = "label_is_delayed_15min",
 *     scalerType = Some("standard")
 *   )
 *   val (model, trainData) = pipeline.fitTransform(trainDF)
 *   val testData = pipeline.transform(model, testDF)
 *
 *   // Example 2: With feature selection and scaling
 *   val pipeline2 = new EnhancedFlightFeaturePipeline(
 *     textCols = textCols,
 *     numericCols = numericCols,
 *     target = "label_is_delayed_60min",
 *     featureSelector = Some(("chi2", 30)),
 *     scalerType = Some("minmax")
 *   )
 *
 *   // Example 3: Using builder pattern
 *   val pipeline3 = EnhancedFlightFeaturePipeline.builder()
 *     .withTextCols(textCols)
 *     .withNumericCols(numericCols)
 *     .withTarget("label_is_delayed_15min")
 *     .withScaler("robust")
 *     .withFeatureSelection("anova", 40)
 *     .build()
 * }}}
 */
class EnhancedFlightFeaturePipeline(
  textCols: Array[String],
  numericCols: Array[String],
  target: String,
  maxCat: Int = 32,
  handleInvalid: String = "skip",
  scalerType: Option[String] = None,
  featureSelector: Option[(String, Int)] = None,
  customStages: Array[PipelineStage] = Array.empty
) {

  // Column naming constants
  private val _label = "label"
  private val _prefix = "indexed_"
  private val _featuresVec = "featuresVec"
  private val _featuresVecIndex = "featuresVecIndexed"
  private val _selectedFeatures = "selectedFeatures"
  private val _features = "features"

  // ===========================================================================================
  // PIPELINE STAGES DEFINITION
  // ===========================================================================================

  /**
   * Stage 1: StringIndexer
   * Encodes categorical text columns and target to numeric indices
   */
  private val stringIndexer = new StringIndexer()
    .setInputCols(textCols ++ Array(target))
    .setOutputCols(textCols.map(_prefix + _) ++ Array(_prefix + target))
    .setHandleInvalid(handleInvalid)

  /**
   * Stage 2: VectorAssembler
   * Combines all features (indexed categorical + numeric) into a single vector
   */
  private val vectorAssembler = new VectorAssembler()
    .setInputCols(textCols.map(_prefix + _) ++ numericCols)
    .setOutputCol(_featuresVec)
    .setHandleInvalid(handleInvalid)

  /**
   * Stage 3: VectorIndexer
   * Auto-detects and indexes categorical features for tree-based models
   */
  private val vectorIndexer = new VectorIndexer()
    .setInputCol(_featuresVec)
    .setOutputCol(_featuresVecIndex)
    .setMaxCategories(maxCat)
    .setHandleInvalid(handleInvalid)

  /**
   * Stage 4: Optional Feature Selector
   * Reduces feature dimensionality using statistical tests
   */
  private val selectorStage: Option[PipelineStage] = featureSelector.map {
    case ("chi2", numFeatures) =>
      new ChiSqSelector()
        .setNumTopFeatures(numFeatures)
        .setFeaturesCol(_featuresVecIndex)
        .setLabelCol(_prefix + target)
        .setOutputCol(_selectedFeatures)

    case ("anova", numFeatures) =>
      new UnivariateFeatureSelector()
        .setFeatureType("continuous")
        .setLabelType("categorical")
        .setSelectionMode("numTopFeatures")
        .setSelectionThreshold(numFeatures)
        .setFeaturesCol(_featuresVecIndex)
        .setLabelCol(_prefix + target)
        .setOutputCol(_selectedFeatures)

    case (selectorType, _) =>
      throw new IllegalArgumentException(s"Unknown selector type: $selectorType. Use 'chi2' or 'anova'")
  }

  /**
   * Stage 5: Optional Scaler
   * Normalizes features for distance-based and gradient-based algorithms
   */
  private val scalerStage: Option[PipelineStage] = scalerType.map {
    case "standard" =>
      val inputCol = selectorStage.map(_ => _selectedFeatures).getOrElse(_featuresVecIndex)
      new StandardScaler()
        .setInputCol(inputCol)
        .setOutputCol(_features)
        .setWithMean(true)
        .setWithStd(true)

    case "minmax" =>
      val inputCol = selectorStage.map(_ => _selectedFeatures).getOrElse(_featuresVecIndex)
      new MinMaxScaler()
        .setInputCol(inputCol)
        .setOutputCol(_features)
        .setMin(0.0)
        .setMax(1.0)

    case "robust" =>
      val inputCol = selectorStage.map(_ => _selectedFeatures).getOrElse(_featuresVecIndex)
      new RobustScaler()
        .setInputCol(inputCol)
        .setOutputCol(_features)
        .setWithCentering(true)
        .setWithScaling(true)

    case scaler =>
      throw new IllegalArgumentException(s"Unknown scaler type: $scaler. Use 'standard', 'minmax', or 'robust'")
  }

  // ===========================================================================================
  // PIPELINE CONSTRUCTION
  // ===========================================================================================

  /**
   * Builds the complete pipeline with all configured stages
   * @return Configured Pipeline ready for fitting
   */
  private def buildPipeline(): Pipeline = {
    val baseStages = Array(stringIndexer, vectorAssembler, vectorIndexer)
    val withSelector = selectorStage.map(s => baseStages :+ s).getOrElse(baseStages)
    val withScaler = scalerStage.map(s => withSelector :+ s).getOrElse(withSelector)
    val allStages = withScaler ++ customStages

    new Pipeline().setStages(allStages)
  }

  // ===========================================================================================
  // PUBLIC API
  // ===========================================================================================

  /**
   * Fit the pipeline on training data and return the fitted model
   * @param data Training DataFrame
   * @return Fitted PipelineModel that can be reused on new data
   */
  def fit(data: DataFrame): PipelineModel = {
    buildPipeline().fit(data)
  }

  /**
   * Fit and transform training data in one operation
   * @param data Training DataFrame
   * @return Tuple of (fitted model, transformed DataFrame with features and label)
   */
  def fitTransform(data: DataFrame): (PipelineModel, DataFrame) = {
    val model = fit(data)
    val transformed = postProcess(model.transform(data))
    (model, transformed)
  }

  /**
   * Transform new data using a previously fitted model
   * @param model Fitted PipelineModel from fit() or fitTransform()
   * @param data New DataFrame to transform
   * @return Transformed DataFrame with features and label columns
   */
  def transform(model: PipelineModel, data: DataFrame): DataFrame = {
    postProcess(model.transform(data))
  }

  /**
   * Post-process transformed data to select final columns
   * @param df Transformed DataFrame from pipeline
   * @return DataFrame with only "features" and "label" columns
   */
  private def postProcess(df: DataFrame): DataFrame = {
    val finalFeaturesCol = if (scalerStage.isDefined) {
      _features
    } else if (selectorStage.isDefined) {
      _selectedFeatures
    } else {
      _featuresVecIndex
    }

    df.select(
      col(finalFeaturesCol).as("features"),
      col(_prefix + target).as(_label)
    )
  }

  /**
   * Get all pipeline stages for inspection
   * @return Array of all configured PipelineStages
   */
  def getStages: Array[PipelineStage] = buildPipeline().getStages

  /**
   * Print pipeline configuration summary
   */
  def printSummary(): Unit = {
    println("=" * 80)
    println("Enhanced Flight Feature Pipeline Configuration")
    println("=" * 80)
    println(s"Text columns: ${textCols.mkString(", ")}")
    println(s"Numeric columns: ${numericCols.mkString(", ")}")
    println(s"Target: $target")
    println(s"Max categories: $maxCat")
    println(s"Handle invalid: $handleInvalid")
    println(s"Scaler: ${scalerType.getOrElse("None")}")
    println(s"Feature selector: ${featureSelector.map(f => s"${f._1} (top ${f._2})").getOrElse("None")}")
    println(s"Custom stages: ${customStages.length}")
    println(s"Total stages: ${getStages.length}")
    println("=" * 80)
  }
}

/**
 * Companion object providing builder pattern for fluent API
 */
object EnhancedFlightFeaturePipeline {

  /**
   * Builder class for constructing EnhancedFlightFeaturePipeline with fluent API
   */
  class Builder {
    private var textCols: Array[String] = Array.empty
    private var numericCols: Array[String] = Array.empty
    private var target: String = ""
    private var maxCat: Int = 32
    private var handleInvalid: String = "skip"
    private var scalerType: Option[String] = None
    private var featureSelector: Option[(String, Int)] = None
    private var customStages: Array[PipelineStage] = Array.empty

    def withTextCols(cols: Array[String]): Builder = {
      this.textCols = cols
      this
    }

    def withNumericCols(cols: Array[String]): Builder = {
      this.numericCols = cols
      this
    }

    def withTarget(col: String): Builder = {
      this.target = col
      this
    }

    def withMaxCategories(max: Int): Builder = {
      this.maxCat = max
      this
    }

    def withHandleInvalid(strategy: String): Builder = {
      require(
        strategy == "skip" || strategy == "error" || strategy == "keep",
        "handleInvalid must be 'skip', 'error', or 'keep'"
      )
      this.handleInvalid = strategy
      this
    }

    def withScaler(scaler: String): Builder = {
      this.scalerType = Some(scaler)
      this
    }

    def withoutScaler(): Builder = {
      this.scalerType = None
      this
    }

    def withFeatureSelection(selectorType: String, numFeatures: Int): Builder = {
      this.featureSelector = Some((selectorType, numFeatures))
      this
    }

    def withoutFeatureSelection(): Builder = {
      this.featureSelector = None
      this
    }

    def withCustomStage(stage: PipelineStage): Builder = {
      this.customStages = this.customStages :+ stage
      this
    }

    def withCustomStages(stages: Array[PipelineStage]): Builder = {
      this.customStages = this.customStages ++ stages
      this
    }

    def build(): EnhancedFlightFeaturePipeline = {
      require(
        textCols.nonEmpty || numericCols.nonEmpty,
        "Must provide at least one feature column (text or numeric)"
      )
      require(target.nonEmpty, "Target column must be specified")

      new EnhancedFlightFeaturePipeline(
        textCols,
        numericCols,
        target,
        maxCat,
        handleInvalid,
        scalerType,
        featureSelector,
        customStages
      )
    }
  }

  /**
   * Creates a new builder instance
   * @return New Builder for fluent pipeline construction
   */
  def builder(): Builder = new Builder()
}
