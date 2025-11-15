package com.flightdelay.features.selection

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{ChiSqSelector, UnivariateFeatureSelector, VectorSlicer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Hybrid Feature Selector that applies different selection strategies to numerical and categorical features.
 *
 * This implementation follows the pattern from PySpark ML best practices:
 * 1. Separate numerical and categorical features using VectorSlicer
 * 2. Apply appropriate feature selection to each type
 * 3. Reassemble selected features
 *
 * Supports:
 * - Chi-Square selection for categorical features (one-hot encoded)
 * - ANOVA F-test for numerical features
 * - Configurable selection criteria (FPR, FDR, FWE, numTopFeatures, percentile)
 *
 * @constructor Creates a hybrid feature selector
 * @param numericalIndices Indices of numerical features in the input vector
 * @param categoricalIndices Indices of categorical (OHE) features in the input vector
 * @param labelCol Label column name for supervised selection
 * @param inputCol Input features column (default: "features")
 * @param outputCol Output selected features column (default: "selectedFeatures")
 * @param categoricalSelectorType Selection type for categorical: "fpr", "fdr", "fwe", "numTopFeatures", "percentile"
 * @param categoricalThreshold Threshold value for categorical selector
 * @param numericalSelectorType Selection type for numerical (default: None to keep all)
 * @param numericalThreshold Threshold value for numerical selector
 *
 * @example
 * {{{
 *   // Example from notebook: 13 numerical + 30 categorical features
 *   val numericalIndices = (0 until 13).toArray
 *   val categoricalIndices = (13 until 43).toArray
 *
 *   val selector = new HybridFeatureSelector(
 *     numericalIndices = numericalIndices,
 *     categoricalIndices = categoricalIndices,
 *     labelCol = "has_purchased",
 *     categoricalSelectorType = "fpr",
 *     categoricalThreshold = 0.05
 *   )
 *
 *   val (model, selected, info) = selector.fitTransform(trainDF)
 *   println(s"Selected ${info.totalSelectedFeatures} features from ${info.originalFeatures}")
 * }}}
 */
class HybridFeatureSelector(
  val numericalIndices: Array[Int],
  val categoricalIndices: Array[Int],
  val labelCol: String,
  val inputCol: String = "features",
  val outputCol: String = "selectedFeatures",
  val categoricalSelectorType: String = "fpr",
  val categoricalThreshold: Double = 0.05,
  val numericalSelectorType: Option[String] = None,
  val numericalThreshold: Option[Double] = None
) extends Serializable {

  // Validate inputs
  require(numericalIndices.nonEmpty || categoricalIndices.nonEmpty, "Must provide at least one feature index")
  require(categoricalThreshold > 0, "Categorical threshold must be positive")

  // Column names for intermediate transformations
  private val numericalFeaturesCol = "numerical_features"
  private val categoricalFeaturesCol = "categorical_features"
  private val selectedCategoricalCol = "selected_categorical"
  private val selectedNumericalCol = "selected_numerical"

  // ===========================================================================================
  // CORE PIPELINE CONSTRUCTION
  // ===========================================================================================

  /**
   * Build the complete feature selection pipeline
   */
  private def buildPipeline(): Pipeline = {
    val stages = scala.collection.mutable.ArrayBuffer.empty[org.apache.spark.ml.PipelineStage]

    // Stage 1: Extract numerical features (if any)
    if (numericalIndices.nonEmpty) {
      val numericalSlicer = new VectorSlicer()
        .setInputCol(inputCol)
        .setOutputCol(numericalFeaturesCol)
        .setIndices(numericalIndices)
      stages += numericalSlicer

      // Optional: Apply feature selection to numerical features
      numericalSelectorType.foreach { selectorType =>
        val numericalSelector = new UnivariateFeatureSelector()
          .setFeaturesCol(numericalFeaturesCol)
          .setLabelCol(labelCol)
          .setOutputCol(selectedNumericalCol)
          .setFeatureType("continuous")
          .setLabelType("categorical")
          .setSelectionMode(selectorType)

        numericalThreshold.foreach { threshold =>
          numericalSelector.setSelectionThreshold(threshold)
        }

        stages += numericalSelector
      }
    }

    // Stage 2: Extract categorical features (if any)
    if (categoricalIndices.nonEmpty) {
      val categoricalSlicer = new VectorSlicer()
        .setInputCol(inputCol)
        .setOutputCol(categoricalFeaturesCol)
        .setIndices(categoricalIndices)
      stages += categoricalSlicer

      // Stage 3: Apply Chi-Square selection to categorical features
      val chiSqSelector = new ChiSqSelector()
        .setFeaturesCol(categoricalFeaturesCol)
        .setLabelCol(labelCol)
        .setOutputCol(selectedCategoricalCol)
        .setSelectorType(categoricalSelectorType)

      // Set appropriate threshold parameter
      categoricalSelectorType match {
        case "fpr" => chiSqSelector.setFpr(categoricalThreshold)
        case "fdr" => chiSqSelector.setFdr(categoricalThreshold)
        case "fwe" => chiSqSelector.setFwe(categoricalThreshold)
        case "numTopFeatures" => chiSqSelector.setNumTopFeatures(categoricalThreshold.toInt)
        case "percentile" => chiSqSelector.setPercentile(categoricalThreshold)
        case _ => throw new IllegalArgumentException(s"Unknown selector type: $categoricalSelectorType")
      }

      stages += chiSqSelector
    }

    // Stage 4: Reassemble selected features
    val inputCols = scala.collection.mutable.ArrayBuffer.empty[String]

    // Determine which columns to assemble based on selection
    if (numericalIndices.nonEmpty) {
      inputCols += (if (numericalSelectorType.isDefined) selectedNumericalCol else numericalFeaturesCol)
    }
    if (categoricalIndices.nonEmpty) {
      inputCols += selectedCategoricalCol
    }

    val finalAssembler = new VectorAssembler()
      .setInputCols(inputCols.toArray)
      .setOutputCol(outputCol)
      .setHandleInvalid("keep")

    stages += finalAssembler

    new Pipeline().setStages(stages.toArray)
  }

  // ===========================================================================================
  // PUBLIC API
  // ===========================================================================================

  /**
   * Fit the feature selection pipeline
   */
  def fit(data: DataFrame): PipelineModel = {
    println(s"Fitting Hybrid Feature Selector...")
    println(s"  Numerical features: ${numericalIndices.length}")
    println(s"  Categorical features: ${categoricalIndices.length}")
    println(s"  Categorical selector: $categoricalSelectorType (threshold=$categoricalThreshold)")

    buildPipeline().fit(data)
  }

  /**
   * Fit and transform, returning model, transformed data, and selection info
   */
  def fitTransform(data: DataFrame): (PipelineModel, DataFrame, SelectionInfo) = {
    val model = fit(data)
    val transformed = model.transform(data)

    // Extract selection information
    val info = extractSelectionInfo(model, data)

    println(s"Feature Selection Complete:")
    println(s"  Original features: ${info.originalFeatures}")
    println(s"  Selected numerical: ${info.selectedNumerical}")
    println(s"  Selected categorical: ${info.selectedCategorical}")
    println(s"  Total selected: ${info.totalSelectedFeatures}")
    println(s"  Reduction: ${info.reductionPercentage}%")

    (model, transformed, info)
  }

  /**
   * Transform new data using fitted model
   */
  def transform(model: PipelineModel, data: DataFrame): DataFrame = {
    model.transform(data)
  }

  /**
   * Extract selection information from fitted model
   */
  private def extractSelectionInfo(model: PipelineModel, data: DataFrame): SelectionInfo = {
    val originalFeatures = numericalIndices.length + categoricalIndices.length

    // Find ChiSqSelector stage to get selected categorical features
    val chiSqModel = model.stages.collectFirst {
      case stage: org.apache.spark.ml.feature.ChiSqSelectorModel => stage
    }

    val selectedCategorical = chiSqModel.map(_.selectedFeatures.length).getOrElse(categoricalIndices.length)

    // Find UnivariateFeatureSelector stage if present
    val univariateModel = model.stages.collectFirst {
      case stage: org.apache.spark.ml.feature.UnivariateFeatureSelectorModel => stage
    }

    val selectedNumerical = univariateModel.map(_.selectedFeatures.length).getOrElse(numericalIndices.length)

    val totalSelected = selectedNumerical + selectedCategorical
    val reductionPct = ((originalFeatures - totalSelected).toDouble / originalFeatures * 100).round.toInt

    SelectionInfo(
      originalFeatures = originalFeatures,
      selectedNumerical = selectedNumerical,
      selectedCategorical = selectedCategorical,
      totalSelectedFeatures = totalSelected,
      reductionPercentage = reductionPct,
      categoricalIndices = chiSqModel.map(_.selectedFeatures).getOrElse(Array.empty)
    )
  }

  /**
   * Get selected feature indices from fitted model
   */
  def getSelectedFeatures(model: PipelineModel): Array[Int] = {
    val info = extractSelectionInfo(model, null)

    // Map selected indices back to original feature space
    val selectedNumerical = (0 until info.selectedNumerical).map(numericalIndices(_))
    val selectedCategorical = info.categoricalIndices.map(idx => categoricalIndices(idx))

    (selectedNumerical ++ selectedCategorical).toArray
  }
}

/**
 * Case class for feature selection information
 */
case class SelectionInfo(
  originalFeatures: Int,
  selectedNumerical: Int,
  selectedCategorical: Int,
  totalSelectedFeatures: Int,
  reductionPercentage: Int,
  categoricalIndices: Array[Int]
) {
  override def toString: String = {
    s"""SelectionInfo(
       |  Original: $originalFeatures features
       |  Selected: $totalSelectedFeatures features ($selectedNumerical numerical + $selectedCategorical categorical)
       |  Reduction: $reductionPercentage%
       |)""".stripMargin
  }
}

/**
 * Companion object with builder and utilities
 */
object HybridFeatureSelector {

  /**
   * Builder for fluent API
   */
  class Builder {
    private var numericalIndices: Array[Int] = Array.empty
    private var categoricalIndices: Array[Int] = Array.empty
    private var labelCol: String = ""
    private var inputCol: String = "features"
    private var outputCol: String = "selectedFeatures"
    private var categoricalSelectorType: String = "fpr"
    private var categoricalThreshold: Double = 0.05
    private var numericalSelectorType: Option[String] = None
    private var numericalThreshold: Option[Double] = None

    def withNumericalIndices(indices: Array[Int]): Builder = {
      this.numericalIndices = indices
      this
    }

    def withCategoricalIndices(indices: Array[Int]): Builder = {
      this.categoricalIndices = indices
      this
    }

    def withLabelCol(col: String): Builder = {
      this.labelCol = col
      this
    }

    def withInputCol(col: String): Builder = {
      this.inputCol = col
      this
    }

    def withOutputCol(col: String): Builder = {
      this.outputCol = col
      this
    }

    def withCategoricalSelector(selectorType: String, threshold: Double): Builder = {
      this.categoricalSelectorType = selectorType
      this.categoricalThreshold = threshold
      this
    }

    def withNumericalSelector(selectorType: String, threshold: Double): Builder = {
      this.numericalSelectorType = Some(selectorType)
      this.numericalThreshold = Some(threshold)
      this
    }

    def build(): HybridFeatureSelector = {
      require(labelCol.nonEmpty, "Label column must be specified")

      new HybridFeatureSelector(
        numericalIndices,
        categoricalIndices,
        labelCol,
        inputCol,
        outputCol,
        categoricalSelectorType,
        categoricalThreshold,
        numericalSelectorType,
        numericalThreshold
      )
    }
  }

  def builder(): Builder = new Builder()

  /**
   * Factory method for Chi-Square FPR selection (like the notebook)
   */
  def chiSquareFPR(
    numericalIndices: Array[Int],
    categoricalIndices: Array[Int],
    labelCol: String,
    fpr: Double = 0.05,
    inputCol: String = "features"
  ): HybridFeatureSelector = {
    new HybridFeatureSelector(
      numericalIndices = numericalIndices,
      categoricalIndices = categoricalIndices,
      labelCol = labelCol,
      inputCol = inputCol,
      categoricalSelectorType = "fpr",
      categoricalThreshold = fpr
    )
  }

  /**
   * Factory method for top-K feature selection
   */
  def topK(
    numericalIndices: Array[Int],
    categoricalIndices: Array[Int],
    labelCol: String,
    k: Int,
    inputCol: String = "features"
  ): HybridFeatureSelector = {
    new HybridFeatureSelector(
      numericalIndices = numericalIndices,
      categoricalIndices = categoricalIndices,
      labelCol = labelCol,
      inputCol = inputCol,
      categoricalSelectorType = "numTopFeatures",
      categoricalThreshold = k.toDouble
    )
  }
}
