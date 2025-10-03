package com.flightdelay.features.pca

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{PCA, PCAModel}
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Advanced PCA Feature Extractor with variance-based component selection.
 *
 * This class provides multiple strategies for dimensionality reduction using PCA:
 * 1. Variance-based: Automatically select components to reach target variance (e.g., 95%)
 * 2. K-based: Select fixed number of components
 * 3. Variance analysis: Compute explained variance, cumulative variance, scree plot data
 *
 * Industry best practices implemented:
 * - Fit on training data, transform on train/test/production
 * - Return reusable PipelineModel
 * - Comprehensive variance analysis
 * - Model persistence support
 *
 * @constructor Creates a PCA feature extractor with configurable selection strategy
 * @param inputCol Input column name containing feature vectors (default: "features")
 * @param outputCol Output column name for PCA-transformed features (default: "pcaFeatures")
 * @param k Number of principal components (used if varianceThreshold is None)
 * @param varianceThreshold Optional target cumulative variance (0.0 to 1.0, e.g., 0.95 for 95%)
 *
 * @example
 * {{{
 *   // Example 1: Variance-based selection (keep 95% variance)
 *   val pca = new PCAFeatureExtractor(
 *     inputCol = "features",
 *     outputCol = "pcaFeatures",
 *     varianceThreshold = Some(0.95)
 *   )
 *   val (model, trainTransformed, analysis) = pca.fitTransform(trainDF)
 *   println(s"Selected ${analysis.numComponents} components for 95% variance")
 *
 *   // Example 2: Fixed K components
 *   val pca2 = new PCAFeatureExtractor(k = 10)
 *   val (model2, data2, _) = pca2.fitTransform(trainDF)
 *
 *   // Example 3: Builder pattern
 *   val pca3 = PCAFeatureExtractor.builder()
 *     .withInputCol("scaledFeatures")
 *     .withOutputCol("pcaFeatures")
 *     .withVarianceThreshold(0.99)
 *     .build()
 * }}}
 */
class PCAFeatureExtractor(
  val inputCol: String = "features",
  val outputCol: String = "pcaFeatures",
  val k: Int = 10,
  val varianceThreshold: Option[Double] = None
) extends Serializable {

  require(k > 0, "Number of components k must be positive")
  require(
    varianceThreshold.forall(v => v > 0.0 && v <= 1.0),
    "Variance threshold must be between 0.0 and 1.0"
  )

  // ===========================================================================================
  // CORE PCA FITTING AND TRANSFORMATION
  // ===========================================================================================

  /**
   * Fit PCA model on training data
   * @param data Training DataFrame with feature vector
   * @return Fitted PCAModel
   */
  def fit(data: DataFrame): PCAModel = {
    // Determine optimal number of components
    val optimalK = varianceThreshold match {
      case Some(threshold) =>
        // First, fit with maximum possible components to analyze variance
        val maxK = getMaxComponents(data)
        val tempPCA = new PCA()
          .setInputCol(inputCol)
          .setOutputCol(outputCol)
          .setK(maxK)

        val tempModel = tempPCA.fit(data)
        selectComponentsByVariance(tempModel, threshold)

      case None => k
    }

    // Fit final PCA model with optimal K
    println(s"Fitting PCA with k=$optimalK components...")
    val pca = new PCA()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setK(optimalK)

    pca.fit(data)
  }

  /**
   * Fit and transform training data, returning model, transformed data, and analysis
   * @param data Training DataFrame
   * @return Tuple of (PCAModel, transformed DataFrame, variance analysis)
   */
  def fitTransform(data: DataFrame): (PCAModel, DataFrame, VarianceAnalysis) = {
    val model = fit(data)
    val transformed = model.transform(data)
    val analysis = analyzeVariance(model, data)

    println(s"\nPCA Transformation Complete:")
    println(s"  Original features: ${getInputDimension(data)}")
    println(s"  PCA components: ${model.getK}")
    println(f"  Variance explained: ${analysis.cumulativeVariance.last * 100}%.2f%%")

    (model, transformed, analysis)
  }

  /**
   * Transform new data using fitted PCA model
   * @param model Fitted PCAModel
   * @param data New DataFrame to transform
   * @return Transformed DataFrame
   */
  def transform(model: PCAModel, data: DataFrame): DataFrame = {
    model.transform(data)
  }

  // ===========================================================================================
  // VARIANCE ANALYSIS
  // ===========================================================================================

  /**
   * Analyze variance explained by PCA components
   * @param model Fitted PCAModel
   * @param data Original data (for context)
   * @return VarianceAnalysis object with detailed metrics
   */
  def analyzeVariance(model: PCAModel, data: DataFrame): VarianceAnalysis = {
    val explainedVariance = model.explainedVariance.toArray
    val cumulativeVariance = explainedVariance.scanLeft(0.0)(_ + _).tail
    val numComponents = model.getK
    val originalDim = getInputDimension(data)

    VarianceAnalysis(
      numComponents = numComponents,
      originalDimension = originalDim,
      explainedVariance = explainedVariance,
      cumulativeVariance = cumulativeVariance,
      totalVarianceExplained = cumulativeVariance.last,
      componentIndices = (1 to numComponents).toArray
    )
  }

  /**
   * Select optimal number of components based on variance threshold
   * @param model Fitted PCAModel with maximum components
   * @param threshold Target cumulative variance (e.g., 0.95)
   * @return Optimal number of components
   */
  private def selectComponentsByVariance(model: PCAModel, threshold: Double): Int = {
    val explainedVariance = model.explainedVariance.toArray
    val cumulativeVariance = explainedVariance.scanLeft(0.0)(_ + _).tail

    val optimalK = cumulativeVariance.indexWhere(_ >= threshold) + 1

    if (optimalK > 0) {
      println(s"Selected $optimalK components to reach ${threshold * 100}% variance")
      optimalK
    } else {
      println(s"Warning: Could not reach ${threshold * 100}% variance. Using all ${model.getK} components.")
      model.getK
    }
  }

  /**
   * Get maximum possible components (min of features or samples)
   */
  private def getMaxComponents(data: DataFrame): Int = {
    val numFeatures = getInputDimension(data)
    val numSamples = data.count().toInt
    math.min(numFeatures, numSamples)
  }

  /**
   * Get input feature dimension
   */
  private def getInputDimension(data: DataFrame): Int = {
    data.select(inputCol).head().getAs[Vector](0).size
  }

  // ===========================================================================================
  // UTILITY METHODS
  // ===========================================================================================

  /**
   * Print detailed variance analysis report
   */
  def printVarianceReport(analysis: VarianceAnalysis): Unit = {
    println("\n" + "=" * 80)
    println("PCA Variance Analysis Report")
    println("=" * 80)
    println(f"Original Dimensions: ${analysis.originalDimension}")
    println(f"Selected Components: ${analysis.numComponents}")
    println(f"Total Variance Explained: ${analysis.totalVarianceExplained * 100}%.2f%%")
    println("\nVariance by Component:")
    println("-" * 80)
    println(f"${"Component"}%-12s ${"Individual"}%-15s ${"Cumulative"}%-15s ${"Cumulative %"}%-15s")
    println("-" * 80)

    analysis.componentIndices.zip(analysis.explainedVariance).zip(analysis.cumulativeVariance).foreach {
      case ((idx, individual), cumulative) =>
        println(f"PC-$idx%-10d ${individual}%-15.6f ${cumulative}%-15.6f ${cumulative * 100}%-15.2f%%")
    }
    println("=" * 80 + "\n")
  }

  /**
   * Generate scree plot data for visualization
   * @param analysis VarianceAnalysis object
   * @return DataFrame with columns: component, variance, cumulative_variance
   */
  def getScreePlotData(analysis: VarianceAnalysis)(implicit spark: org.apache.spark.sql.SparkSession): DataFrame = {
    import spark.implicits._

    val data = analysis.componentIndices.zip(analysis.explainedVariance).zip(analysis.cumulativeVariance).map {
      case ((idx, variance), cumulative) =>
        (idx, variance, cumulative)
    }

    data.toSeq.toDF("component", "explained_variance", "cumulative_variance")
  }

  /**
   * Find elbow point (optimal K) using variance curve
   * Uses the "elbow method" to find point of diminishing returns
   */
  def findElbowPoint(analysis: VarianceAnalysis): Int = {
    val variances = analysis.explainedVariance

    // Calculate second derivative (rate of change of variance)
    val firstDiff = variances.sliding(2).map(pair => pair(1) - pair(0)).toArray
    val secondDiff = firstDiff.sliding(2).map(pair => pair(1) - pair(0)).toArray

    // Find maximum second derivative (biggest change in slope)
    val elbowIdx = secondDiff.zipWithIndex.maxBy(_._1)._2 + 2

    println(s"Elbow point detected at component $elbowIdx")
    elbowIdx
  }
}

/**
 * Case class for PCA variance analysis results
 */
case class VarianceAnalysis(
  numComponents: Int,
  originalDimension: Int,
  explainedVariance: Array[Double],
  cumulativeVariance: Array[Double],
  totalVarianceExplained: Double,
  componentIndices: Array[Int]
) {
  /**
   * Get variance explained by top N components
   */
  def getVarianceForTopK(k: Int): Double = {
    require(k <= numComponents, s"k must be <= $numComponents")
    cumulativeVariance(k - 1)
  }

  /**
   * Find minimum components needed for target variance
   */
  def getMinComponentsForVariance(threshold: Double): Int = {
    cumulativeVariance.indexWhere(_ >= threshold) + 1
  }

  /**
   * Pretty print summary
   */
  override def toString: String = {
    s"""VarianceAnalysis(
       |  Components: $numComponents/$originalDimension
       |  Total Variance: ${(totalVarianceExplained * 100).round}%
       |  Top 3 Components: ${explainedVariance.take(3).map(v => f"${v * 100}%.1f%%").mkString(", ")}
       |)""".stripMargin
  }
}

/**
 * Companion object with builder pattern and utility methods
 */
object PCAFeatureExtractor {

  /**
   * Builder for fluent API
   */
  class Builder {
    private var inputCol: String = "features"
    private var outputCol: String = "pcaFeatures"
    private var k: Int = 10
    private var varianceThreshold: Option[Double] = None

    def withInputCol(col: String): Builder = {
      this.inputCol = col
      this
    }

    def withOutputCol(col: String): Builder = {
      this.outputCol = col
      this
    }

    def withK(k: Int): Builder = {
      this.k = k
      this.varianceThreshold = None // Clear variance threshold
      this
    }

    def withVarianceThreshold(threshold: Double): Builder = {
      this.varianceThreshold = Some(threshold)
      this
    }

    def build(): PCAFeatureExtractor = {
      new PCAFeatureExtractor(inputCol, outputCol, k, varianceThreshold)
    }
  }

  def builder(): Builder = new Builder()

  /**
   * Quick factory method for variance-based PCA
   */
  def varianceBased(
    threshold: Double,
    inputCol: String = "features",
    outputCol: String = "pcaFeatures"
  ): PCAFeatureExtractor = {
    new PCAFeatureExtractor(inputCol, outputCol, k = 100, Some(threshold))
  }

  /**
   * Quick factory method for K-based PCA
   */
  def fixedK(
    k: Int,
    inputCol: String = "features",
    outputCol: String = "pcaFeatures"
  ): PCAFeatureExtractor = {
    new PCAFeatureExtractor(inputCol, outputCol, k, None)
  }

  /**
   * Perform PCA variance analysis without transformation
   * Useful for exploratory data analysis
   */
  def exploreVariance(
    data: DataFrame,
    inputCol: String = "features",
    maxK: Int = 50
  ): VarianceAnalysis = {
    val pca = new PCAFeatureExtractor(inputCol = inputCol, k = maxK)
    val model = pca.fit(data)
    pca.analyzeVariance(model, data)
  }
}
