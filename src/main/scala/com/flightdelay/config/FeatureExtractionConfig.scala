package com.flightdelay.config

/**
 * Feature transformation configuration
 * @param transformation Transformation to apply: "None", "StringIndexer", "OneHotEncoder", "StandardScaler", etc.
 */
case class FeatureTransformationConfig(
  transformation: String
)

/**
 * Aggregated feature configuration
 * @param aggregation Aggregation method: "sum", "avg", "max", "min", "std"
 * @param transformation Transformation to apply to aggregated features: "None", "StringIndexer", "StandardScaler", etc.
 */
case class AggregatedFeatureConfig(
  aggregation: String,
  transformation: String = "None"
)

/**
 * Feature extraction configuration
 * @param featureType Type of feature extraction: "pca", "feature_selection", "none"
 * @param dxCol Column name for delay classification (e.g., "D2_60")
 * @param delayThresholdMin Delay threshold in minutes for classification
 * @param nDelayed Number of delayed samples to use for training
 * @param nOnTime Number of on-time samples to use for training
 * @param pcaVarianceThreshold Variance threshold for PCA (0.0 to 1.0)
 * @param maxCategoricalCardinality Maximum distinct values for a column to be treated as categorical (default: 50)
 * @param handleInvalid How to handle invalid data in StringIndexer: "skip", "keep", "error" (default: "keep")
 * @param flightSelectedFeatures Map of feature names to their transformation config (for feature_selection type)
 * @param weatherSelectedFeatures Map of feature names to their transformation config (for feature_selection type)
 * @param aggregatedSelectedFeatures Map of weather feature names to their aggregation config (for accumulation features)
 */
case class FeatureExtractionConfig(
   featureType: String,
   dxCol: String,
   delayThresholdMin: Int,
   pcaVarianceThreshold: Double,
   storeJoinData: Boolean,
   storeExplodeJoinData: Boolean,
   weatherOriginDepthHours : Int,
   weatherDestinationDepthHours : Int,
   maxCategoricalCardinality: Int = 50,
   handleInvalid: String = "keep",
   flightSelectedFeatures: Option[Map[String, FeatureTransformationConfig]] = None,
   weatherSelectedFeatures: Option[Map[String, FeatureTransformationConfig]] = None,
   aggregatedSelectedFeatures: Option[Map[String, AggregatedFeatureConfig]] = None
) {
  /**
   * Helper method to check if PCA is enabled
   */
  def isPcaEnabled: Boolean = featureType.toLowerCase == "pca"

  /**
   * Helper method to check if feature selection is enabled
   */
  def isFeatureSelectionEnabled: Boolean = featureType.toLowerCase == "feature_selection"

  /**
   * Helper method to check if weather features are enabled
   */
  def isWeatherEnabled: Boolean = weatherSelectedFeatures.exists(_.nonEmpty)

  /**
   * Get all feature names (flight + weather)
   */
  def getAllFeatureNames: Seq[String] = {
    val flightFeatures = flightSelectedFeatures.map(_.keys.toSeq).getOrElse(Seq.empty)
    val weatherFeatures = weatherSelectedFeatures.map(_.keys.toSeq).getOrElse(Seq.empty)
    flightFeatures ++ weatherFeatures
  }

  /**
   * Get weather features including aggregated features with their transformations
   * This enriches weatherSelectedFeatures with the aggregated features automatically
   */
  def getEnrichedWeatherFeatures: Map[String, FeatureTransformationConfig] = {
    val baseWeatherFeatures = weatherSelectedFeatures.getOrElse(Map.empty)
    val aggregatedFeatures = aggregatedSelectedFeatures.getOrElse(Map.empty)

    // Generate aggregated feature names with their transformations
    val aggregatedFeatureMap = aggregatedFeatures.flatMap { case (varName, aggConfig) =>
      val aggMethod = aggConfig.aggregation.toLowerCase.capitalize
      val transformation = FeatureTransformationConfig(aggConfig.transformation)

      // Generate both origin and destination feature names
      val features = Map(
        s"origin_weather_${varName}_$aggMethod" -> transformation,
        s"destination_weather_${varName}_$aggMethod" -> transformation
      )
      features
    }

    // Merge base weather features with aggregated features
    baseWeatherFeatures ++ aggregatedFeatureMap
  }
}
