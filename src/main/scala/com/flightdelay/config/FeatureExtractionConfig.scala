package com.flightdelay.config

/**
 * Feature transformation configuration
 * @param transformation Transformation to apply: "None", "StringIndexer", "OneHotEncoder", "StandardScaler", etc.
 */
case class FeatureTransformationConfig(
  transformation: String
)

/**
 * Feature extraction configuration
 * @param featureType Type of feature extraction: "pca", "feature_selection", "none"
 * @param pcaVarianceThreshold Variance threshold for PCA (0.0 to 1.0)
 * @param maxCategoricalCardinality Maximum distinct values for a column to be treated as categorical (default: 50)
 * @param flightSelectedFeatures Map of feature names to their transformation config (for feature_selection type)
 * @param weatherSelectedFeatures Map of feature names to their transformation config (for feature_selection type)
 */
case class FeatureExtractionConfig(
   featureType: String,
   pcaVarianceThreshold: Double,
   storeJoinData: Boolean,
   storeExplodeJoinData: Boolean,
   weatherDepthHours : Int,
   maxCategoricalCardinality: Int = 50,
   flightSelectedFeatures: Option[Map[String, FeatureTransformationConfig]] = None,
   weatherSelectedFeatures: Option[Map[String, FeatureTransformationConfig]] = None
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
}
