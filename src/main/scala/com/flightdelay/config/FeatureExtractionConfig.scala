package com.flightdelay.config

/**
 * Feature extraction configuration
 * @param featureType Type of feature extraction: "pca", "feature_selection", "none"
 * @param pcaVarianceThreshold Variance threshold for PCA (0.0 to 1.0)
 * @param maxCategoricalCardinality Maximum distinct values for a column to be treated as categorical (default: 50)
 * @param flightSelectedFeatures List of feature names to select (for feature_selection type)
 * @param weatherSelectedFeatures List of feature names to select (for feature_selection type)
 */
case class FeatureExtractionConfig(
   featureType: String,
   pcaVarianceThreshold: Double,
   storeJoinData: Boolean,
   storeExplodeJoinData: Boolean,
   weatherDepthHours : Int,
   maxCategoricalCardinality: Int = 50,
   flightSelectedFeatures: Option[Seq[String]] = None,
   weatherSelectedFeatures: Option[Seq[String]] = None
) {
  /**
   * Helper method to check if PCA is enabled
   */
  def isPcaEnabled: Boolean = featureType.toLowerCase == "pca"

  /**
   * Helper method to check if feature selection is enabled
   */
  def isFeatureSelectionEnabled: Boolean = featureType.toLowerCase == "feature_selection"
}
