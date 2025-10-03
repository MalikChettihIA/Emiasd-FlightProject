package com.flightdelay.config

/**
 * Model configuration for training
 * @param name Model name (e.g., "flight_delay_rf")
 * @param target Target column name (e.g., "label_is_delayed_15min")
 * @param modelType Type of ML model: "randomforest", "gbt", "logisticregression", "decisiontree"
 * @param trainRatio Ratio of data used for training (0.0 to 1.0)
 * @param numTrees Number of trees (for RandomForest and GBT)
 * @param maxDepth Maximum depth of trees
 * @param maxBins Maximum number of bins for discretizing continuous features
 * @param minInstancesPerNode Minimum instances per tree node
 * @param seed Random seed for reproducibility
 */
case class ModelConfig(
   name: String,
   target: String,
   modelType: String,
   trainRatio: Double,
   numTrees: Int,
   maxDepth: Int,
   maxBins: Int,
   minInstancesPerNode: Int,
   seed: Long
)
