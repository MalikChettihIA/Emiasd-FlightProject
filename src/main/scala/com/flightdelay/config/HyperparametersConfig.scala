package com.flightdelay.config

/**
 * Hyperparameters configuration for model training
 * Arrays allow grid search to test multiple values
 *
 * @param numTrees Number of trees (for RandomForest and GBT) - can be array for grid search
 * @param maxDepth Maximum depth of trees - can be array for grid search
 * @param maxBins Maximum number of bins for discretizing continuous features
 * @param minInstancesPerNode Minimum instances per tree node
 * @param subsamplingRate Fraction of data to use for training each tree (0.0 to 1.0)
 * @param featureSubsetStrategy Number of features to consider for splits ("auto", "all", "sqrt", "log2", "onethird")
 * @param impurity Impurity measure ("gini" or "entropy")
 */
case class HyperparametersConfig(
  numTrees: Seq[Int],
  maxDepth: Seq[Int],
  maxBins: Int,
  minInstancesPerNode: Int,
  subsamplingRate: Double,
  featureSubsetStrategy: String,
  impurity: String
)
