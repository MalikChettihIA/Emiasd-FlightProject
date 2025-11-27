package com.flightdelay.config

/**
 * Hyperparameters configuration for model training
 * All parameters support arrays for grid search
 *
 * RandomForest:
 * @param numTrees Number of trees - can be array for grid search
 * @param maxDepth Maximum depth of trees - can be array for grid search
 * @param maxBins Maximum number of bins for discretizing continuous features - can be array for grid search
 * @param minInstancesPerNode Minimum instances per tree node - can be array for grid search
 * @param subsamplingRate Fraction of data to use for training each tree (0.0 to 1.0) - can be array for grid search
 * @param featureSubsetStrategy Number of features to consider for splits ("auto", "all", "sqrt", "log2", "onethird") - can be array for grid search
 * @param impurity Impurity measure ("gini" or "entropy")
 *
 * Gradient Boosted Trees (GBT):
 * @param maxIter Maximum number of iterations/trees (for GBT) - can be array for grid search
 * @param stepSize Learning rate (for GBT, typically 0.01-0.1) - can be array for grid search
 * Note: GBT also uses maxDepth, maxBins, minInstancesPerNode, subsamplingRate
 *
 * Logistic Regression:
 * @param maxIter Maximum number of iterations (for LR) - can be array for grid search
 * @param regParam Regularization parameter (for LR) - can be array for grid search
 * @param elasticNetParam ElasticNet mixing parameter (for LR) - can be array for grid search
 *
 * XGBoost:
 * @param maxIter Maximum number of boosting rounds/trees (for XGBoost) - can be array for grid search
 * @param stepSize Learning rate aka eta (for XGBoost, typically 0.01-0.3) - can be array for grid search
 * @param alpha L1 regularization term (for XGBoost) - can be array for grid search
 * @param lambda L2 regularization term (for XGBoost) - can be array for grid search
 * @param gamma Minimum loss reduction required for split (for XGBoost) - can be array for grid search
 * @param colsampleBytree Subsample ratio of columns for each tree (for XGBoost) - can be array for grid search
 * Note: XGBoost also uses maxDepth, maxBins, minInstancesPerNode, subsamplingRate
 */
case class HyperparametersConfig(
  // Tree-based model params (optional)
  numTrees: Option[Seq[Int]] = None,
  maxDepth: Option[Seq[Int]] = None,
  maxBins: Option[Seq[Int]] = None,
  minInstancesPerNode: Option[Seq[Int]] = None,
  subsamplingRate: Option[Seq[Double]] = None,
  featureSubsetStrategy: Option[Seq[String]] = None,
  impurity: Option[String] = None,
  // GBT/XGBoost shared params
  stepSize: Option[Seq[Double]] = None,  // Learning rate (for GBT and XGBoost)
  maxIter: Option[Seq[Int]] = None,      // Max iterations (for LR, GBT, XGBoost)
  // Logistic Regression specific params (optional)
  regParam: Option[Seq[Double]] = None,
  elasticNetParam: Option[Seq[Double]] = None,
  // XGBoost specific params (optional)
  alpha: Option[Seq[Double]] = None,           // L1 regularization term
  lambda: Option[Seq[Double]] = None,          // L2 regularization term
  gamma: Option[Seq[Double]] = None,           // Minimum loss reduction for split
  colsampleBytree: Option[Seq[Double]] = None  // Subsample ratio of columns
)
