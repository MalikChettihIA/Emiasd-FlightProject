# PCA and Feature Selection - Complete Guide

## Overview

This guide covers the complete feature extraction and dimensionality reduction pipeline, including:

1. **PCAFeatureExtractor**: Variance-based PCA for dimensionality reduction
2. **HybridFeatureSelector**: Chi-Square selection for categorical and numerical features
3. **FlightFeatureExtractor**: Integrated pipeline with optional PCA (60% variance threshold)

---

## Table of Contents

- [PCAFeatureExtractor](#pcafeatureextractor)
- [HybridFeatureSelector](#hybridfeatureselector)
- [FlightFeatureExtractor](#flightfeatureextractor)
- [Usage Examples](#usage-examples)
- [When to Use PCA vs Feature Selection](#when-to-use-pca-vs-feature-selection)
- [Best Practices](#best-practices)

---

## PCAFeatureExtractor

**Location:** `com.flightdelay.features.pca.PCAFeatureExtractor`

### Purpose

Principal Component Analysis (PCA) for dimensionality reduction with intelligent component selection:
- **Variance-based selection**: Automatically select components to reach target variance (e.g., 60%, 95%)
- **K-based selection**: Manually specify number of components
- **Variance analysis**: Detailed explained variance metrics
- **Elbow detection**: Find optimal number of components

### Key Features

| Feature | Description |
|---------|-------------|
| **Variance Threshold** | Auto-select components for target cumulative variance |
| **Reusable Models** | Fit once, transform train/test/production |
| **Variance Analysis** | Detailed metrics and scree plot data |
| **Elbow Detection** | Find point of diminishing returns |
| **Model Persistence** | Save/load PCA models |

### Constructor Parameters

```scala
class PCAFeatureExtractor(
  inputCol: String = "features",           // Input features column
  outputCol: String = "pcaFeatures",       // Output PCA features column
  k: Int = 10,                             // Number of components (if no threshold)
  varianceThreshold: Option[Double] = None // Target variance (0.0 to 1.0)
)
```

### Usage Examples

#### 1. Variance-Based Selection (Recommended)

```scala
import com.flightdelay.features.pca.PCAFeatureExtractor

// Select components to reach 95% variance
val pca = PCAFeatureExtractor.varianceBased(
  threshold = 0.95,
  inputCol = "features",
  outputCol = "pcaFeatures"
)

// Fit and transform
val (model, trainTransformed, analysis) = pca.fitTransform(trainDF)

// Print variance report
pca.printVarianceReport(analysis)

// Transform test data
val testTransformed = pca.transform(model, testDF)

// Save model
model.write.overwrite().save("models/pca_model")
```

**Output:**
```
================================================================================
PCA Variance Analysis Report
================================================================================
Original Dimensions: 43
Selected Components: 15
Total Variance Explained: 95.23%

Variance by Component:
--------------------------------------------------------------------------------
Component    Individual      Cumulative      Cumulative %
--------------------------------------------------------------------------------
PC-1         0.121953        0.121953        12.20%
PC-2         0.110171        0.232124        23.21%
PC-3         0.078745        0.310869        31.09%
...
```

#### 2. Fixed K Components

```scala
// Use exactly 10 components
val pca = PCAFeatureExtractor.fixedK(
  k = 10,
  inputCol = "features",
  outputCol = "pcaFeatures"
)

val (model, data, analysis) = pca.fitTransform(trainDF)
println(s"Variance explained: ${analysis.totalVarianceExplained * 100}%")
```

#### 3. Builder Pattern

```scala
val pca = PCAFeatureExtractor.builder()
  .withInputCol("scaledFeatures")
  .withOutputCol("pcaFeatures")
  .withVarianceThreshold(0.99) // 99% variance
  .build()

val (model, data, analysis) = pca.fitTransform(trainDF)
```

#### 4. Explore Variance (Data Analysis)

```scala
// Explore variance without transformation
val analysis = PCAFeatureExtractor.exploreVariance(
  data = trainDF,
  inputCol = "features",
  maxK = 50
)

println(s"For 60% variance: ${analysis.getMinComponentsForVariance(0.60)} components")
println(s"For 90% variance: ${analysis.getMinComponentsForVariance(0.90)} components")
println(s"For 95% variance: ${analysis.getMinComponentsForVariance(0.95)} components")

// Find elbow point
val pca = new PCAFeatureExtractor()
val elbowK = pca.findElbowPoint(analysis)
println(s"Optimal K by elbow method: $elbowK")
```

#### 5. Generate Scree Plot Data

```scala
import spark.implicits._

val pca = new PCAFeatureExtractor(k = 20)
val (model, _, analysis) = pca.fitTransform(trainDF)

// Get data for visualization
val screePlotDF = pca.getScreePlotData(analysis)
screePlotDF.show()

/*
+----------+------------------+--------------------+
| component|explained_variance|cumulative_variance |
+----------+------------------+--------------------+
|         1|         0.1219526|           0.1219526|
|         2|         0.1101713|           0.2321239|
|         3|         0.0787446|           0.3108685|
...
*/
```

### VarianceAnalysis Class

```scala
case class VarianceAnalysis(
  numComponents: Int,
  originalDimension: Int,
  explainedVariance: Array[Double],
  cumulativeVariance: Array[Double],
  totalVarianceExplained: Double,
  componentIndices: Array[Int]
)

// Helper methods
analysis.getVarianceForTopK(k = 10)                 // Variance for top 10 components
analysis.getMinComponentsForVariance(threshold = 0.90) // Min components for 90% variance
```

---

## HybridFeatureSelector

**Location:** `com.flightdelay.features.selection.HybridFeatureSelector`

### Purpose

Hybrid feature selection that applies different strategies to numerical and categorical features:
- **Chi-Square** for categorical (one-hot encoded) features
- **ANOVA F-test** for numerical features (optional)
- **VectorSlicer** to separate and reassemble features

This follows the pattern from the PySpark notebook: separate numerical/categorical → select → reassemble.

### Constructor Parameters

```scala
class HybridFeatureSelector(
  numericalIndices: Array[Int],              // Indices of numerical features
  categoricalIndices: Array[Int],            // Indices of categorical features
  labelCol: String,                          // Label for supervised selection
  inputCol: String = "features",             // Input features column
  outputCol: String = "selectedFeatures",    // Output selected features column
  categoricalSelectorType: String = "fpr",   // "fpr", "fdr", "fwe", "numTopFeatures", "percentile"
  categoricalThreshold: Double = 0.05,       // Threshold for categorical selector
  numericalSelectorType: Option[String] = None,    // Optional selector for numerical
  numericalThreshold: Option[Double] = None        // Threshold for numerical selector
)
```

### Selection Types

| Type | Description | Threshold |
|------|-------------|-----------|
| **fpr** | False Positive Rate | 0.0 to 1.0 (e.g., 0.05) |
| **fdr** | False Discovery Rate | 0.0 to 1.0 |
| **fwe** | Family-Wise Error | 0.0 to 1.0 |
| **numTopFeatures** | Top K features | Integer K |
| **percentile** | Top X percentile | 0.0 to 1.0 |

### Usage Examples

#### 1. Chi-Square FPR (Like the Notebook)

```scala
import com.flightdelay.features.selection.HybridFeatureSelector

// Define feature indices (from your pipeline)
val numericalIndices = (0 until 13).toArray    // First 13 are numerical
val categoricalIndices = (13 until 43).toArray // Next 30 are categorical (OHE)

// Chi-Square with FPR = 0.05
val selector = HybridFeatureSelector.chiSquareFPR(
  numericalIndices = numericalIndices,
  categoricalIndices = categoricalIndices,
  labelCol = "has_purchased",
  fpr = 0.05,
  inputCol = "features"
)

val (model, selected, info) = selector.fitTransform(trainDF)

println(info)
/*
SelectionInfo(
  Original: 43 features
  Selected: 27 features (13 numerical + 14 categorical)
  Reduction: 37%
)
*/

// Transform test data
val testSelected = selector.transform(model, testDF)
```

#### 2. Top-K Selection

```scala
// Select top 20 categorical features
val selector = HybridFeatureSelector.topK(
  numericalIndices = numericalIndices,
  categoricalIndices = categoricalIndices,
  labelCol = "label_is_delayed_15min",
  k = 20,
  inputCol = "features"
)

val (model, data, info) = selector.fitTransform(trainDF)
println(s"Selected ${info.totalSelectedFeatures} features")
```

#### 3. Builder Pattern with Numerical Selection

```scala
val selector = HybridFeatureSelector.builder()
  .withNumericalIndices(numericalIndices)
  .withCategoricalIndices(categoricalIndices)
  .withLabelCol("label")
  .withInputCol("features")
  .withOutputCol("selectedFeatures")
  .withCategoricalSelector("fpr", 0.05)
  .withNumericalSelector("numTopFeatures", 10.0) // Keep top 10 numerical
  .build()

val (model, data, info) = selector.fitTransform(trainDF)
```

#### 4. Get Selected Feature Indices

```scala
val selectedIndices = selector.getSelectedFeatures(model)
println(s"Selected feature indices: ${selectedIndices.mkString(", ")}")
```

### SelectionInfo Class

```scala
case class SelectionInfo(
  originalFeatures: Int,
  selectedNumerical: Int,
  selectedCategorical: Int,
  totalSelectedFeatures: Int,
  reductionPercentage: Int,
  categoricalIndices: Array[Int]
)
```

---

## FlightFeatureExtractor

**Location:** `com.flightdelay.features.FlightFeatureExtractor`

### Purpose

**Main entry point** for the feature extraction pipeline with integrated PCA support:
1. Automatic column type detection (text vs numeric)
2. Feature vectorization and indexing
3. **Optional PCA** with 60% variance threshold (default)

### Updated API

#### Method 1: Without PCA (Original Behavior)

```scala
import com.flightdelay.features.FeatureExtractor

// Extract features without PCA
val data = FeatureExtractor.extract(
  df,
  target = "label_is_delayed_15min"
)

// Returns: DataFrame with columns [features: Vector, label: Double]
```

#### Method 2: With PCA (NEW)

```scala
// Extract features with PCA (60% variance by default)
val (data, pcaModel, analysis) = FlightFeatureExtractor.extractWithPCA(
  df,
  target = "label_is_delayed_15min",
  varianceThreshold = 0.60  // 60% minimum variance
)

// Returns: (DataFrame, PCAModel, VarianceAnalysis)
```

**Output:**
```
[FlightFeatureExtractor] Starting feature extraction for target: label_is_delayed_15min
[FlightFeatureExtractor] Dropped 10 unused label columns
[FlightFeatureExtractor] Detected 15 text columns and 45 numeric columns
[FlightFeatureExtractor] Base feature pipeline completed
Fitting PCA with k=18 components...

[FlightFeatureExtractor] PCA Summary:
  - Original features: 60
  - PCA components: 18
  - Variance explained: 60%
  - Dimensionality reduction: 70%
```

#### Method 3: Explore Variance Thresholds

```scala
// Explore different variance thresholds before choosing
val analysis = FlightFeatureExtractor.explorePCAVariance(
  df,
  target = "label_is_delayed_15min",
  maxK = 50
)

/*
PCA Variance Exploration Results:
  - For 60% variance: 18 components
  - For 75% variance: 25 components
  - For 90% variance: 35 components
  - For 95% variance: 42 components
*/

// Then use the optimal threshold
val (data, model, _) = FlightFeatureExtractor.extractWithPCA(
  df,
  target = "label_is_delayed_15min",
  varianceThreshold = 0.75  // Choose 75% based on exploration
)
```

### Configuration

```scala
object FlightFeatureExtractor {
  private val defaultVarianceThreshold = 0.60  // 60% minimum variance
  private val maxCat = 32                      // Max categories for VectorIndexer
  private val handleInvalid = "skip"           // Invalid value handling
}
```

---

## Usage Examples

### Example 1: Complete Pipeline with PCA

```scala
import com.flightdelay.features.FeatureExtractor
import com.flightdelay.data.preprocessing.FlightPreprocessingPipeline
import org.apache.spark.ml.classification.RandomForestClassifier

// Step 1: Preprocess data (feature engineering)
val processedData = flights.FlightPreprocessingPipeline.execute(rawFlightData)

// Step 2: Extract features with PCA (60% variance)
val (trainFeatures, pcaModel, analysis) = FeatureExtractor.extractWithPCA(
  processedData,
  target = "label_is_delayed_15min",
  varianceThreshold = 0.60
)

println(s"Reduced from ${analysis.originalDimension} to ${analysis.numComponents} features")

// Step 3: Train model
val rf = new RandomForestClassifier()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setNumTrees(100)

val model = rf.fit(trainFeatures)

// Step 4: Save models
pcaModel.write.overwrite().save("models/pca_60pct")
model.write.overwrite().save("models/rf_classifier")
```

### Example 2: Compare PCA Thresholds

```scala
// Compare different variance thresholds
val thresholds = Array(0.60, 0.75, 0.90, 0.95)

thresholds.foreach { threshold =>
  val (data, model, analysis) = FlightFeatureExtractor.extractWithPCA(
    processedData,
    target = "label_is_delayed_15min",
    varianceThreshold = threshold
  )

  println(s"Threshold ${threshold * 100}%: ${analysis.numComponents} components")
}

/*
Threshold 60.0%: 18 components
Threshold 75.0%: 25 components
Threshold 90.0%: 35 components
Threshold 95.0%: 42 components
*/
```

### Example 3: Hybrid Selection Without PCA

```scala
import com.flightdelay.features.selection.HybridFeatureSelector

// Step 1: Extract base features (no PCA)
val baseFeatures = FlightFeatureExtractor.extract(
  processedData,
  target = "label_is_delayed_15min"
)

// Step 2: Define feature indices
val numCols = 13
val catCols = 30
val numericalIndices = (0 until numCols).toArray
val categoricalIndices = (numCols until numCols + catCols).toArray

// Step 3: Apply hybrid selection
val selector = HybridFeatureSelector.chiSquareFPR(
  numericalIndices,
  categoricalIndices,
  labelCol = "label",
  fpr = 0.05
)

val (selectionModel, selectedData, info) = selector.fitTransform(baseFeatures)
println(s"Selected ${info.totalSelectedFeatures} features from ${info.originalFeatures}")
```

### Example 4: Production Pipeline

```scala
// Training phase
val (trainData, pcaModel, analysis) = FlightFeatureExtractor.extractWithPCA(
  trainingDF,
  target = "label_is_delayed_15min",
  varianceThreshold = 0.60
)

val rfModel = new RandomForestClassifier().fit(trainData)

// Save models
pcaModel.write.overwrite().save("s3://bucket/models/pca_v1")
rfModel.write.overwrite().save("s3://bucket/models/rf_v1")

// Production phase (different system/time)
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.classification.RandomForestClassificationModel

val loadedPCA = PCAModel.load("s3://bucket/models/pca_v1")
val loadedRF = RandomForestClassificationModel.load("s3://bucket/models/rf_v1")

// Apply to new data
val pca = new PCAFeatureExtractor()
val newDataPCA = pca.transform(loadedPCA, newProductionData)
val predictions = loadedRF.transform(newDataPCA)
```

---

## When to Use PCA vs Feature Selection

### Use PCA When:

✅ **High dimensionality** (100+ features) - Need aggressive reduction
✅ **Multicollinearity** - Features are highly correlated
✅ **Computational efficiency** - Need faster training/inference
✅ **Noise reduction** - Want to filter out noise in data
✅ **Visualization** - Reduce to 2-3 dimensions for plotting

**Trade-off:** Lose interpretability (components are linear combinations)

### Use Feature Selection When:

✅ **Interpretability** - Need to explain which original features matter
✅ **Domain knowledge** - Want to validate feature importance
✅ **Moderate dimensionality** (10-100 features)
✅ **Categorical features** - Chi-Square works well with OHE
✅ **Regulatory requirements** - Must explain model decisions

**Trade-off:** May keep correlated features, less aggressive reduction

### Recommended Workflow

```
1. Start with Feature Engineering (60+ features)
2. Try both approaches:

   a) PCA (60%, 75%, 90% variance)
      → Evaluate model performance

   b) Feature Selection (FPR=0.05)
      → Evaluate model performance

3. Compare:
   - Accuracy/F1 Score
   - Training time
   - Interpretability needs

4. Choose based on:
   - If accuracy similar → Feature Selection (interpretable)
   - If PCA much faster → PCA for production
   - If PCA much better → PCA (can explain via loadings)
```

---

## Best Practices

### 1. Always Fit on Training Data Only

```scala
// ✅ CORRECT: Fit on train, transform on test
val (model, trainData, _) = FlightFeatureExtractor.extractWithPCA(trainDF, ...)
val testData = pca.transform(model, testDF)

// ❌ WRONG: Fitting on test data (data leakage!)
val testData = FlightFeatureExtractor.extractWithPCA(testDF, ...)._1
```

### 2. Choose Variance Threshold Wisely

```scala
// For flight delays (interpretability important)
varianceThreshold = 0.60 to 0.75  // Good balance

// For image/text data (high dimensional)
varianceThreshold = 0.95 to 0.99  // Keep more variance

// Explore first!
val analysis = FlightFeatureExtractor.explorePCAVariance(df, target)
```

### 3. Monitor Dimensionality Reduction

```scala
val (data, model, analysis) = FlightFeatureExtractor.extractWithPCA(...)

// Check reduction ratio
val reductionPct = (1 - analysis.numComponents.toDouble / analysis.originalDimension) * 100
println(s"Reduced by ${reductionPct.round}%")

// If < 30% reduction → Consider feature selection instead
if (reductionPct < 30) {
  println("PCA not effective, try feature selection")
}
```

### 4. Save Models for Production

```scala
// Save PCA model with metadata
pcaModel.write.overwrite().save("models/pca_60pct_v1")

// Save metadata separately
val metadata = Map(
  "variance_threshold" -> "0.60",
  "num_components" -> analysis.numComponents.toString,
  "original_features" -> analysis.originalDimension.toString,
  "variance_explained" -> analysis.totalVarianceExplained.toString,
  "created_date" -> java.time.LocalDate.now.toString
)

// Write metadata JSON
```

### 5. Handle Edge Cases

```scala
// Check if variance threshold is achievable
val analysis = PCAFeatureExtractor.exploreVariance(df, "features", maxK = 100)

if (analysis.getMinComponentsForVariance(0.95) == -1) {
  println("Cannot reach 95% variance with available components")
  // Fall back to lower threshold
  varianceThreshold = 0.90
}
```

---

## Troubleshooting

### Issue 1: PCA selects too many components

```scala
// Problem: 95% variance needs 45/50 components (not much reduction)
// Solution: Lower threshold or use feature selection

val analysis = FlightFeatureExtractor.explorePCAVariance(df, target)
println(s"For 60%: ${analysis.getMinComponentsForVariance(0.60)} components")
println(s"For 75%: ${analysis.getMinComponentsForVariance(0.75)} components")

// Choose based on acceptable reduction
varianceThreshold = 0.60  // More aggressive
```

### Issue 2: Features need scaling before PCA

```scala
// PCA is sensitive to feature scale
// Use EnhancedFlightFeaturePipeline with scaling first

import com.flightdelay.features.pipelines.EnhancedFlightFeaturePipeline

val pipeline = EnhancedFlightFeaturePipeline.builder()
  .withTextCols(textCols)
  .withNumericCols(numericCols)
  .withTarget(target)
  .withScaler("standard")  // Scale before PCA
  .build()

val (model, scaledData) = pipeline.fitTransform(trainDF)

// Then apply PCA
val pca = PCAFeatureExtractor.varianceBased(0.60, inputCol = "features")
val (pcaModel, pcaData, analysis) = pca.fitTransform(scaledData)
```

### Issue 3: Model loading fails

```scala
// Ensure Spark versions match
// Save: Spark 3.5.3 → Load: Spark 3.5.3 ✅

import org.apache.hadoop.fs.{FileSystem, Path}

val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
if (!fs.exists(new Path("models/pca_model"))) {
  println("Model path does not exist")
}
```

---

## Performance Considerations

### Memory Usage

- **PCA Fitting**: O(n × d²) where n = samples, d = features
- **PCA Transform**: O(n × k) where k = components
- **Recommendation**: For large d (>1000), consider sampling for fitting

### Speed Optimization

```scala
// Cache data if using multiple times
val baseFeatures = FlightFeatureExtractor.extract(df, target)
baseFeatures.cache()

// Try different PCA thresholds
val pca60 = PCAFeatureExtractor.varianceBased(0.60).fitTransform(baseFeatures)
val pca75 = PCAFeatureExtractor.varianceBased(0.75).fitTransform(baseFeatures)
val pca90 = PCAFeatureExtractor.varianceBased(0.90).fitTransform(baseFeatures)

baseFeatures.unpersist()
```

---

## Summary

| Class | Purpose | When to Use |
|-------|---------|-------------|
| **PCAFeatureExtractor** | Variance-based dimensionality reduction | High dimensions (100+ features), need speed, can sacrifice interpretability |
| **HybridFeatureSelector** | Separate selection for numerical/categorical | Need interpretability, moderate dimensions, categorical features |
| **FlightFeatureExtractor** | Main entry point with optional PCA | Default pipeline, configurable reduction strategy |

**Default Recommendation for Flight Delays:**
- Start with `FlightFeatureExtractor.extractWithPCA(df, target, varianceThreshold = 0.60)`
- Evaluate model performance
- Compare with feature selection if interpretability is critical
- Use PCA for production if speed/dimensionality is priority

---

## Related Documentation

- [EnhancedFlightFeaturePipeline](./EnhancedFlightFeaturePipeline_explanation.md)
- [BasicFlightFeaturePipeline](./BasicFlightFeaturePipeline_explanation.md)
- [Spark ML PCA Documentation](https://spark.apache.org/docs/latest/ml-features.html#pca)
- [Spark ML Feature Selection](https://spark.apache.org/docs/latest/ml-features.html#feature-selectors)
