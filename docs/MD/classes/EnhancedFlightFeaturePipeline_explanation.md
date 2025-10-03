# EnhancedFlightFeaturePipeline - Complete Guide

## Overview

`EnhancedFlightFeaturePipeline` is an advanced feature preprocessing pipeline that addresses all limitations of `BasicFlightFeaturePipeline`. It provides production-ready capabilities for machine learning workflows including feature scaling, selection, extensibility, and model reusability.

---

## Key Improvements Over BasicFlightFeaturePipeline

| Feature | BasicFlightFeaturePipeline | EnhancedFlightFeaturePipeline |
|---------|---------------------------|------------------------------|
| **Feature Scaling** | ❌ None | ✅ Standard, MinMax, Robust |
| **Feature Selection** | ❌ None | ✅ Chi-Squared, ANOVA |
| **Custom Stages** | ❌ Fixed pipeline | ✅ Injectable custom transformers |
| **Model Reusability** | ❌ Returns DataFrame only | ✅ Returns PipelineModel for train/test |
| **Builder Pattern** | ❌ Constructor only | ✅ Fluent builder API |
| **Model Persistence** | ❌ No | ✅ Save/load fitted models |

---

## Architecture

### Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Raw DataFrame                                │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │   StringIndexer      │
                  │  (Categorical → ID)  │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │  VectorAssembler     │
                  │  (Features → Vector) │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │   VectorIndexer      │
                  │ (Mark Categorical)   │
                  └──────────┬───────────┘
                             │
                             ▼
         ┌───────────────────────────────────────┐
         │        Optional: Feature Selector      │
         │  Chi-Squared / ANOVA / Custom         │
         └───────────────────┬───────────────────┘
                             │
                             ▼
         ┌───────────────────────────────────────┐
         │          Optional: Scaler             │
         │  Standard / MinMax / Robust           │
         └───────────────────┬───────────────────┘
                             │
                             ▼
         ┌───────────────────────────────────────┐
         │       Optional: Custom Stages         │
         │  (User-defined transformers)          │
         └───────────────────┬───────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────────┐
│                    ML-Ready DataFrame                              │
│                features: Vector | label: Double                    │
└────────────────────────────────────────────────────────────────────┘
```

---

## Constructor Parameters

```scala
class EnhancedFlightFeaturePipeline(
  textCols: Array[String],              // Categorical columns
  numericCols: Array[String],           // Numeric columns
  target: String,                       // Target column
  maxCat: Int = 32,                     // Max categories for VectorIndexer
  handleInvalid: String = "skip",       // "skip", "error", or "keep"
  scalerType: Option[String] = None,    // "standard", "minmax", "robust"
  featureSelector: Option[(String, Int)] = None,  // (type, numFeatures)
  customStages: Array[PipelineStage] = Array.empty
)
```

---

## Feature 1: Feature Scaling

### Why Scaling Matters

Different ML algorithms have different scaling requirements:

| Algorithm | Needs Scaling? | Reason |
|-----------|---------------|--------|
| **Tree-based** (RF, DT) | ❌ No | Splits are threshold-based, scale-invariant |
| **Logistic Regression** | ✅ Yes | Gradient descent converges faster |
| **SVM** | ✅ Yes | Distance-based, sensitive to feature magnitude |
| **Neural Networks** | ✅ Yes | Gradient stability and convergence |
| **k-NN** | ✅ Yes | Distance-based algorithm |

### Available Scalers

#### 1. StandardScaler (Z-score normalization)
```scala
// Mean = 0, StdDev = 1
// Formula: (x - μ) / σ

val pipeline = new EnhancedFlightFeaturePipeline(
  textCols = textCols,
  numericCols = numericCols,
  target = "label_is_delayed_15min",
  scalerType = Some("standard")
)
```

**Use when:** Features follow normal distribution, need zero mean

#### 2. MinMaxScaler (Range normalization)
```scala
// Scales to [0, 1] range
// Formula: (x - min) / (max - min)

val pipeline = new EnhancedFlightFeaturePipeline(
  textCols = textCols,
  numericCols = numericCols,
  target = "label_is_delayed_15min",
  scalerType = Some("minmax")
)
```

**Use when:** Need bounded range, features have outliers

#### 3. RobustScaler (Outlier-resistant)
```scala
// Uses median and IQR instead of mean/std
// Formula: (x - median) / IQR

val pipeline = new EnhancedFlightFeaturePipeline(
  textCols = textCols,
  numericCols = numericCols,
  target = "label_is_delayed_15min",
  scalerType = Some("robust")
)
```

**Use when:** Dataset has outliers, need robust statistics

---

## Feature 2: Feature Selection

### Why Feature Selection?

- **Reduce overfitting** by removing irrelevant features
- **Improve training speed** with fewer dimensions
- **Better interpretability** with focused feature set
- **Reduce noise** from uninformative features

### Available Selectors

#### 1. Chi-Squared Test
```scala
val pipeline = new EnhancedFlightFeaturePipeline(
  textCols = textCols,
  numericCols = numericCols,
  target = "label_is_delayed_15min",
  featureSelector = Some(("chi2", 30))  // Select top 30 features
)
```

**Best for:**
- Categorical features vs categorical target
- Non-negative features
- Fast computation

#### 2. ANOVA F-Test
```scala
val pipeline = new EnhancedFlightFeaturePipeline(
  textCols = textCols,
  numericCols = numericCols,
  target = "label_is_delayed_15min",
  featureSelector = Some(("anova", 40))  // Select top 40 features
)
```

**Best for:**
- Continuous features vs categorical target
- Normally distributed features
- Linear relationships

---

## Feature 3: Model Reusability

### The Problem with BasicFlightFeaturePipeline

```scala
// ❌ Cannot reuse transformations
val basicPipeline = new BasicFlightFeaturePipeline(...)
val trainData = basicPipeline.fit(trainDF)  // Returns DataFrame
val testData = basicPipeline.fit(testDF)    // WRONG! Re-fits on test data
```

### Solution: Return PipelineModel

```scala
// ✅ Fit once, transform many times
val pipeline = new EnhancedFlightFeaturePipeline(...)

// Fit on training data
val (model, trainTransformed) = pipeline.fitTransform(trainDF)

// Apply same transformations to test data
val testTransformed = pipeline.transform(model, testDF)

// Apply to validation data
val valTransformed = pipeline.transform(model, valDF)

// Save model for production
model.write.overwrite().save("models/flight_pipeline_v1")

// Load in production
val prodModel = PipelineModel.load("models/flight_pipeline_v1")
val newPredictions = pipeline.transform(prodModel, newDataDF)
```

---

## Feature 4: Custom Stage Injection

### Adding Custom Transformers

```scala
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

// Example: Custom PCA transformer
class CustomPCATransformer(k: Int) extends Transformer {
  override def transform(dataset: Dataset[_]): DataFrame = {
    // Custom transformation logic
    dataset.toDF()
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
  override val uid: String = "custom_pca"
}

// Inject into pipeline
val customPCA = new CustomPCATransformer(k = 20)

val pipeline = new EnhancedFlightFeaturePipeline(
  textCols = textCols,
  numericCols = numericCols,
  target = "label_is_delayed_15min",
  scalerType = Some("standard"),
  customStages = Array(customPCA)
)
```

---

## Feature 5: Builder Pattern API

### Fluent Construction

```scala
import com.flightdelay.features.pipelines.EnhancedFlightFeaturePipeline

val pipeline = EnhancedFlightFeaturePipeline.builder()
  .withTextCols(Array("carrier", "route_id", "departure_quarter_name"))
  .withNumericCols(Array("departure_hour", "distance_score", "feature_flights_on_route"))
  .withTarget("label_is_delayed_15min")
  .withMaxCategories(50)
  .withHandleInvalid("skip")
  .withScaler("robust")
  .withFeatureSelection("chi2", 35)
  .withCustomStage(myTransformer)
  .build()

// Print configuration
pipeline.printSummary()
```

**Output:**
```
================================================================================
Enhanced Flight Feature Pipeline Configuration
================================================================================
Text columns: carrier, route_id, departure_quarter_name
Numeric columns: departure_hour, distance_score, feature_flights_on_route
Target: label_is_delayed_15min
Max categories: 50
Handle invalid: skip
Scaler: robust
Feature selector: chi2 (top 35)
Custom stages: 1
Total stages: 6
================================================================================
```

---

## Complete Usage Examples

### Example 1: Simple Pipeline with Scaling

```scala
import com.flightdelay.features.pipelines.EnhancedFlightFeaturePipeline

// Define columns
val textCols = Array("carrier", "route_id", "departure_time_period")
val numericCols = Array(
  "departure_hour", "distance_score", "feature_flights_on_route",
  "feature_origin_airport_traffic", "feature_is_weekend"
)

// Create pipeline with standard scaling
val pipeline = new EnhancedFlightFeaturePipeline(
  textCols = textCols,
  numericCols = numericCols,
  target = "label_is_delayed_15min",
  scalerType = Some("standard")
)

// Fit and transform
val (model, trainData) = pipeline.fitTransform(trainDF)
val testData = pipeline.transform(model, testDF)

// Train model
import org.apache.spark.ml.classification.LogisticRegression

val lr = new LogisticRegression()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setMaxIter(100)

val lrModel = lr.fit(trainData)
val predictions = lrModel.transform(testData)
```

---

### Example 2: Pipeline with Feature Selection

```scala
// For high-dimensional data (60+ features), select top 30
val pipeline = new EnhancedFlightFeaturePipeline(
  textCols = textCols,
  numericCols = numericCols,
  target = "label_is_delayed_60min",
  featureSelector = Some(("chi2", 30)),
  scalerType = Some("minmax")
)

val (model, trainData) = pipeline.fitTransform(trainDF)

// Check selected features
val selectedFeatures = model.stages
  .find(_.isInstanceOf[ChiSqSelectorModel])
  .map(_.asInstanceOf[ChiSqSelectorModel].selectedFeatures)

println(s"Selected feature indices: ${selectedFeatures.mkString(", ")}")
```

---

### Example 3: Cross-Validation with Reusable Model

```scala
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.classification.RandomForestClassifier

// Create pipeline (no scaling needed for Random Forest)
val pipeline = new EnhancedFlightFeaturePipeline(
  textCols = textCols,
  numericCols = numericCols,
  target = "label_is_delayed_15min",
  featureSelector = Some(("chi2", 40))
)

// Fit pipeline once
val (featureModel, trainFeatures) = pipeline.fitTransform(trainDF)

// Setup cross-validation
val rf = new RandomForestClassifier()
  .setFeaturesCol("features")
  .setLabelCol("label")

val paramGrid = new ParamGridBuilder()
  .addGrid(rf.numTrees, Array(50, 100, 200))
  .addGrid(rf.maxDepth, Array(5, 10, 15))
  .build()

val cv = new CrossValidator()
  .setEstimator(rf)
  .setEvaluator(new BinaryClassificationEvaluator())
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(5)

// Train with cross-validation
val cvModel = cv.fit(trainFeatures)

// Transform test data with same feature pipeline
val testFeatures = pipeline.transform(featureModel, testDF)
val predictions = cvModel.transform(testFeatures)
```

---

### Example 4: Production Deployment

```scala
// Training phase
val pipeline = EnhancedFlightFeaturePipeline.builder()
  .withTextCols(textCols)
  .withNumericCols(numericCols)
  .withTarget("label_is_delayed_15min")
  .withScaler("robust")
  .withFeatureSelection("chi2", 30)
  .build()

val (featureModel, trainData) = pipeline.fitTransform(trainDF)

// Train final model
val rf = new RandomForestClassifier().fit(trainData)

// Save both feature pipeline and ML model
featureModel.write.overwrite().save("models/feature_pipeline")
rf.write.overwrite().save("models/rf_classifier")

// Production phase (different system/time)
import org.apache.spark.ml.classification.RandomForestClassificationModel

val loadedFeaturePipeline = PipelineModel.load("models/feature_pipeline")
val loadedRFModel = RandomForestClassificationModel.load("models/rf_classifier")

// Process new data
val newDataFeatures = pipeline.transform(loadedFeaturePipeline, newDataDF)
val predictions = loadedRFModel.transform(newDataFeatures)

predictions.select("features", "prediction", "probability").show()
```

---

## Comparison: Basic vs Enhanced

### Basic Pipeline
```scala
// Limited functionality
val basic = new BasicFlightFeaturePipeline(
  textCols, numericCols, target, maxCat = 32, handleInvalid = "skip"
)

val trainData = basic.fit(trainDF)  // ❌ Returns DataFrame, can't reuse
```

### Enhanced Pipeline
```scala
// Full control and reusability
val enhanced = EnhancedFlightFeaturePipeline.builder()
  .withTextCols(textCols)
  .withNumericCols(numericCols)
  .withTarget(target)
  .withScaler("robust")
  .withFeatureSelection("chi2", 30)
  .build()

val (model, trainData) = enhanced.fitTransform(trainDF)  // ✅ Returns both
val testData = enhanced.transform(model, testDF)         // ✅ Reusable
model.save("path")                                       // ✅ Persistent
```

---

## Best Practices

### 1. Choose the Right Scaler

```scala
// For tree-based models (Random Forest, Decision Tree)
scalerType = None  // No scaling needed

// For logistic regression, SVM
scalerType = Some("standard")  // Z-score normalization

// For neural networks with activation functions
scalerType = Some("minmax")  // [0,1] range

// For data with outliers
scalerType = Some("robust")  // Uses median/IQR
```

### 2. Feature Selection Strategy

```scala
// Start with all features
val fullPipeline = new EnhancedFlightFeaturePipeline(...)

// Iteratively reduce
val selectedPipeline = new EnhancedFlightFeaturePipeline(
  featureSelector = Some(("chi2", numFeatures)),
  ...
)

// Test different thresholds: 20, 30, 40, 50
// Choose based on validation performance
```

### 3. Train/Test Consistency

```scala
// ✅ CORRECT: Fit on train, transform on test
val (model, trainData) = pipeline.fitTransform(trainDF)
val testData = pipeline.transform(model, testDF)

// ❌ WRONG: Fitting on test data (data leakage!)
val trainData = pipeline.fitTransform(trainDF)._2
val testData = pipeline.fitTransform(testDF)._2  // NEVER DO THIS
```

### 4. Model Versioning

```scala
// Save with version and metadata
val version = "v1.2.0"
val modelPath = s"models/flight_pipeline_$version"

model.write.overwrite().save(modelPath)

// Save metadata separately
val metadata = Map(
  "version" -> version,
  "train_date" -> java.time.LocalDate.now.toString,
  "scaler" -> "robust",
  "num_features" -> "30"
)

// Write metadata JSON
import org.json4s.jackson.Serialization.write
implicit val formats = org.json4s.DefaultFormats
val metadataJson = write(metadata)
// Save to file...
```

---

## Performance Considerations

### Memory Usage

- **StringIndexer**: O(n × m) where n = rows, m = categorical columns
- **VectorAssembler**: O(n × d) where d = total features
- **StandardScaler**: O(n × d) for computing statistics
- **Feature Selection**: O(n × d) for chi-squared computation

### Speed Optimizations

```scala
// Cache intermediate results for iterative workflows
val (model, trainData) = pipeline.fitTransform(trainDF)
trainData.cache()  // Cache if training multiple models

// Persist model for fast loading
model.write.overwrite().save(modelPath)

// Use fewer features to speed up training
featureSelector = Some(("chi2", 20))  // Reduce from 60 to 20
```

---

## Troubleshooting

### Issue 1: "Column not found" error
```scala
// Check column names match exactly (case-sensitive)
trainDF.columns.foreach(println)

// Ensure columns exist before pipeline
require(textCols.forall(trainDF.columns.contains))
```

### Issue 2: NaN in scaled features
```scala
// Check for columns with zero variance
trainDF.describe().show()

// Use RobustScaler or filter zero-variance features
```

### Issue 3: Model loading fails
```scala
// Ensure Spark versions match
// Save: Spark 3.5.3 → Load: Spark 3.5.3 ✅
// Save: Spark 3.5.3 → Load: Spark 3.4.0 ❌

// Check save path exists
import org.apache.hadoop.fs.{FileSystem, Path}
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
fs.exists(new Path(modelPath))
```

---

## Migration Guide: Basic → Enhanced

```scala
// Old code (Basic)
val oldPipeline = new BasicFlightFeaturePipeline(
  textCols, numericCols, target, maxCat = 32, handleInvalid = "skip"
)
val trainData = oldPipeline.fit(trainDF)

// New code (Enhanced) - minimal changes
val newPipeline = new EnhancedFlightFeaturePipeline(
  textCols, numericCols, target, maxCat = 32, handleInvalid = "skip"
  // All other params optional with defaults
)
val (model, trainData) = newPipeline.fitTransform(trainDF)

// To keep old behavior, just use transformed data
val trainDataOldStyle = newPipeline.fitTransform(trainDF)._2
```

---

## Related Classes

- **BasicFlightFeaturePipeline**: Simple version without advanced features
- **FlightFeatureExtractor**: Orchestrates pipeline with automatic column detection
- **DataQualityMetrics**: Identifies column types for pipeline configuration
- **FlightPreprocessingPipeline**: Upstream feature engineering

---

## References

- [Spark ML Pipeline](https://spark.apache.org/docs/latest/ml-pipeline.html)
- [Feature Scaling Guide](https://spark.apache.org/docs/latest/ml-features.html#standardscaler)
- [Feature Selection Methods](https://spark.apache.org/docs/latest/ml-features.html#chisqselector)
- [Model Persistence](https://spark.apache.org/docs/latest/ml-pipeline.html#saving-and-loading-pipelines)
