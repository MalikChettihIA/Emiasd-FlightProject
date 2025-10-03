# BasicFlightFeaturePipeline - Detailed Explanation

## Overview

This class implements an **automated ML feature preprocessing pipeline** using Spark MLlib's Pipeline API. It transforms raw flight data into ML-ready format with vectorized features and indexed labels.

---

## Purpose

Automates the standard ML data preparation workflow:
1. **Encode categorical variables** (text → numeric indices)
2. **Assemble all features** into a single vector
3. **Auto-detect discrete features** for tree-based models
4. **Prepare label column** for supervised learning

---

## Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `textCols` | `Array[String]` | Categorical columns to encode (e.g., `["carrier", "route_id"]`) |
| `numericCols` | `Array[String]` | Numeric columns to include as-is (e.g., `["departure_hour", "distance_score"]`) |
| `target` | `String` | Target column name (e.g., `"label_is_delayed_15min"`) |
| `maxCat` | `Int` | Max distinct values for VectorIndexer to treat as categorical (default: 32) |
| `handleInvalid` | `String` | Strategy for invalid values: `"skip"`, `"error"`, or `"keep"` |

---

## Pipeline Architecture

The pipeline consists of **3 Spark MLlib transformers** executed sequentially:

```
Raw Data → StringIndexer → VectorAssembler → VectorIndexer → ML-Ready Data
```

---

## Stage 1: StringIndexer

**Location:** Lines 54-61 in `BasicFlightFeaturePipeline.scala`

**What it does:**
- Converts categorical text columns to numeric indices
- Also indexes the target column for classification

**Configuration:**
```scala
// Input: ["carrier", "route_id", ..., "label_is_delayed_15min"]
// Output: ["indexed_carrier", "indexed_route_id", ..., "indexed_label_is_delayed_15min"]

val stringIndexer = new StringIndexer()
  .setInputCols(textCols ++ Array(target))  // All categorical + target
  .setOutputCols(outAttsNames)               // Prefixed with "indexed_"
  .setHandleInvalid(handleInvalid)           // Skip/error/keep invalid values
```

**Example Transformation:**
```
carrier     → indexed_carrier
"AA"        → 0.0
"DL"        → 1.0
"UA"        → 2.0
```

**Encoding Strategy:**
- Most frequent value → index 0
- Second most frequent → index 1
- Handles unseen values based on `handleInvalid` policy

---

## Stage 2: VectorAssembler

**Location:** Lines 63-69 in `BasicFlightFeaturePipeline.scala`

**What it does:**
- Combines indexed categorical features + numeric features into a single vector
- Creates the feature vector required by Spark ML algorithms

**Configuration:**
```scala
// Features = indexed categorical columns + original numeric columns
val features = outAttsNames.filterNot(_.contains(target)) ++ numericCols

val vectorAssembler = new VectorAssembler()
  .setInputCols(features)           // All feature columns
  .setOutputCol("featuresVec")      // Output vector name
  .setHandleInvalid(handleInvalid)  // Handle nulls/NaNs
```

**Example Transformation:**
```
Input columns:
  indexed_carrier     = 1.0
  indexed_route_id    = 45.0
  departure_hour      = 14.0
  distance_score      = 0.35

Output:
  featuresVec = [1.0, 45.0, 14.0, 0.35]
```

---

## Stage 3: VectorIndexer

**Location:** Lines 71-76 in `BasicFlightFeaturePipeline.scala`

**What it does:**
- Automatically detects which features are categorical within the vector
- Re-indexes categorical features for tree-based algorithms (Random Forest, Decision Trees)
- Critical for proper handling of categorical variables in tree models

**Configuration:**
```scala
val vectorIndexer = new VectorIndexer()
  .setInputCol("featuresVec")       // Input from VectorAssembler
  .setOutputCol("features")         // Final output name
  .setMaxCategories(maxCat)         // Threshold for categorical detection
  .setHandleInvalid(handleInvalid)  // Handle invalid indices
```

**Categorical Detection Logic:**
- If a feature has ≤ `maxCat` distinct values → treated as categorical
- Otherwise → treated as continuous

**Why This Matters:**

Tree-based models treat categorical features differently:
- **Categorical**: Splits consider all possible subsets (e.g., `{AA, DL}` vs `{UA}`)
- **Continuous**: Splits use threshold comparisons (e.g., `x < 5.3`)

---

## The `fit()` Method

**Location:** Lines 79-86 in `BasicFlightFeaturePipeline.scala`

**Execution Flow:**

```scala
def fit(data: DataFrame): DataFrame = {
  val pipeline = new Pipeline()
    .setStages(Array(stringIndexer, vectorAssembler, vectorIndexer))

  pipeline.fit(data)              // 1. Fit all transformers
          .transform(data)        // 2. Apply transformations
          .select(                // 3. Select final columns
            col("features"),      //    - Feature vector
            col("indexed_" + target)  // - Indexed target
          )
          .withColumnRenamed(     // 4. Rename target to "label"
            "indexed_" + target,
            "label"
          )
}
```

**Output Schema:**
```
features: Vector (dense/sparse)
label: Double (0.0, 1.0 for binary classification)
```

---

## Complete Example

### Input DataFrame:
```
+--------+---------+--------------+---------------+------------------+
| carrier| route_id| departure_hour| distance_score| label_delayed_15 |
+--------+---------+--------------+---------------+------------------+
|    AA  | 10397_1 |     14.0     |     0.35      |        1         |
|    DL  | 12266_5 |     8.0      |     0.82      |        0         |
+--------+---------+--------------+---------------+------------------+
```

### Pipeline Instantiation:
```scala
val pipeline = new BasicFlightFeaturePipeline(
  textCols      = Array("carrier", "route_id"),
  numericCols   = Array("departure_hour", "distance_score"),
  target        = "label_delayed_15",
  maxCat        = 32,
  handleInvalid = "skip"
)
```

### After StringIndexer:
```
+----------------+------------------+--------------+---------------------------+
| indexed_carrier| indexed_route_id | departure_hour| indexed_label_delayed_15 |
+----------------+------------------+--------------+---------------------------+
|      0.0       |      45.0        |     14.0     |           1.0             |
|      1.0       |      89.0        |     8.0      |           0.0             |
+----------------+------------------+--------------+---------------------------+
```

### After VectorAssembler:
```
+----------------------------------+-------+
|          featuresVec             | label |
+----------------------------------+-------+
| [0.0, 45.0, 14.0, 0.35]          |  1.0  |
| [1.0, 89.0, 8.0, 0.82]           |  0.0  |
+----------------------------------+-------+
```

### After VectorIndexer (Final Output):
```
+----------------------------------+-------+
|            features              | label |
+----------------------------------+-------+
| [0.0, 45.0, 14.0, 0.35]          |  1.0  |  ← Ready for ML!
| [1.0, 89.0, 8.0, 0.82]           |  0.0  |
+----------------------------------+-------+
```

---

## Key Design Decisions

### 1. Why Three Stages?
- **StringIndexer**: Required for categorical text → numeric conversion
- **VectorAssembler**: Required by all Spark ML algorithms (expect Vector input)
- **VectorIndexer**: Optimizes tree-based models by marking categorical features

### 2. Target Column Handling
The target is indexed separately because:
- Classification models expect numeric labels (0.0, 1.0, ...)
- Original string labels ("delayed", "on_time") must be converted
- Final rename to "label" follows Spark ML convention

### 3. HandleInvalid Strategy
- `"skip"`: Drop rows with nulls/unseen categories (default in this project)
- `"error"`: Throw exception on invalid values
- `"keep"`: Assign special index to invalid values

### 4. MaxCategories Threshold
- Default: 32
- Too low → categorical features treated as continuous (loses meaning)
- Too high → continuous features treated as categorical (inefficient splits)

---

## Integration with FlightFeatureExtractor

From `FlightFeatureExtractor.scala`:
```scala
val pipeline = new BasicFlightFeaturePipeline(
  textCols,
  numericCols,
  target,
  maxCat = 32,
  handleInvalid = "skip"
)
val extractedData = pipeline.fit(flightData)
```

The extractor:
1. Identifies text vs numeric columns using `DataQualityMetrics`
2. Drops unused label columns (keeps only target)
3. Passes column lists to this pipeline
4. Returns ML-ready DataFrame with `features` and `label`

---

## Strengths

✅ **Automated**: No manual feature engineering for categorical encoding
✅ **Reusable**: Works with any dataset given column specifications
✅ **Optimized**: VectorIndexer improves tree model performance
✅ **Configurable**: Handles different invalid value strategies
✅ **Standard Spark ML**: Compatible with all MLlib estimators

---

## Limitations

⚠️ **No feature scaling**: Numeric features aren't normalized (tree models don't need it, but logistic regression would)
⚠️ **No feature selection**: All features included (could add feature importance filtering)
⚠️ **Fixed pipeline**: Cannot easily inject custom transformers
⚠️ **Returns DataFrame**: Cannot reuse fitted transformers for new data (would need to return `PipelineModel`)

---

## Typical Use Case in This Project

```scala
// After preprocessing pipeline generates 60+ features
val processedData = FlightPreprocessingPipeline.execute(rawData)

// Extract features for "delay >= 15 minutes" prediction
val mlReadyData = FlightFeatureExtractor.extract(
  processedData,
  target = "label_is_delayed_15min"
)

// Now ready for Random Forest or Decision Tree
val rf = new RandomForestClassifier()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .fit(mlReadyData)
```

---

## Visual Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Raw Flight DataFrame                         │
│  carrier, route_id, departure_hour, distance_score, target      │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
        ┌──────────────────────────┐
        │    StringIndexer         │
        │  Text → Numeric Indices  │
        └──────────┬───────────────┘
                   │
                   ▼
        ┌──────────────────────────┐
        │   VectorAssembler        │
        │  Features → Vector       │
        └──────────┬───────────────┘
                   │
                   ▼
        ┌──────────────────────────┐
        │   VectorIndexer          │
        │  Mark Categorical Dims   │
        └──────────┬───────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────┐
│              ML-Ready DataFrame                                 │
│              features: Vector                                   │
│              label: Double                                      │
└─────────────────────────────────────────────────────────────────┘
```

This pipeline transforms engineered features into the vectorized format required by Spark's ML algorithms, completing the journey from raw CSV to model-ready data.

---

## Related Classes

- **FlightFeatureExtractor**: Orchestrates this pipeline with column detection
- **DataQualityMetrics**: Identifies text vs numeric columns automatically
- **FlightPreprocessingPipeline**: Generates the 60+ features that feed into this pipeline
- **FlightDelayPredictionApp**: Main application that coordinates the entire workflow

---

## References

- [Spark MLlib StringIndexer](https://spark.apache.org/docs/latest/ml-features.html#stringindexer)
- [Spark MLlib VectorAssembler](https://spark.apache.org/docs/latest/ml-features.html#vectorassembler)
- [Spark MLlib VectorIndexer](https://spark.apache.org/docs/latest/ml-features.html#vectorindexer)
- [Spark ML Pipeline](https://spark.apache.org/docs/latest/ml-pipeline.html)
