# 🔬 Feature Engineering Guide

Complete guide to feature extraction, vectorization, PCA dimensionality reduction, and feature analysis in the Flight Delay Prediction system.

---

## Table of Contents

- [Overview](#overview)
- [Feature Extraction Pipeline](#feature-extraction-pipeline)
- [Column Type Detection](#column-type-detection)
- [Feature Vectorization](#feature-vectorization)
- [PCA Dimensionality Reduction](#pca-dimensionality-reduction)
- [Variance Analysis](#variance-analysis)
- [PCA Artifacts](#pca-artifacts)
- [Feature Selection](#feature-selection)
- [Best Practices](#best-practices)

---

## Overview

Feature engineering transforms preprocessed flight data into **ML-ready feature vectors**, with optional **PCA dimensionality reduction** to handle high-dimensional data.

### Feature Engineering Objectives

1. **Detect** column types (text vs numeric) automatically
2. **Index** categorical text columns (e.g., carrier codes)
3. **Assemble** feature vectors from all columns
4. **Standardize** features (mean=0, std=1) when using PCA
5. **Reduce** dimensionality using PCA (optional)
6. **Save** feature vectors for training

### Key Benefits

- **Dimensionality Reduction**: 50 features → 12-15 PCA components
- **Faster Training**: Fewer features → faster models
- **Reduced Overfitting**: Remove noise and redundancy
- **Feature Decorrelation**: PCA removes feature correlations

---

## Feature Extraction Pipeline

### High-Level Flow

```
┌────────────────────────────────────────────────────────────────┐
│ STEP 1: Load Preprocessed Data                                 │
├────────────────────────────────────────────────────────────────┤
│ Load from: processed_flights.parquet                           │
│ Input: ~50 features + 4 labels                                 │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STEP 2: Select Target Label                                    │
├────────────────────────────────────────────────────────────────┤
│ Keep: label_is_delayed_15min (from config)                     │
│ Drop: label_is_delayed_30min, label_is_delayed_45min, etc.     │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STEP 3: Detect Column Types                                    │
├────────────────────────────────────────────────────────────────┤
│ Text columns: OP_CARRIER, ORIGIN, DEST, etc.                   │
│ Numeric columns: DISTANCE, CRS_DEP_TIME, feature_*, etc.       │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STEP 4: Feature Vectorization Pipeline                         │
├────────────────────────────────────────────────────────────────┤
│ 1. StringIndexer (for text columns)                            │
│    OP_CARRIER → indexed_OP_CARRIER (0, 1, 2, ...)              │
│                                                                 │
│ 2. VectorAssembler                                             │
│    [indexed_OP_CARRIER, DISTANCE, ...] → features vector       │
│                                                                 │
│ 3. StandardScaler (if PCA enabled)                             │
│    features → scaled_features (mean=0, std=1)                  │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STEP 5: PCA Dimensionality Reduction (Optional)                │
├────────────────────────────────────────────────────────────────┤
│ IF featureExtraction.type == "pca":                            │
│   1. Fit PCA on training data                                  │
│   2. Select components by variance threshold (e.g., 70%)       │
│   3. Transform features: 50 dims → 12 dims                     │
│   4. Save PCA model and analysis                               │
│                                                                 │
│ ELSE:                                                           │
│   Use all original features                                    │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STEP 6: Save Extracted Features                                │
├────────────────────────────────────────────────────────────────┤
│ Output: (features: Vector, label: Double)                      │
│ Save to: /output/{exp_name}/features/extracted_features/       │
└────────────────────────────────────────────────────────────────┘
```

### FlightFeatureExtractor

**Class**: `com.flightdelay.features.FlightFeatureExtractor`

**Entry Point**:

```scala
FlightFeatureExtractor.extract(
  data: DataFrame,
  experiment: ExperimentConfig
)(implicit config: AppConfiguration): DataFrame
```

---

## Column Type Detection

### Automatic Detection

The system uses `DataQualityMetrics` to detect column types automatically:

```scala
val flightDataMetric = DataQualityMetrics.metrics(flightData)

// Text columns (categorical)
val textCols = flightDataMetric
  .filter(col("colType").contains("text"))
  .select("name")
  .collect()

// Numeric columns (continuous)
val numericCols = flightDataMetric
  .filter(col("colType").contains("numeric"))
  .filter(!col("name").contains(target))  // Exclude label
  .select("name")
  .collect()
```

### Column Classification

| Type | Criteria | Examples |
|------|----------|----------|
| **Text** | String columns with distinct values | `OP_CARRIER`, `ORIGIN`, `DEST` |
| **Numeric** | Int, Double, Long columns | `DISTANCE`, `CRS_DEP_TIME`, `feature_departure_hour` |

**Example Output**:
```
- Detected 8 text columns and 42 numeric columns
```

---

## Feature Vectorization

### EnhancedFlightFeaturePipeline

**Class**: `com.flightdelay.features.pipelines.EnhancedFlightFeaturePipeline`

**Purpose**: Build Spark ML pipeline to vectorize features

### Pipeline Stages

#### Stage 1: StringIndexer (for categorical columns)

**Purpose**: Convert text categories to numeric indices

```scala
// Example: OP_CARRIER
"AA" → 0
"UA" → 1
"DL" → 2
...
```

**Implementation**:
```scala
val indexers = textCols.map { col =>
  new StringIndexer()
    .setInputCol(col)
    .setOutputCol(s"indexed_$col")
    .setHandleInvalid("skip")  // Skip rows with unseen categories
}
```

**Parameters**:
- `maxCat`: Maximum categories per column (default: 32)
- `handleInvalid`: How to handle unseen categories ("skip" = drop row)

#### Stage 2: VectorAssembler

**Purpose**: Combine all features into a single vector

```scala
val assembler = new VectorAssembler()
  .setInputCols(indexedTextCols ++ numericCols)
  .setOutputCol("features")
```

**Example**:
```
Input columns:
- indexed_OP_CARRIER: 2.0
- indexed_ORIGIN: 15.0
- DISTANCE: 1500.0
- CRS_DEP_TIME: 1530.0
- feature_departure_hour: 15.0

Output vector:
features: [2.0, 15.0, 1500.0, 1530.0, 15.0, ...]
```

#### Stage 3: StandardScaler (optional, for PCA)

**Purpose**: Standardize features to mean=0, std=1

```scala
val scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaled_features")
  .setWithMean(true)
  .setWithStd(true)
```

**Why Needed for PCA**:
- PCA is sensitive to feature scales
- Features with larger variances dominate principal components
- Standardization ensures all features contribute equally

**Example**:
```
Before:
DISTANCE: mean=1000, std=500
CRS_DEP_TIME: mean=1400, std=400

After:
DISTANCE: mean=0, std=1
CRS_DEP_TIME: mean=0, std=1
```

### Pipeline Execution

```scala
val pipeline = new Pipeline().setStages(
  indexers ++ Array(assembler, scaler)
)

val model = pipeline.fit(data)
val transformed = model.transform(data)
```

**Output Columns**:
- `features`: Assembled feature vector (or scaled_features if scaling enabled)
- `label`: Target label (renamed from target column)

---

## PCA Dimensionality Reduction

### PCAFeatureExtractor

**Class**: `com.flightdelay.features.pca.PCAFeatureExtractor`

**Purpose**: Reduce feature dimensionality while preserving variance

### PCA Basics

**Principal Component Analysis (PCA)**:
- Unsupervised linear transformation
- Finds orthogonal axes (principal components) that maximize variance
- Components ordered by variance explained (PC1 > PC2 > PC3 > ...)

**Benefits**:
- **Dimensionality Reduction**: 50 features → 12 components
- **Noise Reduction**: Remove low-variance components
- **Decorrelation**: Remove feature correlations
- **Faster Training**: Fewer features → faster models

### Variance-Based Component Selection

**Configuration**:
```yaml
featureExtraction:
  type: pca
  pcaVarianceThreshold: 0.7  # Keep 70% of variance
```

**Algorithm**:
```
1. Fit PCA with maximum components (min(n_features, n_samples))
2. Compute cumulative variance for each component
3. Select K components where cumulative_variance[K] >= threshold
4. Refit PCA with K components
5. Transform data
```

**Example**:
```
Original features: 50
Variance threshold: 0.70 (70%)

Component Variance Analysis:
PC1:  15.2% → Cumulative: 15.2%
PC2:  12.8% → Cumulative: 28.0%
PC3:  10.1% → Cumulative: 38.1%
PC4:   8.5% → Cumulative: 46.6%
PC5:   7.2% → Cumulative: 53.8%
PC6:   5.9% → Cumulative: 59.7%
PC7:   5.1% → Cumulative: 64.8%
PC8:   4.7% → Cumulative: 69.5%
PC9:   3.2% → Cumulative: 72.7%  ← Selected 9 components (>= 70%)
...

Result: 9 components explain 72.7% of variance
Dimensionality reduction: 82% (50 → 9 features)
```

### PCA Execution

```scala
val pca = PCAFeatureExtractor.varianceBased(
  threshold = 0.7,
  inputCol = "features",
  outputCol = "pcaFeatures"
)

// Fit PCA on training data
val (pcaModel, pcaData, analysis) = pca.fitTransform(trainData)

// Transform test data (use same model)
val testPcaData = pca.transform(pcaModel, testData)
```

**Output**:
```
PCA Transformation Complete:
  Original features: 50
  PCA components: 12
  Variance explained: 71.23%
```

### When to Use PCA

**Use PCA when**:
- ✅ High-dimensional data (50+ features)
- ✅ Multicollinearity (correlated features)
- ✅ Want faster training
- ✅ Overfitting suspected

**Don't use PCA when**:
- ❌ Need feature interpretability
- ❌ Low-dimensional data (< 20 features)
- ❌ Features already uncorrelated
- ❌ Non-linear relationships (PCA is linear)

---

## Variance Analysis

### VarianceAnalysis Object

**Case Class**: `com.flightdelay.features.pca.VarianceAnalysis`

**Purpose**: Detailed analysis of PCA variance explained

```scala
case class VarianceAnalysis(
  numComponents: Int,                // Number of selected components
  originalDimension: Int,            // Original number of features
  explainedVariance: Array[Double],  // Variance per component
  cumulativeVariance: Array[Double], // Cumulative variance
  totalVarianceExplained: Double,    // Total variance (last cumulative)
  componentIndices: Array[Int]       // Component indices (1, 2, 3, ...)
)
```

### Variance Metrics

#### Explained Variance

**Definition**: Variance explained by each individual component

```scala
explainedVariance(0) = 0.152  // PC1 explains 15.2% of variance
explainedVariance(1) = 0.128  // PC2 explains 12.8% of variance
explainedVariance(2) = 0.101  // PC3 explains 10.1% of variance
```

#### Cumulative Variance

**Definition**: Total variance explained by first K components

```scala
cumulativeVariance(0) = 0.152  // PC1:      15.2%
cumulativeVariance(1) = 0.280  // PC1+PC2:  28.0%
cumulativeVariance(2) = 0.381  // PC1+PC2+PC3: 38.1%
```

### Variance Report

**Generated Output**:
```
================================================================================
PCA Variance Analysis Report
================================================================================
Original Dimensions: 50
Selected Components: 12
Total Variance Explained: 71.23%

Variance by Component:
--------------------------------------------------------------------------------
Component    Individual     Cumulative     Cumulative %
--------------------------------------------------------------------------------
PC-1         0.152345       0.152345       15.23%
PC-2         0.128012       0.280357       28.04%
PC-3         0.101234       0.381591       38.16%
PC-4         0.085421       0.467012       46.70%
PC-5         0.072156       0.539168       53.92%
PC-6         0.059823       0.598991       59.90%
PC-7         0.051234       0.650225       65.02%
PC-8         0.047012       0.697237       69.72%
PC-9         0.032145       0.729382       72.94%  ← Selected (>= 70%)
...
================================================================================
```

---

## PCA Artifacts

PCA analysis generates multiple artifacts for visualization and analysis:

### 1. Variance CSV

**File**: `/output/{exp_name}/metrics/pca_variance.csv`

**Content**:
```csv
component,explained_variance,cumulative_variance,cumulative_variance_pct
1,0.152345,0.152345,15.23
2,0.128012,0.280357,28.04
3,0.101234,0.381591,38.16
...
```

**Use**: Scree plot, elbow detection

### 2. PCA Projections

**File**: `/output/{exp_name}/metrics/pca_projections.csv`

**Content**:
```csv
pc1,pc2,label
-2.345,1.234,0.0
1.567,-0.891,1.0
0.234,0.456,0.0
...
```

**Use**: Biplot, scatter plot (first 2 PCs)

**Limit**: Max 5,000 samples for performance

### 3. PCA Loadings

**File**: `/output/{exp_name}/metrics/pca_loadings.csv`

**Content**:
```csv
feature_index,feature_name,PC1,PC2,PC3,...
0,indexed_OP_CARRIER,0.234,-0.156,0.089,...
1,DISTANCE,0.456,0.321,-0.234,...
2,CRS_DEP_TIME,-0.123,0.567,0.345,...
...
```

**Purpose**: Show feature contributions to each PC

**Interpretation**:
- High absolute loading → feature strongly influences PC
- Positive/negative → direction of influence

**Example**:
```
PC1 loadings:
  DISTANCE: 0.456       (high positive)
  CRS_ELAPSED_TIME: 0.432  (high positive)
  → PC1 represents "flight distance/duration"

PC2 loadings:
  feature_departure_hour: 0.567  (high positive)
  feature_is_weekend: 0.398      (high positive)
  → PC2 represents "flight timing"
```

### 4. Feature Names

**File**: `/output/{exp_name}/features/feature_names.csv`

**Content**:
```csv
feature_index,feature_name
0,indexed_OP_CARRIER
1,indexed_ORIGIN
2,indexed_DEST
3,DISTANCE
4,CRS_DEP_TIME
...
```

**Purpose**: Map feature indices to names

---

## Feature Selection

**Note**: Feature selection is **separate from PCA** (not currently used in experiments)

### Configuration

```yaml
featureExtraction:
  type: feature_selection
  featureSelectionType: hybrid
  selectedFeatures:
    - DISTANCE
    - CRS_DEP_TIME
    - feature_departure_hour
    - ...
```

**Behavior**:
- Filter columns to `selectedFeatures` list
- No PCA applied
- Use only specified features

**Use Case**: When you know which features are important

---

## Best Practices

### 1. Always Standardize Before PCA

**Critical for PCA**:

```yaml
featureExtraction:
  type: pca  # Automatically enables StandardScaler
```

**Why**:
- PCA is scale-sensitive
- Large-scale features dominate principal components
- Standardization ensures equal contribution

### 2. Choose Variance Threshold Carefully

**Recommendations**:

| Threshold | Use Case | Trade-off |
|-----------|----------|-----------|
| **0.5** (50%) | Aggressive reduction | Fast, but loses information |
| **0.7** (70%) | Balanced (recommended) | Good speed/accuracy trade-off |
| **0.9** (90%) | Conservative | Slower, but retains more info |
| **0.95** (95%) | Minimal reduction | Close to original performance |

**Guideline**:
- Start with 0.7 (70%)
- If underfitting: Increase to 0.8 or 0.9
- If overfitting: Decrease to 0.5 or 0.6

### 3. Analyze Variance Reports

**Check**:
- Total variance explained
- Number of components selected
- Top 3-5 components (should explain significant variance)

**Red Flags**:
- PC1 explains < 10% → Features may not be correlated
- Need > 80% of components for 70% variance → PCA not effective

### 4. Interpret Principal Components

**Use PCA loadings to understand PCs**:

```
PC1 high loadings:
  DISTANCE: 0.45
  CRS_ELAPSED_TIME: 0.43
  → PC1 ≈ "Flight distance/duration"

PC2 high loadings:
  feature_departure_hour: 0.57
  feature_is_weekend: 0.40
  → PC2 ≈ "Flight timing"
```

### 5. Visualize PCA Results

**Generate visualizations**:

```bash
python work/scripts/visualize_pca.py /output/{exp_name}/metrics/pca_analysis
```

**Plots**:
- **Scree Plot**: Variance per component (find elbow)
- **Cumulative Variance**: Select threshold visually
- **Biplot**: First 2 PCs with feature vectors
- **Projection**: Data points in PC1-PC2 space

### 6. Compare PCA vs No-PCA

**Run experiments**:

```yaml
experiments:
  # Without PCA
  - name: "exp_no_pca"
    featureExtraction:
      type: none

  # With PCA
  - name: "exp_with_pca_70"
    featureExtraction:
      type: pca
      pcaVarianceThreshold: 0.7
```

**Compare**:
- Training time
- Model performance (F1, AUC)
- Overfitting (CV variance)

### 7. Save and Reuse PCA Model

**PCA model is saved**:

```
/output/{exp_name}/models/pca_model/
```

**Reuse for prediction**:

```scala
val pcaModel = PCAModel.load("pca_model_path")
val newDataPca = pcaModel.transform(newData)
```

**Important**: Always use the **same PCA model** fitted on training data for test/production

---

## Troubleshooting

### Issue 1: "Not enough variance with selected components"

**Symptoms**:
```
Warning: Could not reach 70% variance. Using all 50 components.
```

**Causes**:
- Features are not correlated
- Variance spread evenly across features

**Solutions**:
- Increase `pcaVarianceThreshold` (e.g., 0.9)
- Or disable PCA (`type: none`)

---

### Issue 2: "PCA gives worse performance than original features"

**Causes**:
- Too aggressive reduction (lost important information)
- Features already uncorrelated (PCA not helpful)

**Solutions**:
- Increase variance threshold (0.8, 0.9)
- Check variance report (PC1 should explain > 10%)
- Try without PCA

---

### Issue 3: "PC1 explains very little variance"

**Symptoms**:
```
PC1: 8.2%
PC2: 7.5%
PC3: 6.9%
...
```

**Interpretation**: Features are already uncorrelated; PCA may not help

**Solutions**:
- Use original features (no PCA)
- Or keep high variance threshold (0.95)

---

## Next Steps

- **[ML Pipeline](08-ml-pipeline.md)** - Model training with extracted features
- **[Visualization](12-visualization.md)** - Visualize PCA analysis
- **[Configuration](05-configuration.md)** - Configure PCA settings

---

**Feature Engineering Guide Complete! Ready to extract and reduce features.** 🔬
