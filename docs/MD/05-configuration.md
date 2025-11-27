# ⚙️ Configuration Guide

Complete guide to configuring experiments, datasets, models, and training parameters in the Flight Delay Prediction system.

---

## Table of Contents

- [Overview](#overview)
- [Configuration Files](#configuration-files)
- [Configuration Structure](#configuration-structure)
- [Common Configuration](#common-configuration)
- [Experiment Configuration](#experiment-configuration)
- [Feature Extraction Configuration](#feature-extraction-configuration)
- [Training Configuration](#training-configuration)
- [Hyperparameters](#hyperparameters)
- [MLflow Configuration](#mlflow-configuration)
- [Environment-Specific Configs](#environment-specific-configs)
- [Configuration Examples](#configuration-examples)
- [Best Practices](#best-practices)

---

## Overview

The Flight Delay Prediction system is **configuration-driven**, meaning all experiments, models, and hyperparameters are defined in YAML files, not hardcoded.

### Benefits

- **Reproducibility**: Experiments can be replayed with exact settings
- **Flexibility**: Change parameters without recompiling
- **Versioning**: Configuration files tracked in git
- **Multiple Experiments**: Run different configurations in sequence
- **Environment Separation**: Different configs for local vs production

### Configuration Lifecycle

```
1. Write/Edit YAML config
   ↓
2. Recompile JAR (only if code changes)
   ↓
3. Submit job with config
   ↓
4. Pipeline reads config
   ↓
5. Experiments execute per config
   ↓
6. Results saved + tracked
```

---

## Configuration Files

### Location

```
src/main/resources/
├── local-config.yml      # Local Docker environment
└── lamsade-config.yml    # Production cluster environment
```

### Selection

Configuration is selected via command-line argument:

```bash
# Use local-config.yml (default)
./local-submit.sh

# Use lamsade-config.yml
./local-submit.sh lamsade-config.yml
```

**Default**: `local-config.yml` if no argument provided

---

## Configuration Structure

### Top-Level Schema

```yaml
environment: "local"          # Environment name

common:                       # Shared settings across all experiments
  seed: 42
  data: {...}
  output: {...}
  mlflow: {...}

experiments:                  # List of experiments
  - name: "exp1"
    enabled: true
    target: "label_is_delayed_15min"
    featureExtraction: {...}
    model: {...}
    train: {...}

  - name: "exp2"
    enabled: false
    ...
```

### Hierarchy

```
AppConfiguration
├── environment: String
├── common: CommonConfig
│   ├── seed: Int
│   ├── data: DataConfig
│   ├── output: OutputConfig
│   └── mlflow: MLFlowConfig
└── experiments: List[ExperimentConfig]
    ├── name: String
    ├── description: String
    ├── enabled: Boolean
    ├── target: String
    ├── featureExtraction: FeatureExtractionConfig
    ├── model: ExperimentModelConfig
    └── train: TrainConfig
```

---

## Common Configuration

Settings shared across **all experiments**.

### Seed

```yaml
common:
  seed: 42
```

**Purpose**: Random seed for reproducibility

**Used in**:
- Train/test splits
- K-fold cross-validation splits
- Random Forest bootstrap sampling

**Recommendation**: Use fixed seed (42, 123, etc.) for reproducibility

---

### Data Paths

```yaml
common:
  data:
    basePath: "/data"
    flight:
      path: "/data/FLIGHT-3Y/Flights/201201.csv"
    weather:
      path: "/data/FLIGHT-3Y/Weather/201201hourly.txt"
    airportMapping:
      path: "/data/FLIGHT-3Y/wban_airport_timezone.csv"
```

| Field | Description | Format | Example |
|-------|-------------|--------|---------|
| `basePath` | Root data directory | Absolute path | `/data` |
| `flight.path` | Flight CSV file(s) | Absolute path, supports wildcards | `*.csv` for multiple files |
| `weather.path` | Weather TXT file(s) | Absolute path, supports wildcards | `*.txt` for multiple files |
| `airportMapping.path` | Airport WBAN mapping | Absolute path | `.../wban_airport_timezone.csv` |

**Wildcard Support**:
```yaml
# Single file
flight:
  path: "/data/FLIGHT-3Y/Flights/201201.csv"

# Multiple files (all months)
flight:
  path: "/data/FLIGHT-3Y/Flights/*.csv"
```

**Docker Paths**:
- In `local-config.yml`: Use `/data` (container path)
- In `lamsade-config.yml`: Use absolute host paths

---

### Output Paths

```yaml
common:
  output:
    basePath: "/output"
```

**Purpose**: Root directory for all experiment outputs

**Structure Created**:
```
/output/
├── common/
│   └── data/
│       └── processed_flights.parquet  # Shared preprocessed data
│
└── {experiment_name}/                 # Per-experiment outputs
    ├── features/
    │   └── extracted_features/        # Features (with PCA)
    ├── models/
    │   └── randomforest_final/        # Trained model
    └── metrics/
        ├── cv_fold_metrics.csv        # Cross-validation per fold
        ├── cv_summary.csv             # CV mean ± std
        ├── holdout_test_metrics.csv   # Test set metrics
        ├── best_hyperparameters.csv   # Best params (if grid search)
        ├── holdout_roc_data.csv       # ROC curve data
        └── pca_variance.csv           # PCA analysis (if PCA enabled)
```

**Docker Mapping**:
- Container: `/output`
- Host: `work/output/`

---

### MLflow Configuration

```yaml
common:
  mlflow:
    enabled: true
    trackingUri: "http://mlflow-server:5000"
```

| Field | Type | Description | Values |
|-------|------|-------------|--------|
| `enabled` | Boolean | Enable/disable MLflow tracking | `true`, `false` |
| `trackingUri` | String | MLflow server URL | `http://mlflow-server:5000` (Docker) |

**Important Notes**:

- **Docker**: Use `http://mlflow-server:5000` (internal container name)
- **Local Manual**: Use `http://localhost:5555`
- **Disabled**: Set `enabled: false` to skip MLflow tracking

**When to Disable**:
- Quick local tests
- No MLflow server available
- Debugging without tracking overhead

---

## Experiment Configuration

Settings specific to **each experiment**.

### Basic Experiment Fields

```yaml
experiments:
  - name: "exp4_rf_pca_cv_15min"
    description: "Random Forest with PCA predicting 15-min delays"
    enabled: true
    target: "label_is_delayed_15min"
```

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `name` | String | Unique experiment identifier | `exp4_rf_pca_cv_15min` |
| `description` | String | Human-readable description | `Baseline Random Forest...` |
| `enabled` | Boolean | Run this experiment? | `true` or `false` |
| `target` | String | Label column name | `label_is_delayed_15min` |

**Target Labels**:

Available delay labels (generated in preprocessing):

| Target | Description | Threshold |
|--------|-------------|-----------|
| `label_is_delayed_15min` | Delayed ≥ 15 minutes | `ArrDelay >= 15` |
| `label_is_delayed_30min` | Delayed ≥ 30 minutes | `ArrDelay >= 30` |
| `label_is_delayed_45min` | Delayed ≥ 45 minutes | `ArrDelay >= 45` |
| `label_is_delayed_60min` | Delayed ≥ 60 minutes | `ArrDelay >= 60` |

**Enabled vs Disabled**:
- `enabled: true` → Experiment runs
- `enabled: false` → Experiment skipped (useful for organizing config)

---

## Feature Extraction Configuration

Controls feature engineering and dimensionality reduction.

### PCA Feature Extraction

```yaml
experiments:
  - name: "exp_with_pca"
    featureExtraction:
      type: pca
      pcaVarianceThreshold: 0.7
```

| Field | Type | Description | Values | Default |
|-------|------|-------------|--------|---------|
| `type` | String | Feature extraction method | `pca`, `none` | `none` |
| `pcaVarianceThreshold` | Double | Cumulative variance to retain | 0.5 - 0.99 | 0.7 |

**PCA Behavior**:

- **Standardization**: Features standardized (mean=0, std=1)
- **Dimensionality Reduction**: Keep components explaining `pcaVarianceThreshold` variance
- **Output**: Reduced feature vector

**Example**:
```
Original features: 50
pcaVarianceThreshold: 0.7 (70% variance)
Result: ~12-15 PCA components
```

**When to Use PCA**:
- ✅ High-dimensional data (50+ features)
- ✅ Multicollinearity suspected
- ✅ Want faster training
- ❌ Need feature interpretability

---

### No Feature Extraction

```yaml
experiments:
  - name: "exp_no_pca"
    featureExtraction:
      type: none
```

Uses all original features without transformation.

---

## Training Configuration

Controls train/test split, cross-validation, and grid search.

### Complete Training Config

```yaml
experiments:
  - name: "exp_example"
    train:
      trainRatio: 0.8

      crossValidation:
        numFolds: 5

      gridSearch:
        enabled: true
        evaluationMetric: "f1"

      hyperparameters:
        numTrees: [50, 100]
        maxDepth: [5, 10]
        maxBins: 32
        minInstancesPerNode: 1
        subsamplingRate: 1.0
        featureSubsetStrategy: "auto"
        impurity: "gini"
```

---

### Train/Test Split

```yaml
train:
  trainRatio: 0.8
```

| Field | Type | Description | Range | Default |
|-------|------|-------------|-------|---------|
| `trainRatio` | Double | Fraction for training (dev set) | 0.5 - 0.95 | 0.8 |

**Split Behavior**:
- **Dev Set**: `trainRatio` portion (e.g., 80%)
- **Test Set**: Remaining portion (e.g., 20%)

**Example**:
```
trainRatio: 0.8
Total: 100,000 samples
Dev:   80,000 (used for CV + final training)
Test:  20,000 (hold-out evaluation)
```

**Recommendations**:
- Standard: `0.8` (80/20 split)
- More data: `0.9` (90/10 split)
- Less data: `0.7` (70/30 split)

---

### Cross-Validation

```yaml
train:
  crossValidation:
    numFolds: 5
```

| Field | Type | Description | Range | Default |
|-------|------|-------------|-------|---------|
| `numFolds` | Int | Number of folds for K-fold CV | 3 - 10 | 5 |

**K-Fold Behavior**:

For `numFolds: 5`:
```
Fold 1: Train on folds [2,3,4,5], Validate on fold 1
Fold 2: Train on folds [1,3,4,5], Validate on fold 2
Fold 3: Train on folds [1,2,4,5], Validate on fold 3
Fold 4: Train on folds [1,2,3,5], Validate on fold 4
Fold 5: Train on folds [1,2,3,4], Validate on fold 5
```

**Results**:
- Per-fold metrics (accuracy, precision, recall, F1, AUC)
- Mean ± Std across folds
- Best hyperparameters (if grid search enabled)

**Recommendations**:
- Standard: `numFolds: 5`
- More robust: `numFolds: 10` (slower)
- Quick tests: `numFolds: 3`

---

### Grid Search

```yaml
train:
  gridSearch:
    enabled: true
    evaluationMetric: "f1"
```

| Field | Type | Description | Values | Default |
|-------|------|-------------|--------|---------|
| `enabled` | Boolean | Enable grid search | `true`, `false` | `false` |
| `evaluationMetric` | String | Metric to optimize | `f1`, `accuracy`, `auc` | `f1` |

**Grid Search Behavior**:

When `enabled: true`:
1. Generate all hyperparameter combinations
2. For each fold:
   - Train models with each combination
   - Evaluate on validation set
3. Select combination with best `evaluationMetric`
4. Return best hyperparameters

**Example**:
```yaml
hyperparameters:
  numTrees: [50, 100]
  maxDepth: [5, 10]

Grid:
  (numTrees=50, maxDepth=5)
  (numTrees=50, maxDepth=10)
  (numTrees=100, maxDepth=5)
  (numTrees=100, maxDepth=10)

Total models trained per fold: 4
Total models (5 folds): 20
```

**Evaluation Metrics**:
- `f1` - F1-score (recommended for imbalanced data)
- `accuracy` - Overall accuracy
- `auc` - Area under ROC curve

**When to Disable**:
- ❌ Quick experiments
- ❌ Already know best hyperparameters
- ✅ Use single hyperparameter values:
  ```yaml
  hyperparameters:
    numTrees: [50]    # Single value
    maxDepth: [5]     # Single value
  ```

---

## Hyperparameters

Random Forest hyperparameters for model training.

### Complete Hyperparameters

```yaml
train:
  hyperparameters:
    numTrees: [50, 100]
    maxDepth: [5, 10]
    maxBins: 32
    minInstancesPerNode: 1
    subsamplingRate: 1.0
    featureSubsetStrategy: "auto"
    impurity: "gini"
```

---

### Grid Search Hyperparameters

**Format**: List of values `[val1, val2, ...]`

#### numTrees

```yaml
numTrees: [50, 100, 200]
```

| Description | Type | Range | Default | Recommendation |
|-------------|------|-------|---------|----------------|
| Number of trees in forest | Int | 10 - 500 | 100 | 50-100 for speed, 200+ for accuracy |

**Trade-offs**:
- More trees → Better performance, longer training
- Fewer trees → Faster, risk underfitting

**Examples**:
- Quick test: `[50]`
- Grid search: `[50, 100, 200]`
- Production: `[100]`

---

#### maxDepth

```yaml
maxDepth: [5, 10, 15]
```

| Description | Type | Range | Default | Recommendation |
|-------------|------|-------|---------|----------------|
| Maximum tree depth | Int | 3 - 30 | 5 | 5-10 to avoid overfitting |

**Trade-offs**:
- Deeper trees → More complex, risk overfitting
- Shallower trees → Simpler, risk underfitting

**Examples**:
- Shallow (simple): `[5]`
- Grid search: `[5, 10, 15]`
- Deep (complex): `[20]`

---

### Fixed Hyperparameters

**Format**: Single value (not a list)

#### maxBins

```yaml
maxBins: 32
```

| Description | Type | Range | Default | Recommendation |
|-------------|------|-------|---------|----------------|
| Max bins for discretizing continuous features | Int | 2 - 256 | 32 | 32 (standard), 64 for more granularity |

**Purpose**: Discretizes continuous features for splitting

**Trade-offs**:
- More bins → Finer splits, slower training
- Fewer bins → Coarser splits, faster

---

#### minInstancesPerNode

```yaml
minInstancesPerNode: 1
```

| Description | Type | Range | Default | Recommendation |
|-------------|------|-------|---------|----------------|
| Minimum samples in a leaf node | Int | 1 - 100 | 1 | 1 (flexible), 5-10 to reduce overfitting |

**Purpose**: Prevents very small leaf nodes

**Trade-offs**:
- Higher value → Less overfitting, risk underfitting
- Lower value → More flexible, risk overfitting

---

#### subsamplingRate

```yaml
subsamplingRate: 1.0
```

| Description | Type | Range | Default | Recommendation |
|-------------|------|-------|---------|----------------|
| Fraction of data to sample per tree | Double | 0.1 - 1.0 | 1.0 | 1.0 (all data), 0.8 for regularization |

**Purpose**: Controls bootstrap sampling

**Trade-offs**:
- 1.0 → Use all data (bagging)
- < 1.0 → Random subsampling (faster, regularization)

---

#### featureSubsetStrategy

```yaml
featureSubsetStrategy: "auto"
```

| Description | Type | Values | Default | Recommendation |
|-------------|------|--------|---------|----------------|
| Features to consider per split | String | `auto`, `all`, `sqrt`, `log2`, `onethird` | `auto` | `auto` (√n for classification) |

**Strategies**:
- `auto` → √n for classification, n/3 for regression
- `all` → All features (no randomness)
- `sqrt` → √n features
- `log2` → log₂(n) features
- `onethird` → n/3 features

**Trade-offs**:
- More features → Less randomness, better individual trees
- Fewer features → More diversity, better ensemble

---

#### impurity

```yaml
impurity: "gini"
```

| Description | Type | Values | Default | Recommendation |
|-------------|------|--------|---------|----------------|
| Impurity measure for splits | String | `gini`, `entropy` | `gini` | `gini` (faster), `entropy` (information gain) |

**Measures**:
- `gini` - Gini impurity (faster to compute)
- `entropy` - Information gain (more principled)

**Performance**: Usually similar, `gini` slightly faster

---

## Model Configuration

```yaml
experiments:
  - name: "exp_example"
    model:
      modelType: "randomforest"
```

| Field | Type | Description | Values |
|-------|------|-------------|--------|
| `modelType` | String | ML algorithm | `randomforest` (extensible) |

**Extensibility**:

To add new models (GBT, LogisticRegression):
1. Implement `MLModel` trait
2. Register in `ModelFactory`
3. Use new `modelType` value

See [Adding Models Guide](10-adding-models.md)

---

## Environment-Specific Configs

### Local Environment (Docker)

**File**: `src/main/resources/local-config.yml`

```yaml
environment: "local"

common:
  data:
    basePath: "/data"  # Container path
    flight:
      path: "/data/FLIGHT-3Y/Flights/201201.csv"

  output:
    basePath: "/output"  # Container path

  mlflow:
    enabled: true
    trackingUri: "http://mlflow-server:5000"  # Container name
```

**Characteristics**:
- Uses Docker container paths (`/data`, `/output`)
- MLflow enabled with internal container communication
- Single month data (201201.csv) for quick testing

---

### Production Environment (LAMSADE Cluster)

**File**: `src/main/resources/lamsade-config.yml`

```yaml
environment: "lamsade"

common:
  data:
    basePath: "/students/p6emiasd2025/mchettih/data"
    flight:
      path: "/students/p6emiasd2025/mchettih/data/FLIGHT-3Y/Flights/*.csv"

  output:
    basePath: "/output"

  mlflow:
    enabled: false  # No MLflow on cluster
```

**Characteristics**:
- Uses absolute cluster paths
- Wildcard for multiple months (`*.csv`)
- MLflow disabled (no server on cluster)

---

## Configuration Examples

### Example 1: Quick Test (No PCA, No Grid Search)

```yaml
experiments:
  - name: "quick_test"
    description: "Fast baseline test"
    enabled: true
    target: "label_is_delayed_15min"

    featureExtraction:
      type: none  # No PCA

    model:
      modelType: "randomforest"

    train:
      trainRatio: 0.8

      crossValidation:
        numFolds: 3  # Fewer folds for speed

      gridSearch:
        enabled: false  # No grid search

      hyperparameters:
        numTrees: [50]      # Single value
        maxDepth: [5]       # Single value
        maxBins: 32
        minInstancesPerNode: 1
        subsamplingRate: 1.0
        featureSubsetStrategy: "auto"
        impurity: "gini"
```

**Use Case**: Quick validation, debugging

---

### Example 2: Production (PCA + Grid Search)

```yaml
experiments:
  - name: "production_15min"
    description: "Full pipeline with hyperparameter tuning"
    enabled: true
    target: "label_is_delayed_15min"

    featureExtraction:
      type: pca
      pcaVarianceThreshold: 0.7

    model:
      modelType: "randomforest"

    train:
      trainRatio: 0.8

      crossValidation:
        numFolds: 5

      gridSearch:
        enabled: true
        evaluationMetric: "f1"

      hyperparameters:
        numTrees: [50, 100, 200]    # Grid search
        maxDepth: [5, 10, 15]       # Grid search
        maxBins: 32
        minInstancesPerNode: 1
        subsamplingRate: 1.0
        featureSubsetStrategy: "auto"
        impurity: "gini"
```

**Use Case**: Best performance, research experiments

---

### Example 3: Multiple Experiments (Different Targets)

```yaml
experiments:
  # Experiment 1: 15-minute delays
  - name: "exp_15min"
    enabled: true
    target: "label_is_delayed_15min"
    featureExtraction:
      type: pca
      pcaVarianceThreshold: 0.7
    train:
      hyperparameters:
        numTrees: [100]
        maxDepth: [10]

  # Experiment 2: 30-minute delays
  - name: "exp_30min"
    enabled: true
    target: "label_is_delayed_30min"
    featureExtraction:
      type: pca
      pcaVarianceThreshold: 0.7
    train:
      hyperparameters:
        numTrees: [100]
        maxDepth: [10]

  # Experiment 3: 60-minute delays
  - name: "exp_60min"
    enabled: true
    target: "label_is_delayed_60min"
    featureExtraction:
      type: pca
      pcaVarianceThreshold: 0.8  # More variance for harder task
    train:
      hyperparameters:
        numTrees: [200]    # More trees for harder task
        maxDepth: [15]     # Deeper trees
```

**Use Case**: Compare performance across different delay thresholds

---

## Best Practices

### 1. Naming Conventions

```yaml
name: "exp4_rf_pca_cv_15min"
       │   │   │   │   └─ Target (15min delay)
       │   │   │   └───── Cross-validation
       │   │   └───────── PCA enabled
       │   └───────────── Random Forest
       └───────────────── Experiment number
```

**Recommended Format**:
- Include experiment number
- Include model type (`rf`, `gbt`, `lr`)
- Include feature extraction (`pca`, `none`)
- Include target (`15min`, `30min`, etc.)

---

### 2. Reproducibility

```yaml
common:
  seed: 42  # Always set a fixed seed
```

**Ensure**:
- Same seed across experiments for fair comparison
- Document seed changes if needed

---

### 3. Incremental Testing

**Start Simple, Add Complexity**:

1. **Baseline**:
   ```yaml
   enabled: true
   featureExtraction: {type: none}
   gridSearch: {enabled: false}
   hyperparameters: {numTrees: [50], maxDepth: [5]}
   ```

2. **Add PCA**:
   ```yaml
   featureExtraction: {type: pca, pcaVarianceThreshold: 0.7}
   ```

3. **Add Grid Search**:
   ```yaml
   gridSearch: {enabled: true}
   hyperparameters: {numTrees: [50, 100], maxDepth: [5, 10]}
   ```

---

### 4. Grid Search Strategy

**Conservative** (2-4 combinations):
```yaml
numTrees: [50, 100]
maxDepth: [5]
# Total: 2 combinations
```

**Moderate** (6-9 combinations):
```yaml
numTrees: [50, 100]
maxDepth: [5, 10]
# Total: 4 combinations
```

**Extensive** (12+ combinations):
```yaml
numTrees: [50, 100, 200]
maxDepth: [5, 10, 15]
# Total: 9 combinations
```

**Consider**: Training time = (combinations) × (folds) × (base time)

---

### 5. Experiment Organization

**Group by Purpose**:

```yaml
experiments:
  # Baseline experiments
  - name: "baseline_15min"
    enabled: true
    ...

  # PCA experiments
  - name: "pca_15min"
    enabled: true
    ...

  # Grid search experiments
  - name: "tuned_15min"
    enabled: true
    ...

  # Disabled for future
  - name: "future_gbt_15min"
    enabled: false
    ...
```

---

### 6. Version Control

**Commit configuration changes**:

```bash
git add src/main/resources/local-config.yml
git commit -m "Add experiment: exp5_rf_pca_cv_30min"
```

**Tag important configs**:

```bash
git tag -a v1.0-baseline -m "Baseline configuration"
```

---

## Validation and Troubleshooting

### Configuration Validation

The system validates configuration on startup:

```
✓ Configuration 'local' loaded successfully
✓ Found 4 enabled experiments
```

### Common Issues

**Issue 1: Invalid YAML Syntax**

```
Error: mapping values are not allowed here
```

**Solution**: Check indentation (use 2 spaces, not tabs)

---

**Issue 2: Unknown Target Label**

```
Error: Column label_is_delayed_XYZ not found
```

**Solution**: Use valid target labels:
- `label_is_delayed_15min`
- `label_is_delayed_30min`
- `label_is_delayed_45min`
- `label_is_delayed_60min`

---

**Issue 3: Invalid Model Type**

```
Error: Unknown model type: XYZ
```

**Solution**: Use `randomforest` (or implement new model)

---

## Next Steps

- **[Data Pipeline](06-data-pipeline.md)** - Understand data loading and preprocessing
- **[Feature Engineering](07-feature-engineering.md)** - Learn about PCA and feature extraction
- **[ML Pipeline](08-ml-pipeline.md)** - Deep dive into training and evaluation
- **[MLflow Integration](09-mlflow-integration.md)** - Experiment tracking with MLflow

---

**Configuration Guide Complete! Ready to run experiments.** ⚙️
