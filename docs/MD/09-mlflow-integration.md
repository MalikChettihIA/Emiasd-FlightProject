# 📊 MLflow Integration Guide

Complete guide to experiment tracking, model registry, and result visualization using MLflow in the Flight Delay Prediction system.

---

## Table of Contents

- [Overview](#overview)
- [MLflow Architecture](#mlflow-architecture)
- [Configuration](#configuration)
- [Tracked Information](#tracked-information)
- [MLFlowTracker API](#mlflowtracker-api)
- [Experiment Lifecycle](#experiment-lifecycle)
- [MLflow UI](#mlflow-ui)
- [Best Practices](#best-practices)

---

## Overview

MLflow provides **experiment tracking**, **model versioning**, and **result visualization** for all machine learning experiments.

### MLflow Objectives

1. **Track** all experiments with parameters, metrics, and artifacts
2. **Compare** multiple experiments side-by-side
3. **Reproduce** experiments with exact configurations
4. **Version** models with metadata and lineage
5. **Visualize** training metrics and model performance
6. **Share** results with team members

### Key Benefits

- **Reproducibility**: Every experiment logged with config and seed
- **Comparison**: Compare 100+ experiments visually
- **Versioning**: Track model evolution over time
- **Collaboration**: Share results via web UI
- **Deployment**: Register and serve best models

---

## MLflow Architecture

### System Architecture

```
┌────────────────────────────────────────────────────────────────┐
│ Docker Network: spark-network                                  │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐                    ┌──────────────┐          │
│  │ spark-submit │ ────────────────→  │ mlflow-server│          │
│  │              │  HTTP POST         │              │          │
│  │ MLFlowTracker│  (params, metrics) │ Port: 5000   │          │
│  │              │                    │              │          │
│  └──────────────┘                    └──────────────┘          │
│                                             │                   │
│                                             ↓                   │
│                                   ┌──────────────┐              │
│                                   │  SQLite DB   │              │
│                                   │  (metadata)  │              │
│                                   └──────────────┘              │
│                                             │                   │
│                                             ↓                   │
│                                   ┌──────────────┐              │
│                                   │  Artifacts   │              │
│                                   │  (models,    │              │
│                                   │   CSVs, etc.)│              │
│                                   └──────────────┘              │
│                                                                 │
└────────────────────────────────────────────────────────────────┘
                            ↓
                ┌──────────────────────┐
                │    Host Browser      │
                │  localhost:5555      │
                │   (MLflow UI)        │
                └──────────────────────┘
```

### Components

| Component | Purpose | Location |
|-----------|---------|----------|
| **MLflow Server** | Central tracking server | Docker container (mlflow-server) |
| **Backend Store** | Metadata database (runs, params, metrics) | SQLite file (work/mlflow/mlflow.db) |
| **Artifact Store** | Model files, CSVs, plots | File system (work/mlflow/artifacts) |
| **MLFlowTracker** | Scala client for logging | src/main/scala/.../tracking/MLFlowTracker.scala |
| **MLflow UI** | Web interface | http://localhost:5555 |

### Docker Services

**MLflow Server**:
```yaml
mlflow-server:
  image: ghcr.io/mlflow/mlflow:v3.4.0
  ports:
    - "5555:5000"  # External:Internal
  environment:
    - MLFLOW_BACKEND_STORE_URI=sqlite:////mlflow/mlflow.db
    - MLFLOW_DEFAULT_ARTIFACT_ROOT=/mlflow/artifacts
  volumes:
    - ./work/mlflow:/mlflow
```

**Spark Submit** (client):
```yaml
spark-submit:
  environment:
    - MLFLOW_TRACKING_URI=http://mlflow-server:5000  # Internal URL
```

---

## Configuration

### Enable MLflow Tracking

**File**: `src/main/resources/local-config.yml`

```yaml
common:
  mlflow:
    enabled: true
    trackingUri: "http://mlflow-server:5000"
```

### Disable MLflow Tracking

**For quick local tests without tracking**:

```yaml
common:
  mlflow:
    enabled: false
```

### Initialization in Code

```scala
// Initialize MLflow at pipeline start
MLFlowTracker.initialize(
  uri = config.common.mlflow.trackingUri,  // "http://mlflow-server:5000"
  enable = config.common.mlflow.enabled     // true or false
)
```

**Output**:
```
[MLFlow] Initialized with tracking URI: http://mlflow-server:5000
```

Or if disabled:
```
[MLFlow] Tracking disabled
```

---

## Tracked Information

### Logged Data Categories

```
┌─────────────────────────────────────────────────────────────┐
│ Experiment Run (exp4_rf_pca_cv_15min)                       │
├─────────────────────────────────────────────────────────────┤
│ PARAMETERS                                                   │
│ • experiment_name: exp4_rf_pca_cv_15min                     │
│ • target: label_is_delayed_15min                            │
│ • model_type: randomforest                                  │
│ • train_ratio: 0.8                                          │
│ • cv_folds: 5                                               │
│ • grid_search_enabled: true                                 │
│ • grid_search_metric: f1                                    │
│ • pca_enabled: true                                         │
│ • pca_variance_threshold: 0.7                               │
│ • random_seed: 42                                           │
│ • numTrees: 100 (best from grid search)                     │
│ • maxDepth: 10 (best from grid search)                      │
│ • ... (all hyperparameters)                                 │
├─────────────────────────────────────────────────────────────┤
│ METRICS                                                      │
│ • cv_mean_accuracy: 0.8732                                  │
│ • cv_std_accuracy: 0.0123                                   │
│ • cv_mean_precision: 0.8567                                 │
│ • cv_std_precision: 0.0210                                  │
│ • cv_mean_recall: 0.8845                                    │
│ • cv_std_recall: 0.0187                                     │
│ • cv_mean_f1: 0.8702                                        │
│ • cv_std_f1: 0.0156                                         │
│ • cv_mean_auc: 0.9234                                       │
│ • cv_std_auc: 0.0156                                        │
│ • test_accuracy: 0.8789                                     │
│ • test_precision: 0.8612                                    │
│ • test_recall: 0.8923                                       │
│ • test_f1: 0.8765                                           │
│ • test_auc: 0.9301                                          │
│ • training_time_seconds: 287.45                             │
├─────────────────────────────────────────────────────────────┤
│ ARTIFACTS                                                    │
│ • models/randomforest_final/                                │
│ • metrics/cv_fold_metrics.csv                               │
│ • metrics/cv_summary.csv                                    │
│ • metrics/holdout_test_metrics.csv                          │
│ • metrics/best_hyperparameters.csv                          │
│ • metrics/holdout_roc_data.csv                              │
│ • metrics/pca_variance.csv (if PCA enabled)                 │
├─────────────────────────────────────────────────────────────┤
│ TAGS                                                         │
│ • mlflow.runName: exp4_rf_pca_cv_15min                      │
│ • experiment_description: Baseline Random Forest...         │
│ • environment: local                                        │
└─────────────────────────────────────────────────────────────┘
```

### Parameters vs Metrics

| Category | Type | Purpose | Examples |
|----------|------|---------|----------|
| **Parameters** | Input | Configuration, hyperparameters | `numTrees=100`, `maxDepth=10` |
| **Metrics** | Output | Performance measurements | `test_f1=0.8765`, `test_auc=0.9301` |
| **Tags** | Metadata | Descriptive labels | `environment=local`, `description=...` |
| **Artifacts** | Files | Models, plots, data | Models, CSVs, plots |

---

## MLFlowTracker API

### Class: MLFlowTracker

**Location**: `com.flightdelay.ml.tracking.MLFlowTracker`

**Purpose**: Scala client for MLflow tracking server

### Core Methods

#### 1. Initialize

```scala
def initialize(uri: String, enable: Boolean = true): Unit
```

**Purpose**: Set up MLflow client with tracking URI

**Example**:
```scala
MLFlowTracker.initialize(
  uri = "http://mlflow-server:5000",
  enable = true
)
```

---

#### 2. Get/Create Experiment

```scala
def getOrCreateExperiment(): Option[String]
```

**Purpose**: Get or create experiment named "flight-delay-prediction"

**Returns**: Experiment ID (or None if disabled/failed)

**Example**:
```scala
val experimentId = MLFlowTracker.getOrCreateExperiment()
// Output: [MLFlow] Using experiment: flight-delay-prediction (ID: 1)
```

---

#### 3. Start Run

```scala
def startRun(experimentId: String, runName: String): Option[String]
```

**Purpose**: Start a new run within an experiment

**Returns**: Run ID (or None if disabled/failed)

**Example**:
```scala
val runId = MLFlowTracker.startRun(
  experimentId = "1",
  runName = "exp4_rf_pca_cv_15min"
)
// Output: [MLFlow] Started run: exp4_rf_pca_cv_15min (ID: abc123...)
```

---

#### 4. Log Parameters

```scala
def logParams(runId: String, params: Map[String, Any]): Unit
```

**Purpose**: Log experiment configuration and hyperparameters

**Example**:
```scala
MLFlowTracker.logParams(runId, Map(
  "experiment_name" -> "exp4_rf_pca_cv_15min",
  "target" -> "label_is_delayed_15min",
  "model_type" -> "randomforest",
  "numTrees" -> 100,
  "maxDepth" -> 10,
  "pca_enabled" -> true,
  "pca_variance_threshold" -> 0.7
))
```

---

#### 5. Log Metrics

```scala
def logMetric(runId: String, key: String, value: Double, step: Long = 0): Unit
def logMetrics(runId: String, metrics: Map[String, Double], step: Long = 0): Unit
```

**Purpose**: Log performance metrics

**Example**:
```scala
// Log single metric
MLFlowTracker.logMetric(runId, "test_f1", 0.8765)

// Log multiple metrics
MLFlowTracker.logMetrics(runId, Map(
  "test_accuracy" -> 0.8789,
  "test_precision" -> 0.8612,
  "test_recall" -> 0.8923,
  "test_f1" -> 0.8765,
  "test_auc" -> 0.9301
))
```

**Step Parameter**: For time-series metrics (e.g., per-fold metrics)

```scala
// Log per-fold metrics
cvResult.foldMetrics.zipWithIndex.foreach { case (metrics, fold) =>
  MLFlowTracker.logMetric(runId, s"cv_fold${fold}_f1", metrics.f1Score, step = fold)
}
```

---

#### 6. Log Artifacts

```scala
def logArtifact(runId: String, localPath: String): Unit
```

**Purpose**: Upload files/directories to MLflow

**Example**:
```scala
// Log model directory
MLFlowTracker.logArtifact(runId, "/output/exp4/models/randomforest_final")

// Log metrics directory
MLFlowTracker.logArtifact(runId, "/output/exp4/metrics")

// Log single file
MLFlowTracker.logArtifact(runId, "/output/exp4/metrics/pca_variance.csv")
```

---

#### 7. Set Tags

```scala
def setTag(runId: String, key: String, value: String): Unit
```

**Purpose**: Add metadata tags to run

**Example**:
```scala
MLFlowTracker.setTag(runId, "experiment_description", "Baseline Random Forest with PCA")
MLFlowTracker.setTag(runId, "environment", "local")
MLFlowTracker.setTag(runId, "git_commit", "abc123def")
```

---

#### 8. End Run

```scala
def endRun(runId: String, status: RunStatus = RunStatus.FINISHED): Unit
```

**Purpose**: Mark run as completed

**Example**:
```scala
MLFlowTracker.endRun(runId)
// Output: [MLFlow] Ended run: abc123... (status: FINISHED)
```

---

## Experiment Lifecycle

### Complete Example

```scala
// 1. Initialize MLflow
MLFlowTracker.initialize(
  uri = "http://mlflow-server:5000",
  enable = true
)

// 2. Get/create experiment
val experimentId = MLFlowTracker.getOrCreateExperiment()

// 3. Start run
val runId = experimentId.flatMap(expId =>
  MLFlowTracker.startRun(expId, "exp4_rf_pca_cv_15min")
)

runId.foreach { rid =>
  // 4. Log parameters
  MLFlowTracker.logParams(rid, Map(
    "experiment_name" -> "exp4_rf_pca_cv_15min",
    "target" -> "label_is_delayed_15min",
    "model_type" -> "randomforest",
    "numTrees" -> 100,
    "maxDepth" -> 10
  ))

  // 5. Train model...
  val cvResult = CrossValidator.validate(devData, experiment)
  val finalModel = Trainer.trainFinal(devData, experiment, cvResult.bestHyperparameters)
  val testMetrics = ModelEvaluator.evaluate(finalModel.transform(testData))

  // 6. Log CV metrics
  MLFlowTracker.logMetrics(rid, Map(
    "cv_mean_f1" -> cvResult.avgMetrics.f1Score,
    "cv_std_f1" -> cvResult.stdMetrics.f1Score
  ))

  // 7. Log test metrics
  MLFlowTracker.logMetrics(rid, Map(
    "test_f1" -> testMetrics.f1Score,
    "test_accuracy" -> testMetrics.accuracy,
    "test_auc" -> testMetrics.areaUnderROC
  ))

  // 8. Log artifacts
  MLFlowTracker.logArtifact(rid, "/output/exp4/models/randomforest_final")
  MLFlowTracker.logArtifact(rid, "/output/exp4/metrics")

  // 9. Set tags
  MLFlowTracker.setTag(rid, "environment", "local")

  // 10. End run
  MLFlowTracker.endRun(rid)
}
```

---

## MLflow UI

### Accessing the UI

**URL**: http://localhost:5555

**Default View**: List of all experiments

### UI Features

#### 1. Experiments List

View all experiments and runs:

```
Experiments
├── flight-delay-prediction (ID: 1)
    ├── exp4_rf_pca_cv_15min
    ├── exp5_rf_pca_cv_30min
    ├── exp6_rf_pca_cv_45min
    └── exp7_rf_pca_cv_60min
```

#### 2. Run Comparison Table

Compare runs side-by-side:

| Run Name | test_f1 | test_accuracy | test_auc | numTrees | maxDepth | Duration |
|----------|---------|---------------|----------|----------|----------|----------|
| exp4_rf_pca_cv_15min | 0.8765 | 0.8789 | 0.9301 | 100 | 10 | 287s |
| exp5_rf_pca_cv_30min | 0.8523 | 0.8701 | 0.9187 | 100 | 10 | 295s |
| exp6_rf_pca_cv_45min | 0.8234 | 0.8534 | 0.8956 | 100 | 10 | 289s |
| exp7_rf_pca_cv_60min | 0.7956 | 0.8423 | 0.8823 | 100 | 10 | 291s |

#### 3. Run Details

Click on a run to see:

- **Parameters**: All hyperparameters and config
- **Metrics**: Performance metrics (with charts)
- **Artifacts**: Download models, CSVs, plots
- **Tags**: Metadata and descriptions
- **System Info**: Duration, start time, status

#### 4. Metric Charts

**Built-in visualizations**:

- **Metric vs Time**: Track metric evolution
- **Parallel Coordinates**: Compare high-dimensional runs
- **Scatter Plot**: Plot metric1 vs metric2
- **Contour Plot**: Visualize hyperparameter impact

#### 5. Filtering and Searching

**Filter by metrics**:
```
test_f1 > 0.85
test_auc > 0.92
```

**Filter by parameters**:
```
params.numTrees = 100
params.pca_enabled = true
```

**Sort by metric**:
```
Sort by: test_f1 ↓ (descending)
```

#### 6. Download Artifacts

**From UI**:
1. Click on run
2. Go to "Artifacts" tab
3. Click on file/directory
4. Click "Download"

**Downloaded files**:
- Trained models (.parquet)
- Metrics CSV files
- PCA analysis
- ROC curve data

---

## Best Practices

### 1. Always Log Seed

**For reproducibility**:

```scala
MLFlowTracker.logParams(runId, Map(
  "random_seed" -> 42
))
```

**Reproduce experiment**:
```yaml
common:
  seed: 42  # Same seed = same splits = same results
```

### 2. Use Descriptive Run Names

**Good**:
```
exp4_rf_pca_cv_15min
exp5_rf_pca_cv_30min_balanced
exp6_gbt_no_pca_15min
```

**Bad**:
```
run_1
test
new_experiment
```

### 3. Log Comprehensive Metadata

**Include**:
- Experiment name
- Target label
- Model type
- All hyperparameters
- Feature extraction type
- PCA settings (if enabled)
- Cross-validation settings
- Environment (local, production)

### 4. Tag Experiments

**Useful tags**:

```scala
MLFlowTracker.setTag(runId, "experiment_description", "Baseline Random Forest with PCA")
MLFlowTracker.setTag(runId, "model_version", "v1.0")
MLFlowTracker.setTag(runId, "dataset_version", "2012-01")
MLFlowTracker.setTag(runId, "git_commit", getCurrentCommitHash())
```

### 5. Compare Before and After

**Before making changes**:
1. Run baseline experiment
2. Log all metrics and params
3. Make changes (e.g., add feature, tune hyperparameters)
4. Run new experiment
5. Compare in MLflow UI side-by-side

### 6. Filter for Best Models

**Find top performers**:

```
Filters:
  test_f1 > 0.87
  test_auc > 0.92
  params.pca_enabled = true

Sort by: test_f1 ↓
```

### 7. Download and Deploy Best Model

**From MLflow UI**:
1. Find best run (highest test_f1)
2. Go to Artifacts
3. Download `models/randomforest_final/`
4. Load in production:
   ```scala
   val model = PipelineModel.load("randomforest_final")
   ```

---

## Troubleshooting

### Issue 1: "MLflow connection refused"

**Symptoms**:
```
[MLFlow] Warning: Failed to start run: Connection refused
```

**Causes**:
- MLflow server not running
- Wrong tracking URI
- Network issue

**Solutions**:

1. Check MLflow server is running:
   ```bash
   docker ps | grep mlflow
   ```

2. Check tracking URI in config:
   ```yaml
   trackingUri: "http://mlflow-server:5000"  # Use container name, not localhost
   ```

3. Test connection:
   ```bash
   docker exec spark-submit curl http://mlflow-server:5000/health
   ```

---

### Issue 2: "Experiment not showing in UI"

**Causes**:
- Experiment not created
- MLflow tracking disabled
- Run failed before logging

**Solutions**:

1. Check MLflow is enabled:
   ```yaml
   mlflow:
     enabled: true
   ```

2. Check run completed successfully:
   ```
   [MLFlow] Ended run: abc123... (status: FINISHED)
   ```

3. Refresh MLflow UI

---

### Issue 3: "Artifacts not uploaded"

**Causes**:
- Artifact path doesn't exist
- Permission issues
- Run ended before upload

**Solutions**:

1. Check artifact exists:
   ```bash
   ls /output/exp4/models/randomforest_final
   ```

2. Check logs for artifact upload:
   ```
   [MLFlow] Warning: Artifact not found: /path/to/artifact
   ```

3. Upload manually after run:
   ```scala
   MLFlowTracker.logArtifact(runId, artifactPath)
   ```

---

## Next Steps

- **[Visualization](12-visualization.md)** - Visualize MLflow results
- **[Code Reference](11-code-reference.md)** - MLFlowTracker class details
- **[Configuration](05-configuration.md)** - Configure MLflow settings

---

**MLflow Integration Guide Complete! Ready to track and compare experiments.** 📊
