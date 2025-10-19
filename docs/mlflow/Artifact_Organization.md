# MLFlow Artifact Organization

## Overview

This document describes how artifacts are organized in MLFlow for better clarity and navigation in the MLFlow UI.

## Artifact Structure

All artifacts are now organized into logical subdirectories within each MLFlow run:

```
mlflow/artifacts/{experiment_id}/{run_id}/artifacts/
├── configuration/              # Experiment configuration
│   └── local-config.yml       # YAML configuration used for this run
│
├── metrics/                    # Training and evaluation metrics
│   ├── cv_fold_metrics.csv    # Per-fold cross-validation metrics
│   ├── cv_summary.csv          # Aggregated CV metrics (mean ± std)
│   ├── holdout_test_metrics.csv  # Final test set metrics
│   ├── best_hyperparameters.csv  # Best hyperparameters from grid search
│   └── holdout_roc_data.csv   # ROC curve data (label, probability, prediction)
│
└── models/                     # Trained models
    └── randomforest_final/    # Final model (Spark ML Pipeline)
        ├── metadata/
        ├── stages/
        └── data/
```

## Benefits

### 1. Better Organization in MLFlow UI

Instead of having all artifacts at the root level:

**Before**:
```
artifacts/
├── cv_fold_metrics.csv
├── cv_summary.csv
├── holdout_test_metrics.csv
├── best_hyperparameters.csv
├── holdout_roc_data.csv
└── randomforest_final/
```

**After** (organized by type):
```
artifacts/
├── configuration/
│   └── local-config.yml
├── metrics/
│   ├── cv_fold_metrics.csv
│   ├── cv_summary.csv
│   ├── holdout_test_metrics.csv
│   ├── best_hyperparameters.csv
│   └── holdout_roc_data.csv
└── models/
    └── randomforest_final/
```

### 2. Easy Navigation

- **Configuration**: Quickly see what settings were used
- **Metrics**: All evaluation metrics grouped together
- **Models**: All model artifacts in one place

### 3. Reproducibility

The YAML configuration file is logged, allowing you to:
- ✅ See exact hyperparameters used
- ✅ Reproduce the experiment with same settings
- ✅ Compare configurations across runs
- ✅ Track changes in feature extraction settings

## Implementation

### Code Changes

#### 1. MLFlowTracker - New Method

Added `logArtifactWithPath()` method to support subdirectories:

```scala
def logArtifactWithPath(runId: String, localPath: String, artifactPath: String): Unit
```

**File**: `src/main/scala/com/flightdelay/ml/tracking/MLFlowTracker.scala`

#### 2. MLPipeline - Organized Logging

Updated artifact logging in MLPipeline:

```scala
// 1. Log metrics CSVs to "metrics/" subdirectory
MLFlowTracker.logArtifactWithPath(rid, metricsPath, "metrics")

// 2. Log model to "models/" subdirectory
MLFlowTracker.logArtifactWithPath(rid, modelPath, "models")

// 3. Log YAML configuration to "configuration/" subdirectory
MLFlowTracker.logArtifactWithPath(rid, configDestPath, "configuration")
```

**File**: `src/main/scala/com/flightdelay/ml/MLPipeline.scala` (lines 289-318)

## Configuration Logging

### What is Logged

The complete YAML configuration file used for the run is saved to MLFlow:

```yaml
environment: "local"

common:
  seed: 42
  data:
    basePath: "/data"
    flight:
      path: "/data/FLIGHT-3Y/Flights/2012*.csv"
  # ... all configuration ...

experiments:
  - name: "Experience-2-local-all-data"
    model:
      modelType: "randomforest"
      hyperparameters:
        numTrees: [20]
        maxDepth: [7]
    # ... complete experiment config ...
```

### Why This is Important

**Reproducibility**:
- Exact settings used for data loading
- Feature extraction configuration
- Model hyperparameters
- Cross-validation settings

**Comparison**:
- Compare configurations across runs
- Identify what changed between experiments
- Track evolution of your experiments

**Documentation**:
- Self-documenting experiments
- No need to remember what settings were used
- Easy to share with team members

## Usage in MLFlow UI

### Viewing Artifacts

1. **Navigate to Run**:
   - Open MLFlow UI: http://localhost:5555
   - Click on your experiment
   - Select a run

2. **View Artifacts**:
   - Click "Artifacts" tab
   - See organized folders: `configuration/`, `metrics/`, `models/`

3. **Download/View**:
   - Click on any file to preview
   - Download individual files or entire folders

### Example Workflow

**Scenario**: Compare two model configurations

1. Run 1: `numTrees: 15, maxDepth: 7`
2. Run 2: `numTrees: 20, maxDepth: 7`

**Steps**:
1. View both runs in MLFlow UI
2. Open `configuration/local-config.yml` for each
3. Compare hyperparameters side-by-side
4. Check `metrics/cv_summary.csv` for performance
5. Download best performing config

## Metrics Files Description

| File | Description | Use Case |
|------|-------------|----------|
| `cv_fold_metrics.csv` | Per-fold metrics (accuracy, precision, recall, F1, AUC) | Analyze variance across folds |
| `cv_summary.csv` | Mean ± Std for each metric | Quick performance overview |
| `holdout_test_metrics.csv` | Final test set evaluation | Production performance estimate |
| `best_hyperparameters.csv` | Optimal hyperparameters from grid search | Model configuration |
| `holdout_roc_data.csv` | ROC curve data points | Plot ROC curves |

## Model Artifacts Description

The `models/` directory contains the complete Spark ML Pipeline:

```
models/randomforest_final/
├── metadata/              # Pipeline metadata
│   ├── part-00000         # Metadata JSON
│   └── _SUCCESS
├── stages/                # Pipeline stages (transformers + classifier)
│   └── 0_rfc_*/          # RandomForestClassifier
│       ├── data/         # Model weights
│       ├── metadata/     # Model config
│       └── treesMetadata/ # Tree structures
└── data/                 # Additional data
```

**This allows**:
- Loading the model for inference
- Inspecting model structure
- Extracting feature importances
- Deploying to production

## Backward Compatibility

### Previous Runs

Old runs (before this change) will still have artifacts at root level. They remain accessible but won't have the new organized structure.

### Migration

Not necessary. The new structure applies only to new runs going forward.

## Best Practices

### 1. Always Log Configuration

Even if you don't make changes, log the config:
- Tracks what was used
- Enables exact reproduction
- Documents experiment settings

### 2. Review Configuration Before Training

Check `configuration/` in UI after first run to ensure correct settings were captured.

### 3. Use Configuration for Comparison

When debugging performance differences:
1. Compare `metrics/` first
2. Then check `configuration/` for differences
3. Identify what changed

### 4. Download Configuration for Reproduction

To reproduce a good result:
1. Find the run in MLFlow
2. Download `configuration/*.yml`
3. Use it as your new config file
4. Re-run training

## Technical Notes

### Configuration Extraction Process

1. **Load from Resources**: Configuration is loaded from classpath (`{environment}-config.yml`)
2. **Copy to Experiment Output**: Saved to `work/output/{experiment_name}/configuration/`
3. **Log to MLFlow**: Uploaded to `artifacts/configuration/`

### File Handling

- **Directories**: Recursively uploaded with all contents
- **Single Files**: Uploaded as-is
- **Error Handling**: Missing files logged as warnings (non-blocking)

## Troubleshooting

### Configuration Not Appearing

**Check**:
```bash
# Verify config exists in resources
ls src/main/resources/local-config.yml

# Check experiment output
ls work/output/Experience-*/configuration/

# Check MLFlow artifacts
docker exec mlflow-server ls /mlflow/artifacts/1/{run_id}/artifacts/configuration/
```

### Metrics Not in Subdirectory

**Ensure** you've recompiled after code changes:
```bash
sbt clean compile package
```

### Old Artifact Structure Still Showing

This is expected for runs created before the update. New runs will use the new structure.

## Examples

### Viewing Configuration in MLFlow UI

```
Run: Experience-2-local-all-data (2025-10-19 19:00)
├── Artifacts
    ├── configuration/
    │   └── local-config.yml         ← Click to view YAML
    ├── metrics/
    │   ├── cv_fold_metrics.csv      ← Cross-validation details
    │   ├── cv_summary.csv           ← Performance overview
    │   ├── holdout_test_metrics.csv ← Final test results
    │   ├── best_hyperparameters.csv ← Best settings found
    │   └── holdout_roc_data.csv     ← ROC curve data
    └── models/
        └── randomforest_final/      ← Complete trained model
```

## References

- [MLFlow Artifacts Documentation](https://mlflow.org/docs/latest/tracking.html#artifact-stores)
- [Spark ML Pipeline Persistence](https://spark.apache.org/docs/latest/ml-pipeline.html#ml-persistence-saving-and-loading-pipelines)

## Changelog

### 2025-10-19
- ✅ Added `logArtifactWithPath()` method to MLFlowTracker
- ✅ Organized metrics into `metrics/` subdirectory
- ✅ Organized models into `models/` subdirectory
- ✅ Added configuration logging to `configuration/` subdirectory
- ✅ Updated MLPipeline to use new organization
- ✅ Created documentation
