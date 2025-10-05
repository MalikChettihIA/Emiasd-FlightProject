# 📈 Visualization Guide

Complete guide to visualizing experiment results, metrics, PCA analysis, and model comparisons.

---

## Table of Contents

- [Overview](#overview)
- [Visualization Scripts](#visualization-scripts)
- [PCA Visualization](#pca-visualization)
- [ML Pipeline Visualization](#ml-pipeline-visualization)
- [Experiment Comparison](#experiment-comparison)
- [MLflow UI Visualizations](#mlflow-ui-visualizations)
- [Custom Visualizations](#custom-visualizations)

---

## Overview

The system provides Python visualization scripts to analyze experiment results, PCA analysis, and compare models.

### Visualization Types

| Type | Script | Purpose |
|------|--------|---------|
| **PCA Analysis** | `visualize_pca.py` | Scree plot, biplot, variance explained |
| **ML Pipeline** | `visualize_ml_pipeline.py` | CV metrics, ROC curves, confusion matrix |
| **Experiment Comparison** | `visualize_experiments_comparison.py` | Compare multiple experiments |
| **MLflow UI** | Web interface | Interactive exploration |

### Prerequisites

**Python packages**:
```bash
pip install matplotlib seaborn pandas numpy scikit-learn plotly
```

**Or use requirements.txt**:
```bash
pip install -r requirements.txt
```

---

## Visualization Scripts

### Location

```
work/scripts/
├── visualize_pca.py                      # PCA analysis
├── visualize_ml_pipeline.py              # ML metrics
└── visualize_experiments_comparison.py   # Multi-experiment comparison
```

### General Usage

**Pattern**:
```bash
python work/scripts/<script_name>.py <input_path>
```

**Output**: PNG files saved to input directory

---

## PCA Visualization

### Script: visualize_pca.py

**Purpose**: Visualize PCA dimensionality reduction results

**Usage**:
```bash
python work/scripts/visualize_pca.py /output/{exp_name}/metrics
```

**Example**:
```bash
python work/scripts/visualize_pca.py /output/exp4_rf_pca_cv_15min/metrics
```

### Required Files

**Input files** (in metrics directory):
- `pca_variance.csv` - Variance per component
- `pca_projections.csv` - First 2 PCs (optional)
- `pca_loadings.csv` - Feature contributions (optional)

### Generated Visualizations

#### 1. Scree Plot

**File**: `pca_scree_plot.png`

**Description**: Variance explained by each principal component

**Interpretation**:
- **X-axis**: Principal component number (1, 2, 3, ...)
- **Y-axis**: Explained variance (%)
- **Curve**: Look for "elbow" where variance drops sharply
- **Horizontal line**: Selected variance threshold (e.g., 70%)

**Example**:
```
     Variance
       |
  15% -|■
  12% -| ■
  10% -|  ■
   8% -|   ■
   6% -|    ■
   4% -|     ■■
   2% -|       ■■■■
       |_____________ PC
        1 2 3 4 5 6 7
        └─┬─┘
       Elbow at PC3
```

**When to Use**:
- Determine optimal number of components
- Identify elbow point
- Validate variance threshold selection

---

#### 2. Cumulative Variance Plot

**File**: `pca_cumulative_variance.png`

**Description**: Cumulative variance explained by top K components

**Interpretation**:
- **X-axis**: Number of components (1, 2, ..., K)
- **Y-axis**: Cumulative variance (0% to 100%)
- **Curve**: Rises quickly then plateaus
- **Threshold line**: Target variance (e.g., 70%)
- **Selected K**: Where curve crosses threshold

**Example**:
```
  Cumulative Variance
       |
 100% -|          ________
  90% -|        /
  80% -|      /
  70% -|    /--- (Threshold, K=9)
  60% -|   /
  50% -|  /
  40% -| /
  30% -|/
       |_____________ Components
        0 2 4 6 8 10 12
```

**When to Use**:
- Validate component selection
- Check if threshold is met
- Decide if more/fewer components needed

---

#### 3. Biplot (First 2 PCs)

**File**: `pca_biplot.png`

**Description**: Data points projected onto first 2 principal components

**Interpretation**:
- **Points**: Individual flights
- **Color**: Label (delayed vs on-time)
- **Clusters**: Separated classes = good discrimination
- **Overlap**: Mixed classes = harder to predict

**Example**:
```
      PC2
       |
       |    ● ●
       |  ●   ●
    +  |● ● ○ ○○
       |  ○ ○
       |○ ○
       |________ PC1
              +

● = Delayed flights
○ = On-time flights
```

**When to Use**:
- Visualize class separation
- Identify outliers
- Understand data structure

---

### PCA Visualization Example

**Command**:
```bash
python work/scripts/visualize_pca.py /output/exp4_rf_pca_cv_15min/metrics
```

**Output**:
```
Loading PCA variance data...
✓ Loaded 12 components

Generating visualizations:
  1. Scree plot... ✓
  2. Cumulative variance... ✓
  3. Biplot (if projection data available)... ✓

Saved visualizations to:
  - /output/exp4_rf_pca_cv_15min/metrics/pca_scree_plot.png
  - /output/exp4_rf_pca_cv_15min/metrics/pca_cumulative_variance.png
  - /output/exp4_rf_pca_cv_15min/metrics/pca_biplot.png
```

---

## ML Pipeline Visualization

### Script: visualize_ml_pipeline.py

**Purpose**: Visualize ML training metrics and evaluation results

**Usage**:
```bash
python work/scripts/visualize_ml_pipeline.py /output/{exp_name}/metrics
```

**Example**:
```bash
python work/scripts/visualize_ml_pipeline.py /output/exp4_rf_pca_cv_15min/metrics
```

### Required Files

**Input files**:
- `cv_fold_metrics.csv` - Per-fold CV metrics
- `cv_summary.csv` - CV mean ± std
- `holdout_test_metrics.csv` - Test set metrics
- `holdout_roc_data.csv` - ROC curve data

### Generated Visualizations

#### 1. Cross-Validation Metrics

**File**: `cv_metrics_per_fold.png`

**Description**: Box plots of metrics across CV folds

**Interpretation**:
- **Box**: Interquartile range (IQR)
- **Line**: Median
- **Whiskers**: Min/max (excluding outliers)
- **Narrow box**: Stable model
- **Wide box**: High variance across folds

**Metrics Displayed**:
- Accuracy
- Precision
- Recall
- F1-Score
- AUC-ROC

---

#### 2. CV vs Test Metrics

**File**: `cv_vs_test_comparison.png`

**Description**: Bar chart comparing CV and test performance

**Interpretation**:
- **Blue bars**: CV mean
- **Orange bars**: Test set
- **Error bars**: CV std dev
- **Close bars**: Good generalization
- **Test < CV**: Possible overfitting

**Example**:
```
Metrics      CV       Test
Accuracy   87.3%    87.9%  ✓ Good
Precision  85.7%    86.1%  ✓ Good
Recall     88.5%    89.2%  ✓ Good
F1-Score   87.0%    87.7%  ✓ Good
AUC-ROC    0.923    0.930  ✓ Good
```

---

#### 3. ROC Curve

**File**: `roc_curve.png`

**Description**: Receiver Operating Characteristic curve

**Interpretation**:
- **X-axis**: False Positive Rate (FPR)
- **Y-axis**: True Positive Rate (TPR = Recall)
- **Curve**: Model's classification performance
- **Diagonal line**: Random classifier (AUC = 0.5)
- **Area under curve**: AUC-ROC score
- **Closer to top-left**: Better model

**Example**:
```
  TPR
   1.0 |    /--------
       |   /
   0.8 |  /
       | /
   0.6 |/
       |
   0.4 |
       |    (Random)
   0.2 |  /
       |/
   0.0 |____________ FPR
      0.0  0.4  0.8  1.0

AUC = 0.93 (Excellent)
```

---

#### 4. Confusion Matrix Heatmap

**File**: `confusion_matrix.png`

**Description**: Heatmap of true vs predicted labels

**Interpretation**:
- **Rows**: Actual labels
- **Columns**: Predicted labels
- **Diagonal**: Correct predictions
- **Off-diagonal**: Errors

**Example**:
```
                Predicted
                0        1
Actual  0   [20000]   [2500]
        1    [800]    [4700]

Accuracy = (20000 + 4700) / 28000 = 88.2%
```

---

### ML Pipeline Visualization Example

**Command**:
```bash
python work/scripts/visualize_ml_pipeline.py /output/exp4_rf_pca_cv_15min/metrics
```

**Output**:
```
Loading metrics data...
✓ Loaded CV fold metrics (5 folds)
✓ Loaded test metrics
✓ Loaded ROC data (10,000 samples)

Generating visualizations:
  1. CV metrics per fold... ✓
  2. CV vs Test comparison... ✓
  3. ROC curve (AUC=0.930)... ✓
  4. Confusion matrix... ✓

Saved visualizations to:
  - cv_metrics_per_fold.png
  - cv_vs_test_comparison.png
  - roc_curve.png
  - confusion_matrix.png
```

---

## Experiment Comparison

### Script: visualize_experiments_comparison.py

**Purpose**: Compare multiple experiments side-by-side

**Usage**:
```bash
python work/scripts/visualize_experiments_comparison.py /output
```

**Example**:
```bash
python work/scripts/visualize_experiments_comparison.py /output
```

### How It Works

1. **Scans** `/output/` for experiment directories
2. **Loads** metrics from each experiment
3. **Compares** test metrics across experiments
4. **Generates** comparison visualizations

### Required Structure

```
/output/
├── exp4_rf_pca_cv_15min/
│   └── metrics/
│       └── holdout_test_metrics.csv
├── exp5_rf_pca_cv_30min/
│   └── metrics/
│       └── holdout_test_metrics.csv
├── exp6_rf_pca_cv_45min/
│   └── metrics/
│       └── holdout_test_metrics.csv
└── exp7_rf_pca_cv_60min/
    └── metrics/
        └── holdout_test_metrics.csv
```

### Generated Visualizations

#### 1. Metrics Comparison Heatmap

**File**: `experiments_comparison_heatmap.png`

**Description**: Heatmap of metrics across experiments

**Interpretation**:
- **Rows**: Experiments
- **Columns**: Metrics
- **Color**: Performance (green=high, red=low)
- **Best**: Highest in each column

**Example**:
```
Experiment           Accuracy  Precision  Recall  F1     AUC
exp4_15min           87.9%     86.1%      89.2%   87.7%  0.93
exp5_30min           87.0%     85.2%      88.9%   85.2%  0.92
exp6_45min           85.3%     83.4%      86.7%   82.3%  0.90
exp7_60min           84.2%     82.1%      85.1%   80.0%  0.88
```

---

#### 2. Metrics Comparison Bar Chart

**File**: `experiments_comparison_bars.png`

**Description**: Grouped bar chart of metrics

**Interpretation**:
- **Groups**: Experiments
- **Bars**: Metrics
- **Height**: Performance
- **Compare**: Which experiment performs best

---

#### 3. ROC Curves Overlay

**File**: `experiments_roc_comparison.png`

**Description**: Multiple ROC curves on same plot

**Interpretation**:
- **Curves**: One per experiment
- **Legend**: Experiment name + AUC
- **Compare**: Which model discriminates best

---

### Experiment Comparison Example

**Command**:
```bash
python work/scripts/visualize_experiments_comparison.py /output
```

**Output**:
```
Scanning experiments in /output...
✓ Found 4 experiments:
  - exp4_rf_pca_cv_15min (F1=87.7%, AUC=0.930)
  - exp5_rf_pca_cv_30min (F1=85.2%, AUC=0.919)
  - exp6_rf_pca_cv_45min (F1=82.3%, AUC=0.896)
  - exp7_rf_pca_cv_60min (F1=80.0%, AUC=0.882)

Generating comparisons:
  1. Metrics heatmap... ✓
  2. Bar chart... ✓
  3. ROC curves overlay... ✓

Saved visualizations to:
  - /output/experiments_comparison_heatmap.png
  - /output/experiments_comparison_bars.png
  - /output/experiments_roc_comparison.png

Best performer: exp4_rf_pca_cv_15min
  F1-Score: 87.7%
  AUC-ROC: 0.930
```

---

## MLflow UI Visualizations

### Accessing MLflow UI

**URL**: http://localhost:5555

**Features**:
- Interactive experiment comparison
- Metric time series
- Parallel coordinates plot
- Parameter importance
- Artifact browser

### Built-in Visualizations

#### 1. Metric Comparison Table

**View**: Experiments → Table

**Features**:
- Sort by any metric
- Filter runs
- Search by parameter
- Compare multiple runs

**Example Filter**:
```
test_f1 > 0.85
AND params.pca_enabled = "true"
```

---

#### 2. Parallel Coordinates Plot

**View**: Experiments → Chart → Parallel Coordinates

**Purpose**: Visualize high-dimensional hyperparameter space

**Features**:
- Each line = one run
- Each axis = one parameter/metric
- Color = selected metric
- Identify optimal parameter ranges

---

#### 3. Scatter Plot

**View**: Experiments → Chart → Scatter Plot

**Purpose**: Explore parameter-metric relationships

**Example**:
```
X-axis: params.numTrees
Y-axis: metrics.test_f1
Color: metrics.test_auc
Size: metrics.training_time_seconds
```

---

#### 4. Contour Plot

**View**: Experiments → Chart → Contour

**Purpose**: Visualize metric surface over 2 parameters

**Example**:
```
X-axis: params.numTrees
Y-axis: params.maxDepth
Z-axis (color): metrics.test_f1
```

---

## Custom Visualizations

### Creating Custom Plots

**Using Python**:

```python
import pandas as pd
import matplotlib.pyplot as plt

# Load metrics
cv_metrics = pd.read_csv("/output/exp4/metrics/cv_fold_metrics.csv")
test_metrics = pd.read_csv("/output/exp4/metrics/holdout_test_metrics.csv")

# Custom plot
plt.figure(figsize=(10, 6))
plt.bar(cv_metrics["fold"], cv_metrics["f1_score"])
plt.xlabel("Fold")
plt.ylabel("F1-Score")
plt.title("F1-Score by Cross-Validation Fold")
plt.savefig("custom_cv_plot.png")
```

---

### Common Custom Visualizations

#### 1. Training Time vs Performance

```python
# Load multiple experiments
experiments = ["exp4", "exp5", "exp6", "exp7"]
times = []
f1_scores = []

for exp in experiments:
    metrics = pd.read_csv(f"/output/{exp}/metrics/holdout_test_metrics.csv")
    times.append(metrics["training_time_seconds"])
    f1_scores.append(metrics["f1_score"])

plt.scatter(times, f1_scores)
plt.xlabel("Training Time (seconds)")
plt.ylabel("F1-Score")
plt.title("Training Time vs Performance")
```

---

#### 2. Hyperparameter Heatmap

```python
# For grid search results
import seaborn as sns

# Load grid search results
results = pd.read_csv("grid_search_results.csv")
pivot = results.pivot("numTrees", "maxDepth", "test_f1")

sns.heatmap(pivot, annot=True, fmt=".3f", cmap="YlGnBu")
plt.xlabel("Max Depth")
plt.ylabel("Num Trees")
plt.title("F1-Score Heatmap (Grid Search)")
```

---

#### 3. Feature Importance

```python
# Load feature importance
importance = pd.read_csv("/output/exp4/metrics/feature_importance.csv")

# Top 20 features
top20 = importance.nlargest(20, "importance")

plt.barh(top20["feature_name"], top20["importance"])
plt.xlabel("Importance")
plt.title("Top 20 Feature Importance")
plt.tight_layout()
```

---

## Best Practices

### 1. Generate Visualizations After Each Experiment

**Automate**:
```bash
#!/bin/bash
# run_and_visualize.sh

cd docker
./submit.sh

# Wait for completion
python ../work/scripts/visualize_pca.py /output/exp4_rf_pca_cv_15min/metrics
python ../work/scripts/visualize_ml_pipeline.py /output/exp4_rf_pca_cv_15min/metrics
```

---

### 2. Compare Before and After Changes

**Workflow**:
1. Run baseline experiment
2. Generate visualizations
3. Make changes (e.g., tune hyperparameters)
4. Run new experiment
5. Generate comparison visualizations

---

### 3. Use MLflow for Interactive Exploration

**Advantages**:
- No coding required
- Interactive filtering
- Real-time updates
- Shareable URLs

---

### 4. Save Visualizations with Git

**Track visualizations**:
```bash
git add work/output/exp4/metrics/*.png
git commit -m "Add visualizations for exp4"
```

---

### 5. Document Insights

**Create analysis report**:

```markdown
# Experiment 4 Analysis

## PCA Insights
- Scree plot shows elbow at PC3
- Selected 12 components explain 71.2% variance
- First 2 PCs show good class separation

## Performance
- CV F1: 87.0% ± 1.6% (stable)
- Test F1: 87.7% (good generalization)
- AUC: 0.930 (excellent discrimination)

## Conclusion
Baseline model performs well. Next: Try GBT.
```

---

## Next Steps

- **[MLflow Integration](09-mlflow-integration.md)** - Learn MLflow features
- **[Configuration](05-configuration.md)** - Configure experiments
- **[Code Reference](11-code-reference.md)** - Understand metrics classes

---

**Visualization Guide Complete! Ready to analyze and visualize experiment results.** 📈
