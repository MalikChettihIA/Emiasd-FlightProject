# 🤖 ML Pipeline Guide

Complete guide to model training, cross-validation, grid search, and evaluation in the Flight Delay Prediction system.

---

## Table of Contents

- [Overview](#overview)
- [Pipeline Architecture](#pipeline-architecture)
- [Training Strategy](#training-strategy)
- [Cross-Validation](#cross-validation)
- [Grid Search](#grid-search)
- [Model Training](#model-training)
- [Model Evaluation](#model-evaluation)
- [Metrics and Artifacts](#metrics-and-artifacts)
- [Best Practices](#best-practices)

---

## Overview

The ML Pipeline orchestrates model training, cross-validation, hyperparameter tuning, and evaluation to produce production-ready models.

### ML Pipeline Objectives

1. **Split** data into development (80%) and hold-out test (20%) sets
2. **Cross-validate** on development set with K-fold CV
3. **Tune** hyperparameters with grid search (optional)
4. **Train** final model on full development set with best parameters
5. **Evaluate** on hold-out test set for unbiased performance estimate
6. **Track** all experiments, parameters, and metrics in MLflow
7. **Save** trained models and metrics for deployment

### Key Characteristics

- **Option B Strategy**: Hold-out test + K-fold CV on dev set
- **Unbiased Evaluation**: Test set never used for training or tuning
- **Grid Search**: Automatic hyperparameter optimization
- **MLflow Integration**: All experiments tracked and versioned

---

## Pipeline Architecture

### High-Level Flow

```
┌────────────────────────────────────────────────────────────────┐
│ INPUT: Extracted Features                                      │
├────────────────────────────────────────────────────────────────┤
│ Load from: /output/{exp_name}/features/extracted_features/     │
│ Schema: (features: Vector, label: Double)                      │
│ Count: ~140K flights                                            │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STEP 1: Initial Split (Hold-out Strategy)                      │
├────────────────────────────────────────────────────────────────┤
│ randomSplit([0.8, 0.2], seed=42)                               │
│   ↓                                                             │
│ Development Set: 112K flights (80%)                            │
│ Hold-out Test:   28K flights (20%)                             │
│                                                                 │
│ ⚠ Test set LOCKED (never used until final evaluation)          │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STEP 2: K-Fold Cross-Validation on Dev Set                     │
├────────────────────────────────────────────────────────────────┤
│ CrossValidator.validate(devData, experiment)                    │
│   ↓                                                             │
│ FOR each fold (1 to K=5):                                       │
│   • Split dev set: 80% train, 20% validation                   │
│   • IF grid search enabled:                                     │
│       FOR each hyperparameter combination:                      │
│         • Train model on train split                            │
│         • Evaluate on validation split                          │
│       SELECT best params by evaluation metric                   │
│   • ELSE:                                                       │
│       • Train model with default params                         │
│       • Evaluate on validation split                            │
│   • Store fold metrics                                          │
│                                                                 │
│ Aggregate metrics: mean ± std across folds                      │
│ Output: Best hyperparameters, per-fold metrics, avg metrics    │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STEP 3: Train Final Model on Full Dev Set                      │
├────────────────────────────────────────────────────────────────┤
│ Trainer.trainFinal(devData, experiment, bestParams)            │
│   ↓                                                             │
│ • Use ALL 112K dev samples                                      │
│ • Use best hyperparameters from CV                              │
│ • Train Random Forest model                                     │
│   ↓                                                             │
│ Output: Trained PipelineModel                                   │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STEP 4: Final Evaluation on Hold-out Test Set                  │
├────────────────────────────────────────────────────────────────┤
│ predictions = finalModel.transform(testData)                    │
│ holdOutMetrics = ModelEvaluator.evaluate(predictions)           │
│   ↓                                                             │
│ • Accuracy, Precision, Recall, F1, AUC                          │
│ • Confusion Matrix (TP, TN, FP, FN)                             │
│ • ROC curve data                                                │
│   ↓                                                             │
│ Output: EvaluationMetrics                                       │
└────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────┐
│ STEP 5: Save Model and Metrics                                 │
├────────────────────────────────────────────────────────────────┤
│ • Save model: /output/{exp_name}/models/randomforest_final/    │
│ • Save CV metrics: /output/{exp_name}/metrics/cv_*.csv         │
│ • Save test metrics: /output/{exp_name}/metrics/holdout_*.csv  │
│ • Log to MLflow: params, metrics, artifacts                    │
└────────────────────────────────────────────────────────────────┘
```

---

## Training Strategy

### Option B: Hold-out Test + K-Fold CV

**Rationale**: Unbiased performance estimate with robust hyperparameter tuning

**Process**:

1. **Initial Split** (80/20):
   - Development set (80%): Used for training and validation
   - Hold-out test (20%): LOCKED until final evaluation

2. **K-Fold CV on Dev** (5 folds):
   - Tune hyperparameters
   - Get robust performance estimate
   - Each fold: 80% train, 20% validation

3. **Final Training on Full Dev**:
   - Train with best params from CV
   - Use ALL dev data (no waste)

4. **Final Evaluation on Test**:
   - Unbiased performance (test never seen during training/tuning)
   - Realistic estimate of production performance

### Why This Strategy?

| Aspect | Benefit |
|--------|---------|
| **Unbiased Evaluation** | Test set never used for tuning |
| **Robust Tuning** | K-fold CV averages out variance |
| **No Data Waste** | Final model trained on full dev set |
| **Production-Ready** | Test performance = expected production performance |

---

## Cross-Validation

### CrossValidator

**Class**: `com.flightdelay.ml.training.CrossValidator`

**Purpose**: K-fold cross-validation with optional grid search

### K-Fold Split

**Configuration**:
```yaml
train:
  crossValidation:
    numFolds: 5
```

**Behavior**:
```
Dev Set (112K samples)

Fold 1: Train[2,3,4,5] (89.6K), Val[1] (22.4K)
Fold 2: Train[1,3,4,5] (89.6K), Val[2] (22.4K)
Fold 3: Train[1,2,4,5] (89.6K), Val[3] (22.4K)
Fold 4: Train[1,2,3,5] (89.6K), Val[4] (22.4K)
Fold 5: Train[1,2,3,4] (89.6K), Val[5] (22.4K)
```

**For each fold**:
1. Train model on train split (80% of dev)
2. Evaluate on validation split (20% of dev)
3. Compute metrics (accuracy, precision, recall, F1, AUC)

**Aggregation**:
```
Mean ± Std across folds:
  Accuracy:  87.32% ± 1.23%
  Precision: 85.67% ± 2.10%
  Recall:    88.45% ± 1.87%
  F1-Score:  87.02% ± 1.56%
  AUC-ROC:   0.9234 ± 0.0156
```

### Cross-Validation Output

**CVResult**:
```scala
case class CVResult(
  foldMetrics: Seq[EvaluationMetrics],   // Per-fold metrics
  avgMetrics: EvaluationMetrics,         // Mean across folds
  stdMetrics: EvaluationMetrics,         // Std dev across folds
  bestHyperparameters: Map[String, Any], // Best params from grid search
  numFolds: Int                          // Number of folds
)
```

**Interpretation**:
- **Low std dev** (< 2%) → Stable model, generalizes well
- **High std dev** (> 5%) → Unstable, consider more data or simpler model

---

## Grid Search

### Grid Search Configuration

```yaml
train:
  gridSearch:
    enabled: true
    evaluationMetric: "f1"

  hyperparameters:
    numTrees: [50, 100, 200]
    maxDepth: [5, 10, 15]
```

**Grid**:
```
(numTrees=50,  maxDepth=5)  → 9 combinations
(numTrees=50,  maxDepth=10)
(numTrees=50,  maxDepth=15)
(numTrees=100, maxDepth=5)
(numTrees=100, maxDepth=10)
(numTrees=100, maxDepth=15)
(numTrees=200, maxDepth=5)
(numTrees=200, maxDepth=10)
(numTrees=200, maxDepth=15)
```

### Grid Search Process

**For each fold**:
```
FOR each hyperparameter combination:
  1. Train model on train split
  2. Evaluate on validation split
  3. Compute evaluation metric (F1)

SELECT best combination by average F1 across folds
```

**Example**:
```
Fold 1 Results:
  (numTrees=50, maxDepth=5):   F1=0.85
  (numTrees=100, maxDepth=10): F1=0.87  ← Best for fold 1
  (numTrees=200, maxDepth=15): F1=0.86

Fold 2 Results:
  (numTrees=50, maxDepth=5):   F1=0.84
  (numTrees=100, maxDepth=10): F1=0.88  ← Best for fold 2
  (numTrees=200, maxDepth=15): F1=0.87

... (folds 3, 4, 5)

Average F1 across all folds:
  (numTrees=50, maxDepth=5):   0.846
  (numTrees=100, maxDepth=10): 0.870  ← Best overall
  (numTrees=200, maxDepth=15): 0.865

Selected: numTrees=100, maxDepth=10
```

### Evaluation Metrics

| Metric | When to Use |
|--------|-------------|
| **f1** | Imbalanced datasets (recommended) |
| **accuracy** | Balanced datasets |
| **auc** | Ranking performance important |

**Default**: `f1` (best for flight delay prediction)

### Total Models Trained

**Calculation**: `combinations × folds`

**Examples**:
```
Grid: [50, 100] × [5, 10] = 4 combinations
Folds: 5
Total: 4 × 5 = 20 models trained

Grid: [50, 100, 200] × [5, 10, 15] = 9 combinations
Folds: 5
Total: 9 × 5 = 45 models trained
```

**Training Time**:
```
Time per model: ~30 seconds
Total time (9×5): ~22 minutes
```

---

## Model Training

### Trainer

**Class**: `com.flightdelay.ml.training.Trainer`

**Purpose**: Train final model on full dev set with best params

### Training Process

```scala
def trainFinal(
  devData: DataFrame,
  experiment: ExperimentConfig,
  bestHyperparameters: Map[String, Any]
): Transformer = {
  // 1. Create model with best hyperparameters
  val model = ModelFactory.createModel(
    modelType = experiment.model.modelType,
    hyperparameters = bestHyperparameters
  )

  // 2. Train on FULL dev set (80% of total data)
  val trainedModel = model.train(devData)

  // 3. Return trained model
  trainedModel
}
```

**Key Points**:
- Uses **ALL** dev data (no holdout from dev)
- Uses **best hyperparameters** from CV
- Returns Spark ML `PipelineModel`

### ModelFactory

**Class**: `com.flightdelay.ml.models.ModelFactory`

**Purpose**: Create models dynamically based on configuration

```scala
def createModel(
  modelType: String,
  hyperparameters: HyperparametersConfig
): MLModel = {
  modelType match {
    case "randomforest" => new RandomForestModel(hyperparameters)
    case _ => throw new IllegalArgumentException(s"Unknown model type: $modelType")
  }
}
```

**Extensibility**: Easy to add new models (GBT, LogisticRegression, etc.)

### RandomForestModel

**Class**: `com.flightdelay.ml.models.RandomForestModel`

**Hyperparameters** (from config):
```scala
val rf = new RandomForestClassifier()
  .setNumTrees(hyperparameters.numTrees)
  .setMaxDepth(hyperparameters.maxDepth)
  .setMaxBins(hyperparameters.maxBins)
  .setMinInstancesPerNode(hyperparameters.minInstancesPerNode)
  .setSubsamplingRate(hyperparameters.subsamplingRate)
  .setFeatureSubsetStrategy(hyperparameters.featureSubsetStrategy)
  .setImpurity(hyperparameters.impurity)
  .setFeaturesCol("features")
  .setLabelCol("label")
```

**Training**:
```scala
val model = rf.fit(data)
```

---

## Model Evaluation

### ModelEvaluator

**Class**: `com.flightdelay.ml.evaluation.ModelEvaluator`

**Purpose**: Compute comprehensive evaluation metrics

### Evaluation Metrics

```scala
def evaluate(predictions: DataFrame): EvaluationMetrics = {
  // Binary classification evaluators
  val accuracyEval = new MulticlassClassificationEvaluator()
    .setMetricName("accuracy")

  val precisionEval = new MulticlassClassificationEvaluator()
    .setMetricName("weightedPrecision")

  val recallEval = new MulticlassClassificationEvaluator()
    .setMetricName("weightedRecall")

  val f1Eval = new MulticlassClassificationEvaluator()
    .setMetricName("f1")

  val aucEval = new BinaryClassificationEvaluator()
    .setMetricName("areaUnderROC")

  // Compute metrics
  EvaluationMetrics(
    accuracy = accuracyEval.evaluate(predictions),
    precision = precisionEval.evaluate(predictions),
    recall = recallEval.evaluate(predictions),
    f1Score = f1Eval.evaluate(predictions),
    areaUnderROC = aucEval.evaluate(predictions),
    ...
  )
}
```

### Metrics Explained

#### Accuracy

**Formula**: `(TP + TN) / (TP + TN + FP + FN)`

**Interpretation**: Overall correctness

**Example**: 87.89% of predictions are correct

**When to Use**: Balanced datasets

---

#### Precision

**Formula**: `TP / (TP + FP)`

**Interpretation**: When we predict delay, how often is it correct?

**Example**: 86.12% → When we predict a flight is delayed, we're correct 86% of the time

**When to Use**: False positives costly (e.g., unnecessary alerts)

---

#### Recall

**Formula**: `TP / (TP + FN)`

**Interpretation**: Of all actual delays, how many did we catch?

**Example**: 89.23% → We catch 89% of all delayed flights

**When to Use**: False negatives costly (e.g., missing critical delays)

---

#### F1-Score

**Formula**: `2 × (Precision × Recall) / (Precision + Recall)`

**Interpretation**: Harmonic mean of precision and recall

**Example**: 87.65% → Balanced performance

**When to Use**: Imbalanced datasets (recommended)

---

#### AUC-ROC

**Formula**: Area under Receiver Operating Characteristic curve

**Interpretation**: Model's ability to distinguish classes

**Range**: 0.5 (random) to 1.0 (perfect)

**Example**: 0.9301 → Excellent discrimination

**When to Use**: Ranking performance important

---

### Confusion Matrix

```scala
// Compute confusion matrix
val tp = predictions.filter(col("label") === 1 && col("prediction") === 1).count()
val tn = predictions.filter(col("label") === 0 && col("prediction") === 0).count()
val fp = predictions.filter(col("label") === 0 && col("prediction") === 1).count()
val fn = predictions.filter(col("label") === 1 && col("prediction") === 0).count()
```

**Example**:
```
                 Predicted
                 0       1
Actual  0    TN=20,000  FP=2,500
        1    FN=800     TP=4,700

True Positives:  4,700 (correctly predicted delays)
True Negatives:  20,000 (correctly predicted on-time)
False Positives: 2,500 (predicted delay, but on-time)
False Negatives: 800 (predicted on-time, but delayed)

Accuracy:  (20000 + 4700) / 28000 = 88.2%
Precision: 4700 / (4700 + 2500) = 65.3%
Recall:    4700 / (4700 + 800) = 85.5%
F1:        2 × (0.653 × 0.855) / (0.653 + 0.855) = 74.0%
```

---

## Metrics and Artifacts

### Saved Metrics Files

**Location**: `/output/{exp_name}/metrics/`

#### 1. CV Fold Metrics

**File**: `cv_fold_metrics.csv`

**Content**:
```csv
fold,accuracy,precision,recall,f1_score,auc_roc,auc_pr
1,0.873245,0.856712,0.884523,0.870234,0.923456,0.891234
2,0.879012,0.862345,0.891234,0.876123,0.928901,0.897123
3,0.868734,0.849023,0.877456,0.862891,0.918234,0.885123
4,0.881234,0.865234,0.893456,0.879012,0.931234,0.901234
5,0.874567,0.858901,0.886789,0.872456,0.925678,0.893456
```

#### 2. CV Summary

**File**: `cv_summary.csv`

**Content**:
```csv
metric,mean,std
accuracy,0.875358,0.004123
precision,0.858443,0.005234
recall,0.886692,0.005123
f1_score,0.872143,0.005012
auc_roc,0.925501,0.004567
```

#### 3. Hold-out Test Metrics

**File**: `holdout_test_metrics.csv`

**Content**:
```csv
metric,value
accuracy,0.878900
precision,0.861200
recall,0.892300
f1_score,0.876500
auc_roc,0.930100
auc_pr,0.902300
true_positives,4700
true_negatives,20000
false_positives,2500
false_negatives,800
```

#### 4. Best Hyperparameters

**File**: `best_hyperparameters.csv`

**Content**:
```csv
parameter,value
numTrees,100
maxDepth,10
maxBins,32
minInstancesPerNode,1
subsamplingRate,1.0
featureSubsetStrategy,auto
impurity,gini
```

#### 5. ROC Curve Data

**File**: `holdout_roc_data.csv`

**Content**:
```csv
label,prob_positive,prediction
0.0,0.123456,0.0
1.0,0.876543,1.0
0.0,0.234567,0.0
1.0,0.987654,1.0
...
```

**Use**: Generate ROC curve visualizations

---

### Saved Model

**Location**: `/output/{exp_name}/models/randomforest_final/`

**Format**: Spark ML PipelineModel (Parquet)

**Loading**:
```scala
val model = PipelineModel.load("models/randomforest_final")
val predictions = model.transform(newData)
```

---

## Best Practices

### 1. Always Use Hold-out Test Set

**DO**:
```
✓ Split once at start: 80% dev, 20% test
✓ Lock test set (never use until final evaluation)
✓ All tuning on dev set only
✓ Final evaluation on test set
```

**DON'T**:
```
✗ Use test set for hyperparameter tuning
✗ Evaluate multiple times on test set
✗ Select model based on test performance
```

### 2. Choose Appropriate K for CV

| K | Use Case | Trade-off |
|---|----------|-----------|
| **3** | Quick validation | Faster, less robust |
| **5** | Standard (recommended) | Good speed/robustness balance |
| **10** | Robust validation | Slower, more robust |

**Recommendation**: Start with K=5

### 3. Grid Search Strategy

**Start Small**:
```yaml
# Initial grid (4 combinations)
numTrees: [50, 100]
maxDepth: [5, 10]
```

**Expand If Needed**:
```yaml
# Expanded grid (9 combinations)
numTrees: [50, 100, 200]
maxDepth: [5, 10, 15]
```

**Avoid**:
```yaml
# Too large (64 combinations = slow!)
numTrees: [50, 100, 150, 200]
maxDepth: [5, 7, 10, 12, 15]
maxBins: [16, 32]
minInstancesPerNode: [1, 5]
```

### 4. Interpret CV Variance

**Low Variance (Good)**:
```
F1: 87.02% ± 1.56%  ✓ Stable model
```

**High Variance (Bad)**:
```
F1: 75.12% ± 8.23%  ✗ Unstable, need more data or simpler model
```

### 5. Compare Metrics

**Use F1 for imbalanced data**:
```
Class 0: 71% (not delayed)
Class 1: 29% (delayed)

Don't trust: Accuracy alone (can be high by predicting majority class)
Use: F1-score, AUC (account for imbalance)
```

### 6. Save and Version Models

**MLflow handles this automatically**, but also save locally:

```
/output/{exp_name}/models/randomforest_final/
```

**Tag with git commit**:
```bash
git tag -a model-v1.0 -m "Model from exp4_rf_pca_cv_15min, F1=87.65%"
```

---

## Next Steps

- **[MLflow Integration](09-mlflow-integration.md)** - Experiment tracking with MLflow
- **[Adding Models](10-adding-models.md)** - Implement new ML models
- **[Visualization](12-visualization.md)** - Visualize training results

---

**ML Pipeline Guide Complete! Ready to train production-ready models.** 🤖
