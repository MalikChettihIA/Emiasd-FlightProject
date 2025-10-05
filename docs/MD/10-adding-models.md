# 🛠️ Adding New Models Guide

Step-by-step guide to implementing new machine learning models in the Flight Delay Prediction system.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Step-by-Step Tutorial](#step-by-step-tutorial)
- [Example: Gradient Boosted Trees](#example-gradient-boosted-trees)
- [Testing Your Model](#testing-your-model)
- [Best Practices](#best-practices)

---

## Overview

The system uses the **Factory Pattern** to create models dynamically, making it easy to add new ML algorithms without modifying the core pipeline.

### Adding a Model (5 Steps)

1. Create model class implementing `MLModel` trait
2. Add hyperparameters to configuration
3. Register model in `ModelFactory`
4. Update configuration file
5. Test your model

**Time Required**: ~30-60 minutes

---

## Architecture

### Current Model Architecture

```
┌──────────────────────────────────────────────────────────┐
│ MLModel Trait (Interface)                                │
├──────────────────────────────────────────────────────────┤
│ • def train(data: DataFrame): Transformer                │
│ • def predict(model: Transformer, data: DataFrame): DF   │
│ • def saveModel(model: Transformer, path: String)        │
│ • def loadModel(path: String): Transformer               │
└──────────────────────────────────────────────────────────┘
                         ↑
                         │ implements
                         │
        ┌────────────────┴────────────────┐
        │                                  │
┌───────────────────┐           ┌─────────────────────┐
│ RandomForestModel │           │  YourNewModel       │
├───────────────────┤           ├─────────────────────┤
│ • Configures RF   │           │ • Configures ...    │
│ • Trains model    │           │ • Trains model      │
│ • Saves features  │           │ • Saves features    │
└───────────────────┘           └─────────────────────┘
        ↑                                  ↑
        └──────────────┬───────────────────┘
                       │
              ┌────────────────┐
              │ ModelFactory   │
              ├────────────────┤
              │ create(config) │
              └────────────────┘
```

### Factory Pattern Benefits

| Benefit | Explanation |
|---------|-------------|
| **Extensibility** | Add models without changing pipeline code |
| **Maintainability** | Each model in separate class |
| **Type Safety** | Compile-time model validation |
| **Testability** | Mock models for testing |

---

## Step-by-Step Tutorial

### Step 1: Create Model Class

**Location**: `src/main/scala/com/flightdelay/ml/models/YourNewModel.scala`

**Template**:

```scala
package com.flightdelay.ml.models

import com.flightdelay.config.ExperimentConfig
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.classification.{YourSparkMLClass}
import org.apache.spark.sql.DataFrame

/**
 * Your Model implementation for flight delay prediction.
 *
 * Description of your model...
 *
 * @param experiment Experiment configuration with hyperparameters
 */
class YourNewModel(experiment: ExperimentConfig) extends MLModel {

  /**
   * Train your model on flight delay data
   * @param data Training data with "features" and "label" columns
   * @return Trained model wrapped in a Pipeline
   */
  override def train(data: DataFrame): Transformer = {
    val hp = experiment.train.hyperparameters

    // Extract hyperparameters (use .head for single training)
    val param1 = hp.yourParam1.head
    val param2 = hp.yourParam2.head

    println(s"\n[YourModel] Training with hyperparameters:")
    println(s"  - Param1: $param1")
    println(s"  - Param2: $param2")

    // Configure your Spark ML classifier
    val classifier = new YourSparkMLClass()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability")
      .setParam1(param1)
      .setParam2(param2)

    // Create pipeline
    val pipeline = new Pipeline().setStages(Array(classifier))

    println("\nStarting training...")
    val startTime = System.currentTimeMillis()

    val model = pipeline.fit(data)

    val endTime = System.currentTimeMillis()
    val trainingTime = (endTime - startTime) / 1000.0

    println(f"\n- Training completed in $trainingTime%.2f seconds")

    model
  }
}

object YourNewModel {
  def apply(experiment: ExperimentConfig): YourNewModel = {
    new YourNewModel(experiment)
  }
}
```

**Key Points**:
- Extend `MLModel` trait
- Accept `ExperimentConfig` in constructor
- Implement `train()` method
- Return Spark ML `Transformer` (usually `PipelineModel`)
- Use `.head` on hyperparameter arrays for single training

---

### Step 2: Add Hyperparameters to Configuration

**File**: `src/main/scala/com/flightdelay/config/HyperparametersConfig.scala`

**Add your hyperparameters**:

```scala
case class HyperparametersConfig(
  // Existing (Random Forest)
  numTrees: Seq[Int],
  maxDepth: Seq[Int],
  ...

  // NEW: Your model's hyperparameters
  yourParam1: Seq[Double],
  yourParam2: Seq[Int],
  yourParam3: Seq[String]
)
```

**Why**:
- Type-safe hyperparameter access
- Grid search iterates over `Seq` values
- Configuration validation at compile-time

---

### Step 3: Register Model in ModelFactory

**File**: `src/main/scala/com/flightdelay/ml/models/ModelFactory.scala`

**Add case to match statement**:

```scala
def create(experiment: ExperimentConfig): MLModel = {
  val modelType = experiment.model.modelType.toLowerCase

  modelType match {
    case "randomforest" | "rf" =>
      println("  → Random Forest Classifier")
      new RandomForestModel(experiment)

    // NEW: Add your model
    case "yourmodel" | "ym" =>
      println("  → Your Model Classifier")
      new YourNewModel(experiment)

    case unknown =>
      throw new IllegalArgumentException(
        s"Unknown model type: '$unknown'"
      )
  }
}
```

**Update supported models**:

```scala
def supportedModels: Seq[String] = Seq(
  "randomforest",
  "yourmodel"  // NEW
)
```

---

### Step 4: Update Configuration File

**File**: `src/main/resources/local-config.yml`

**Add experiment with your model**:

```yaml
experiments:
  - name: "exp_yourmodel_15min"
    description: "Your Model baseline"
    enabled: true
    target: "label_is_delayed_15min"

    featureExtraction:
      type: pca
      pcaVarianceThreshold: 0.7

    model:
      modelType: "yourmodel"  # Your model type

    train:
      trainRatio: 0.8

      crossValidation:
        numFolds: 5

      gridSearch:
        enabled: true
        evaluationMetric: "f1"

      hyperparameters:
        # Your model's hyperparameters
        yourParam1: [0.01, 0.1]
        yourParam2: [10, 20]
        yourParam3: ["option1"]
```

---

### Step 5: Test Your Model

**Compile**:

```bash
sbt package
```

**Expected Output**:
```
[success] Total time: 15 s
```

**Run Experiment**:

```bash
cd docker
./submit.sh
```

**Expected Output**:
```
[ModelFactory] Creating model: yourmodel
  → Your Model Classifier
[YourModel] Training with hyperparameters:
  - Param1: 0.01
  - Param2: 10
...
Training completed in 45.32 seconds
```

**Verify Results**:

1. Check MLflow UI: http://localhost:5555
2. Check output: `work/output/exp_yourmodel_15min/`
3. Check metrics: `work/output/exp_yourmodel_15min/metrics/`

---

## Example: Gradient Boosted Trees

### Complete Implementation

**File**: `GradientBoostedTreesModel.scala`

```scala
package com.flightdelay.ml.models

import com.flightdelay.config.ExperimentConfig
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.sql.DataFrame

/**
 * Gradient Boosted Trees model implementation.
 *
 * GBT builds an ensemble of decision trees sequentially, where each tree
 * corrects errors made by previous trees. Excellent for tabular data.
 *
 * @param experiment Experiment configuration
 */
class GradientBoostedTreesModel(experiment: ExperimentConfig) extends MLModel {

  override def train(data: DataFrame): Transformer = {
    val hp = experiment.train.hyperparameters

    // Extract GBT-specific hyperparameters
    val maxIter = hp.maxIter.head
    val maxDepth = hp.maxDepth.head
    val stepSize = hp.stepSize.head

    println(s"\n[GBT] Training with hyperparameters:")
    println(s"  - Max iterations: $maxIter")
    println(s"  - Max depth: $maxDepth")
    println(s"  - Step size: $stepSize")
    println(s"  - Max bins: ${hp.maxBins}")

    // Configure GBT classifier
    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setMaxIter(maxIter)
      .setMaxDepth(maxDepth)
      .setStepSize(stepSize)
      .setMaxBins(hp.maxBins)
      .setFeatureSubsetStrategy(hp.featureSubsetStrategy)
      .setImpurity(hp.impurity)

    val pipeline = new Pipeline().setStages(Array(gbt))

    println("\nStarting GBT training...")
    val startTime = System.currentTimeMillis()

    val model = pipeline.fit(data)

    val endTime = System.currentTimeMillis()
    val trainingTime = (endTime - startTime) / 1000.0

    println(f"\n- GBT training completed in $trainingTime%.2f seconds")

    // Display feature importance
    val gbtModel = model.stages(0).asInstanceOf[GBTClassificationModel]
    displayFeatureImportance(gbtModel)

    model
  }

  private def displayFeatureImportance(model: GBTClassificationModel): Unit = {
    val importances = model.featureImportances.toArray
    println(f"\nTop 10 Feature Importances:")
    importances.zipWithIndex
      .sortBy(-_._1)
      .take(10)
      .foreach { case (imp, idx) =>
        println(f"Feature $idx%3d: ${imp * 100}%6.2f%%")
      }
  }
}

object GradientBoostedTreesModel {
  def apply(experiment: ExperimentConfig): GradientBoostedTreesModel = {
    new GradientBoostedTreesModel(experiment)
  }
}
```

### Update HyperparametersConfig

```scala
case class HyperparametersConfig(
  // Random Forest
  numTrees: Seq[Int],
  maxDepth: Seq[Int],

  // GBT-specific
  maxIter: Seq[Int],        // Number of boosting iterations
  stepSize: Seq[Double],    // Learning rate (0.0-1.0)

  // Shared
  maxBins: Int,
  featureSubsetStrategy: String,
  impurity: String
)
```

### Register in ModelFactory

```scala
def create(experiment: ExperimentConfig): MLModel = {
  modelType match {
    case "randomforest" | "rf" =>
      new RandomForestModel(experiment)

    case "gbt" | "gradientboostedtrees" =>
      new GradientBoostedTreesModel(experiment)

    case unknown =>
      throw new IllegalArgumentException(...)
  }
}
```

### Configuration Example

```yaml
experiments:
  - name: "exp_gbt_15min"
    model:
      modelType: "gbt"

    train:
      hyperparameters:
        maxIter: [20, 30]      # Boosting iterations
        maxDepth: [5, 10]      # Tree depth
        stepSize: [0.1, 0.3]   # Learning rate
        maxBins: 32
        featureSubsetStrategy: "auto"
        impurity: "gini"
```

### Grid Search

With this config, grid search will try:
```
(maxIter=20, maxDepth=5, stepSize=0.1)
(maxIter=20, maxDepth=5, stepSize=0.3)
(maxIter=20, maxDepth=10, stepSize=0.1)
(maxIter=20, maxDepth=10, stepSize=0.3)
(maxIter=30, maxDepth=5, stepSize=0.1)
(maxIter=30, maxDepth=5, stepSize=0.3)
(maxIter=30, maxDepth=10, stepSize=0.1)
(maxIter=30, maxDepth=10, stepSize=0.3)

Total: 8 combinations × 5 folds = 40 models trained
```

---

## Testing Your Model

### 1. Compile and Build

```bash
sbt clean compile package
```

**Check for errors**:
- Type mismatches
- Missing hyperparameters
- Incorrect method signatures

### 2. Quick Test (No Grid Search)

**Config**:
```yaml
experiments:
  - name: "test_yourmodel"
    enabled: true
    target: "label_is_delayed_15min"
    model:
      modelType: "yourmodel"
    train:
      trainRatio: 0.8
      crossValidation:
        numFolds: 3  # Fewer folds for speed
      gridSearch:
        enabled: false  # Disable grid search
      hyperparameters:
        yourParam1: [0.1]   # Single value
        yourParam2: [10]    # Single value
```

**Run**:
```bash
./submit.sh
```

**Verify**:
```
[ModelFactory] Creating model: yourmodel
  → Your Model Classifier
[YourModel] Training with hyperparameters...
Training completed in XX seconds

Hold-out Test Metrics:
  Accuracy:  XX.XX%
  F1-Score:  XX.XX%
  AUC-ROC:   X.XXXX
```

### 3. Full Test (With Grid Search)

**Config**:
```yaml
gridSearch:
  enabled: true

hyperparameters:
  yourParam1: [0.01, 0.1, 1.0]  # 3 values
  yourParam2: [5, 10, 20]       # 3 values
```

**Run**: This will train 3 × 3 × 5 = 45 models

**Verify**: Check MLflow for best hyperparameters

### 4. Compare with RandomForest

**Run both experiments**:
```yaml
experiments:
  - name: "baseline_rf"
    model:
      modelType: "randomforest"

  - name: "new_yourmodel"
    model:
      modelType: "yourmodel"
```

**Compare in MLflow**:
- test_f1
- test_auc
- training_time_seconds

---

## Best Practices

### 1. Start with Spark ML Built-in Algorithms

**Available in Spark MLlib**:
- `RandomForestClassifier` ✅ (implemented)
- `GBTClassifier` (Gradient Boosted Trees)
- `LogisticRegression`
- `DecisionTreeClassifier`
- `NaiveBayes`
- `MultilayerPerceptronClassifier` (Neural Network)

**External Libraries** (requires additional dependencies):
- XGBoost
- LightGBM

### 2. Use Descriptive Class Names

**Good**:
```scala
GradientBoostedTreesModel
LogisticRegressionModel
NeuralNetworkModel
```

**Bad**:
```scala
Model2
MyModel
NewClassifier
```

### 3. Document Hyperparameters

**Include in docstring**:

```scala
/**
 * Gradient Boosted Trees for flight delay prediction.
 *
 * Hyperparameters:
 * - maxIter: Number of boosting iterations (default: 20, range: 10-100)
 * - maxDepth: Maximum tree depth (default: 5, range: 3-10)
 * - stepSize: Learning rate (default: 0.1, range: 0.01-0.3)
 */
```

### 4. Handle Hyperparameter Defaults

**Use getOrElse for optional params**:

```scala
val maxIter = hp.maxIter.headOption.getOrElse(20)
val stepSize = hp.stepSize.headOption.getOrElse(0.1)
```

### 5. Log Training Progress

**Good logging**:

```scala
println(s"\n[YourModel] Training with hyperparameters:")
hp.toMap.foreach { case (k, v) => println(s"  - $k: $v") }

println("\nStarting training...")
val model = pipeline.fit(data)
println(f"\n- Training completed in $trainingTime%.2f seconds")
```

### 6. Save Model Artifacts

**Feature importance**:

```scala
val importances = model.featureImportances
saveFeatureImportance(importances, s"$outputPath/feature_importance.csv")
```

**Model metadata**:

```scala
saveModelMetadata(Map(
  "model_type" -> "yourmodel",
  "num_features" -> data.columns.length,
  "training_samples" -> data.count()
), s"$outputPath/model_metadata.json")
```

---

## Next Steps

After adding your model:

1. **Run experiments** and compare with RandomForest
2. **Document** your model in `11-code-reference.md`
3. **Add tests** (optional but recommended)
4. **Share results** via MLflow
5. **Update README.md** with supported models

---

## Checklist

Before submitting your new model:

- [ ] Model class created and implements `MLModel`
- [ ] Hyperparameters added to `HyperparametersConfig`
- [ ] Model registered in `ModelFactory`
- [ ] Configuration example added
- [ ] Code compiles without errors
- [ ] Model trains successfully on test data
- [ ] Results logged to MLflow
- [ ] Model compared with baseline (Random Forest)
- [ ] Documentation updated

---

**Adding Models Guide Complete! Ready to extend the system with new ML algorithms.** 🛠️
