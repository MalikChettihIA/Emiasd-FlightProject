# 📚 Code Reference - Class-by-Class Documentation

Complete reference documentation for all classes in the Flight Delay Prediction system.

---

## Table of Contents

- [Application Layer](#application-layer)
- [Configuration Layer](#configuration-layer)
- [Data Layer](#data-layer)
- [Feature Engineering Layer](#feature-engineering-layer)
- [Machine Learning Layer](#machine-learning-layer)
- [Tracking & Utils Layer](#tracking--utils-layer)

---

## Application Layer

### FlightDelayPredictionApp

**Package**: `com.flightdelay.app`

**File**: `src/main/scala/com/flightdelay/app/FlightDelayPredictionApp.scala`

**Purpose**: Main application entry point and pipeline orchestrator

#### Description

The main object that orchestrates the entire ML pipeline. Loads configuration, manages Spark session, and executes experiments sequentially.

#### Key Methods

```scala
def main(args: Array[String]): Unit
```

**Purpose**: Application entry point

**Flow**:
1. Create Spark session
2. Load configuration from YAML
3. Get enabled experiments
4. Parse tasks to execute
5. Execute load → preprocess → feature-extraction → train for each experiment
6. Handle errors gracefully

**Arguments**:
- `args(0)`: Configuration file name (optional, default: `local-config.yml`)
- `args(1)`: Tasks to execute (optional, default: all tasks)

**Example**:
```bash
# Run all tasks with local-config.yml
spark-submit App.jar

# Run specific tasks
spark-submit App.jar local-config.yml "load,preprocess,train"
```

---

```scala
private def runExperiment(
  experiment: ExperimentConfig,
  tasks: Set[String]
)(implicit spark: SparkSession, configuration: AppConfiguration): Unit
```

**Purpose**: Execute a single experiment

**Flow**:
1. Load preprocessed data
2. Extract features (with optional PCA)
3. Train model (CV + hold-out)
4. Track to MLflow

**Error Handling**: Catches exceptions per experiment, continues with next

---

## Configuration Layer

### ConfigurationLoader

**Package**: `com.flightdelay.config`

**File**: `src/main/scala/com/flightdelay/config/ConfigurationLoader.scala`

**Purpose**: Load and parse YAML configuration files

#### Key Methods

```scala
def loadConfiguration(args: Array[String]): AppConfiguration
```

**Purpose**: Load configuration from YAML file

**Arguments**:
- `args(0)`: Config filename (e.g., `local-config.yml`)

**Returns**: `AppConfiguration` object

**Flow**:
1. Determine config filename from args or use default
2. Read YAML file from resources
3. Parse YAML to case classes using Jackson
4. Validate configuration
5. Return `AppConfiguration`

**Example**:
```scala
val config = ConfigurationLoader.loadConfiguration(Array("local-config.yml"))
```

---

### AppConfiguration

**Package**: `com.flightdelay.config`

**File**: `src/main/scala/com/flightdelay/config/AppConfiguration.scala`

**Purpose**: Root configuration object

#### Fields

```scala
case class AppConfiguration(
  environment: String,                // "local", "lamsade", etc.
  common: CommonConfig,                // Shared config (seed, paths, mlflow)
  experiments: Seq[ExperimentConfig]   // List of experiments
)
```

#### Methods

```scala
def enabledExperiments: Seq[ExperimentConfig]
```

**Purpose**: Filter experiments where `enabled = true`

**Returns**: Sequence of enabled experiments

---

### CommonConfig

**Package**: `com.flightdelay.config`

**Purpose**: Shared configuration across all experiments

#### Fields

```scala
case class CommonConfig(
  seed: Int,              // Random seed (e.g., 42)
  data: DataConfig,       // Data paths
  output: OutputConfig,   // Output paths
  mlflow: MLFlowConfig    // MLflow settings
)
```

---

### DataConfig

**Package**: `com.flightdelay.config`

**Purpose**: Data source configuration

#### Fields

```scala
case class DataConfig(
  basePath: String,            // Base data directory
  flight: FileConfig,          // Flight CSV config
  weather: FileConfig,         // Weather TXT config
  airportMapping: FileConfig   // Airport mapping config
)
```

---

### ExperimentConfig

**Package**: `com.flightdelay.config`

**Purpose**: Per-experiment configuration

#### Fields

```scala
case class ExperimentConfig(
  name: String,                                      // Experiment name
  description: String,                               // Description
  enabled: Boolean,                                  // Run this experiment?
  target: String,                                    // Target label column
  featureExtraction: FeatureExtractionConfig,        // Feature engineering
  model: ExperimentModelConfig,                      // Model config
  train: TrainConfig                                 // Training config
)
```

---

### HyperparametersConfig

**Package**: `com.flightdelay.config`

**Purpose**: Model hyperparameters

#### Fields (Random Forest)

```scala
case class HyperparametersConfig(
  numTrees: Seq[Int],                  // Number of trees [50, 100, 200]
  maxDepth: Seq[Int],                  // Max tree depth [5, 10, 15]
  maxBins: Int,                        // Max bins for discretization
  minInstancesPerNode: Int,            // Min samples per leaf
  subsamplingRate: Double,             // Bootstrap sampling rate
  featureSubsetStrategy: String,       // Feature subset ("auto", "sqrt")
  impurity: String                     // Impurity measure ("gini", "entropy")
)
```

**Note**: `Seq[Int]` for grid search (multiple values), `Int` for fixed params

---

## Data Layer

### FlightDataLoader

**Package**: `com.flightdelay.data.loaders`

**File**: `src/main/scala/com/flightdelay/data/loaders/FlightDataLoader.scala`

**Purpose**: Load flight, weather, and airport data from CSV/TXT files

#### Key Methods

```scala
def loadFromConfiguration()(
  implicit config: AppConfiguration,
  spark: SparkSession
): DataFrame
```

**Purpose**: Load all data sources and join them

**Flow**:
1. Load flight CSV
2. Load weather TXT
3. Load airport mapping CSV
4. Join on WBAN codes
5. Save to `raw_flights.parquet`

**Returns**: DataFrame with ~142K flights, 65 columns

---

### FlightPreprocessingPipeline

**Package**: `com.flightdelay.data.preprocessing`

**File**: `src/main/scala/com/flightdelay/data/preprocessing/FlightPreprocessingPipeline.scala`

**Purpose**: Complete preprocessing pipeline orchestrator

#### Key Methods

```scala
def execute()(
  implicit spark: SparkSession,
  configuration: AppConfiguration
): DataFrame
```

**Purpose**: Execute complete preprocessing pipeline

**Flow**:
1. Load raw data from parquet
2. `FlightDataCleaner.preprocess()` - Clean nulls, outliers
3. `FlightDataGenerator.preprocess()` - Generate features
4. `FlightLabelGenerator.preprocess()` - Create labels
5. `FlightDataLeakageCleaner.preprocess()` - Remove leakage
6. `FlightDataBalancer.preprocess()` - Balance classes (optional)
7. Validate schema
8. Save to `processed_flights.parquet`

**Returns**: DataFrame with ~140K flights, ~68 columns (features + labels)

---

### FlightDataCleaner

**Package**: `com.flightdelay.data.preprocessing`

**File**: `src/main/scala/com/flightdelay/data/preprocessing/FlightDataCleaner.scala`

**Purpose**: Data cleaning (nulls, outliers, types)

#### Key Methods

```scala
def preprocess(data: DataFrame)(implicit spark: SparkSession): DataFrame
```

**Purpose**: Clean raw data

**Operations**:
1. Drop nulls in critical fields (FL_DATE, ORIGIN, DEST, etc.)
2. Cap extreme delays (> 500 minutes → 500)
3. Convert types (String → Date, Int → Double)
4. Remove invalid values

**Returns**: Cleaned DataFrame (~98% retention)

---

### FlightLabelGenerator

**Package**: `com.flightdelay.data.preprocessing`

**File**: `src/main/scala/com/flightdelay/data/preprocessing/FlightLabelGenerator.scala`

**Purpose**: Generate binary classification labels

#### Key Methods

```scala
def preprocess(data: DataFrame)(implicit spark: SparkSession): DataFrame
```

**Purpose**: Create delay labels for different thresholds

**Generated Labels**:
- `label_is_delayed_15min`: `ArrDelay >= 15.0`
- `label_is_delayed_30min`: `ArrDelay >= 30.0`
- `label_is_delayed_45min`: `ArrDelay >= 45.0`
- `label_is_delayed_60min`: `ArrDelay >= 60.0`
- `label_arr_delay_filled`: Fill nulls with 0
- `label_weather_delay_filled`: Fill nulls with 0
- `label_has_weather_delay`: Binary weather delay indicator
- `label_has_nas_delay`: Binary NAS delay indicator
- `label_is_on_time`: No delay
- `label_is_early`: Early arrival

**Returns**: DataFrame with original columns + 10 label columns

---

### FlightDataLeakageCleaner

**Package**: `com.flightdelay.data.preprocessing`

**File**: `src/main/scala/com/flightdelay/data/preprocessing/FlightDataLeakageCleaner.scala`

**Purpose**: Remove data leakage columns (post-flight information)

#### Key Methods

```scala
def preprocess(data: DataFrame)(implicit spark: SparkSession): DataFrame
```

**Purpose**: Remove columns only known after flight lands

**Removed Columns**:
- `ARR_DELAY_NEW` - Actual arrival delay (target variable source)
- `WEATHER_DELAY` - Weather delay component
- `NAS_DELAY` - NAS delay component
- `ARR_TIME` - Actual arrival time
- `DEP_TIME` - Actual departure time
- `DEP_DELAY` - Departure delay
- `CARRIER_DELAY`, `SECURITY_DELAY`, `LATE_AIRCRAFT_DELAY`

**Returns**: DataFrame with leakage-free features only

---

### FlightDataGenerator

**Package**: `com.flightdelay.data.preprocessing`

**File**: `src/main/scala/com/flightdelay/data/preprocessing/FlightDataGenerator.scala`

**Purpose**: Generate derived features

#### Key Methods

```scala
def preprocess(data: DataFrame)(implicit spark: SparkSession): DataFrame
```

**Purpose**: Engineer time-based and route-based features

**Generated Features**:
- `feature_departure_hour`: Hour extracted from CRS_DEP_TIME
- `feature_flight_month`: Month from FL_DATE
- `feature_flight_year`: Year from FL_DATE
- `feature_flight_quarter`: Quarter (1-4)
- `feature_flight_day_of_week`: Day of week (1-7)
- `feature_is_weekend`: Binary weekend indicator
- `feature_route_id`: "ORIGIN-DEST" string
- `feature_distance_category`: "short", "medium", "long"

**Returns**: DataFrame with original columns + 8 feature columns

---

## Feature Engineering Layer

### FlightFeatureExtractor

**Package**: `com.flightdelay.features`

**File**: `src/main/scala/com/flightdelay/features/FlightFeatureExtractor.scala`

**Purpose**: Main entry point for feature extraction

#### Key Methods

```scala
def extract(
  data: DataFrame,
  experiment: ExperimentConfig
)(implicit configuration: AppConfiguration): DataFrame
```

**Purpose**: Extract features with optional PCA

**Flow**:
1. Drop unused labels (keep only target)
2. Detect column types (text vs numeric)
3. Apply feature selection (if enabled)
4. Build feature pipeline (StringIndexer + VectorAssembler + StandardScaler)
5. Apply PCA (if enabled)
6. Save extracted features

**Returns**: DataFrame with (features: Vector, label: Double)

---

```scala
def applyPCA(
  data: DataFrame,
  featureNames: Array[String],
  experiment: ExperimentConfig
)(implicit configuration: AppConfiguration): (DataFrame, PCAModel, VarianceAnalysis)
```

**Purpose**: Apply PCA dimensionality reduction

**Flow**:
1. Create PCAFeatureExtractor with variance threshold
2. Fit PCA on data
3. Transform data
4. Save PCA analysis (variance, projections, loadings)

**Returns**: (Transformed DataFrame, PCA model, Variance analysis)

---

### PCAFeatureExtractor

**Package**: `com.flightdelay.features.pca`

**File**: `src/main/scala/com/flightdelay/features/pca/PCAFeatureExtractor.scala`

**Purpose**: PCA dimensionality reduction with variance-based selection

#### Constructor Parameters

```scala
class PCAFeatureExtractor(
  val inputCol: String = "features",
  val outputCol: String = "pcaFeatures",
  val k: Int = 10,
  val varianceThreshold: Option[Double] = None
)
```

**Parameters**:
- `inputCol`: Input feature column name
- `outputCol`: Output PCA feature column name
- `k`: Number of components (if varianceThreshold is None)
- `varianceThreshold`: Target cumulative variance (e.g., 0.7 for 70%)

#### Key Methods

```scala
def fit(data: DataFrame): PCAModel
```

**Purpose**: Fit PCA on training data

**Flow**:
1. If variance threshold: fit with max components, select K by variance
2. Else: fit with fixed K
3. Return fitted PCAModel

---

```scala
def fitTransform(data: DataFrame): (PCAModel, DataFrame, VarianceAnalysis)
```

**Purpose**: Fit and transform in one step

**Returns**: (PCA model, Transformed data, Variance analysis)

---

```scala
def analyzeVariance(model: PCAModel, data: DataFrame): VarianceAnalysis
```

**Purpose**: Compute variance analysis

**Returns**: VarianceAnalysis with explained variance per component

---

```scala
def saveVarianceAnalysis(analysis: VarianceAnalysis, outputPath: String): Unit
```

**Purpose**: Save variance to CSV for visualization

**Output**: `pca_variance.csv` with columns: component, explained_variance, cumulative_variance

---

```scala
def savePCAProjections(
  transformedData: DataFrame,
  outputPath: String,
  labelCol: Option[String],
  maxSamples: Int = 5000
): Unit
```

**Purpose**: Save first 2 PCs for biplot

**Output**: `pca_projections.csv` with columns: pc1, pc2, label

---

```scala
def savePCALoadings(
  model: PCAModel,
  outputPath: String,
  topN: Option[Int],
  featureNames: Option[Array[String]]
): Unit
```

**Purpose**: Save feature contributions to PCs

**Output**: `pca_loadings.csv` with columns: feature_index, feature_name, PC1, PC2, ...

---

### VarianceAnalysis

**Package**: `com.flightdelay.features.pca`

**Purpose**: PCA variance analysis result container

#### Fields

```scala
case class VarianceAnalysis(
  numComponents: Int,                    // Selected components
  originalDimension: Int,                // Original features
  explainedVariance: Array[Double],      // Variance per PC
  cumulativeVariance: Array[Double],     // Cumulative variance
  totalVarianceExplained: Double,        // Total (last cumulative)
  componentIndices: Array[Int]           // PC indices (1, 2, 3, ...)
)
```

#### Methods

```scala
def getVarianceForTopK(k: Int): Double
```

**Purpose**: Get cumulative variance for top K components

---

```scala
def getMinComponentsForVariance(threshold: Double): Int
```

**Purpose**: Find minimum K for target variance

---

## Machine Learning Layer

### MLPipeline

**Package**: `com.flightdelay.ml`

**File**: `src/main/scala/com/flightdelay/ml/MLPipeline.scala`

**Purpose**: Main ML pipeline orchestrator (Option B: Hold-out + K-fold CV)

#### Key Methods

```scala
def train(
  data: DataFrame,
  experiment: ExperimentConfig
)(implicit spark: SparkSession, config: AppConfiguration): MLResult
```

**Purpose**: Train and evaluate model with complete pipeline

**Flow**:
1. **MLflow Init**: Initialize tracking, start run
2. **Initial Split**: 80% dev, 20% test
3. **K-Fold CV**: Cross-validate on dev set (with optional grid search)
4. **Final Training**: Train on full dev set with best params
5. **Hold-out Evaluation**: Evaluate on test set
6. **Save**: Save model and metrics
7. **MLflow Log**: Log params, metrics, artifacts
8. **End Run**: Close MLflow run

**Returns**: `MLResult` with model, CV metrics, test metrics, best params

---

### MLResult

**Package**: `com.flightdelay.ml`

**Purpose**: ML pipeline result container

#### Fields

```scala
case class MLResult(
  experiment: ExperimentConfig,                // Experiment config
  model: Transformer,                          // Trained model
  cvMetrics: CVMetrics,                        // CV aggregated metrics
  holdOutMetrics: EvaluationMetrics,           // Test metrics
  bestHyperparameters: Map[String, Any],       // Best params from grid search
  trainingTimeSeconds: Double                  // Total time
)
```

---

### CrossValidator

**Package**: `com.flightdelay.ml.training`

**File**: `src/main/scala/com/flightdelay/ml/training/CrossValidator.scala`

**Purpose**: K-fold cross-validation with optional grid search

#### Key Methods

```scala
def validate(
  data: DataFrame,
  experiment: ExperimentConfig
)(implicit spark: SparkSession, config: AppConfiguration): CVResult
```

**Purpose**: Perform K-fold cross-validation

**Flow**:
1. Split data into K folds
2. For each fold:
   - Split train/validation
   - If grid search: try all hyperparameter combinations
   - Train models, evaluate on validation
   - Select best by evaluation metric
3. Aggregate metrics across folds (mean ± std)
4. Return best hyperparameters and metrics

**Returns**: `CVResult` with per-fold metrics, avg metrics, best params

---

### CVResult

**Package**: `com.flightdelay.ml.training`

**Purpose**: Cross-validation result container

#### Fields

```scala
case class CVResult(
  foldMetrics: Seq[EvaluationMetrics],   // Metrics per fold
  avgMetrics: EvaluationMetrics,         // Mean across folds
  stdMetrics: EvaluationMetrics,         // Std dev across folds
  bestHyperparameters: Map[String, Any], // Best params from grid search
  numFolds: Int                          // Number of folds (e.g., 5)
)
```

---

### Trainer

**Package**: `com.flightdelay.ml.training`

**File**: `src/main/scala/com/flightdelay/ml/training/Trainer.scala`

**Purpose**: Model training logic

#### Key Methods

```scala
def trainFinal(
  devData: DataFrame,
  experiment: ExperimentConfig,
  bestHyperparameters: Map[String, Any]
)(implicit spark: SparkSession, config: AppConfiguration): Transformer
```

**Purpose**: Train final model on full dev set with best params

**Flow**:
1. Create model via ModelFactory
2. Override hyperparameters with best params from CV
3. Train on ALL dev data
4. Return trained model

**Returns**: Trained PipelineModel

---

### ModelEvaluator

**Package**: `com.flightdelay.ml.evaluation`

**File**: `src/main/scala/com/flightdelay/ml/evaluation/ModelEvaluator.scala`

**Purpose**: Compute evaluation metrics

#### Key Methods

```scala
def evaluate(predictions: DataFrame): EvaluationMetrics
```

**Purpose**: Compute comprehensive metrics from predictions

**Computed Metrics**:
- Accuracy
- Weighted Precision
- Weighted Recall
- F1-Score
- AUC-ROC
- AUC-PR
- Confusion matrix (TP, TN, FP, FN)

**Returns**: `EvaluationMetrics` case class

---

### EvaluationMetrics

**Package**: `com.flightdelay.ml.evaluation`

**Purpose**: Evaluation metrics container

#### Fields

```scala
case class EvaluationMetrics(
  accuracy: Double,          // (TP + TN) / Total
  precision: Double,         // TP / (TP + FP)
  recall: Double,            // TP / (TP + FN)
  f1Score: Double,           // 2 × P × R / (P + R)
  areaUnderROC: Double,      // AUC-ROC
  areaUnderPR: Double,       // AUC-PR
  truePositives: Long,       // TP count
  trueNegatives: Long,       // TN count
  falsePositives: Long,      // FP count
  falseNegatives: Long       // FN count
)
```

---

### ModelFactory

**Package**: `com.flightdelay.ml.models`

**File**: `src/main/scala/com/flightdelay/ml/models/ModelFactory.scala`

**Purpose**: Factory for creating models dynamically

#### Key Methods

```scala
def create(experiment: ExperimentConfig): MLModel
```

**Purpose**: Create model based on modelType in config

**Supported Models**:
- `"randomforest"` or `"rf"` → `RandomForestModel`

**Planned Models** (not yet implemented):
- `"gbt"` → Gradient Boosted Trees
- `"logisticregression"` → Logistic Regression
- `"decisiontree"` → Decision Tree
- `"xgboost"` → XGBoost
- `"lightgbm"` → LightGBM

**Returns**: MLModel instance

**Throws**: IllegalArgumentException if unknown model type

---

### MLModel (Trait)

**Package**: `com.flightdelay.ml.models`

**File**: `src/main/scala/com/flightdelay/ml/models/MLModel.scala`

**Purpose**: Abstract interface for all ML models

#### Required Methods

```scala
def train(data: DataFrame): Transformer
```

**Purpose**: Train model on data

**Input**: DataFrame with "features" and "label" columns

**Returns**: Trained Transformer (usually PipelineModel)

---

#### Default Methods

```scala
def predict(model: Transformer, data: DataFrame): DataFrame
```

**Purpose**: Make predictions

---

```scala
def saveModel(model: Transformer, path: String): Unit
```

**Purpose**: Save model to disk

---

```scala
def loadModel(path: String): Transformer
```

**Purpose**: Load saved model

---

### RandomForestModel

**Package**: `com.flightdelay.ml.models`

**File**: `src/main/scala/com/flightdelay/ml/models/RandomForestModel.scala`

**Purpose**: Random Forest implementation

#### Constructor

```scala
class RandomForestModel(experiment: ExperimentConfig) extends MLModel
```

#### Key Methods

```scala
override def train(data: DataFrame): Transformer
```

**Purpose**: Train Random Forest classifier

**Flow**:
1. Extract hyperparameters from experiment config
2. Configure RandomForestClassifier
3. Create Pipeline with RF
4. Fit model on data
5. Extract and display feature importance
6. Return trained model

**Returns**: PipelineModel with trained RF

---

## Tracking & Utils Layer

### MLFlowTracker

**Package**: `com.flightdelay.ml.tracking`

**File**: `src/main/scala/com/flightdelay/ml/tracking/MLFlowTracker.scala`

**Purpose**: MLflow experiment tracking client

#### Key Methods

```scala
def initialize(uri: String, enable: Boolean = true): Unit
```

**Purpose**: Initialize MLflow client

**Parameters**:
- `uri`: Tracking server URL (e.g., `http://mlflow-server:5000`)
- `enable`: Enable/disable tracking

---

```scala
def getOrCreateExperiment(): Option[String]
```

**Purpose**: Get or create experiment "flight-delay-prediction"

**Returns**: Experiment ID (or None if disabled)

---

```scala
def startRun(experimentId: String, runName: String): Option[String]
```

**Purpose**: Start a new run

**Returns**: Run ID (or None if disabled)

---

```scala
def logParams(runId: String, params: Map[String, Any]): Unit
```

**Purpose**: Log parameters (hyperparameters, config)

---

```scala
def logMetric(runId: String, key: String, value: Double, step: Long = 0): Unit
def logMetrics(runId: String, metrics: Map[String, Double], step: Long = 0): Unit
```

**Purpose**: Log metrics (performance measurements)

---

```scala
def logArtifact(runId: String, localPath: String): Unit
```

**Purpose**: Upload files/directories to MLflow

---

```scala
def setTag(runId: String, key: String, value: String): Unit
```

**Purpose**: Add metadata tags to run

---

```scala
def endRun(runId: String, status: RunStatus = RunStatus.FINISHED): Unit
```

**Purpose**: Mark run as completed

---

### MetricsWriter

**Package**: `com.flightdelay.utils`

**File**: `src/main/scala/com/flightdelay/utils/MetricsWriter.scala`

**Purpose**: Write metrics to CSV files

#### Key Methods

```scala
def writeCsv(
  headers: Seq[String],
  rows: Seq[Seq[String]],
  outputPath: String
): Try[Unit]
```

**Purpose**: Write CSV file

**Parameters**:
- `headers`: Column names
- `rows`: Data rows
- `outputPath`: Output file path

**Returns**: `Try[Unit]` (Success or Failure)

---

### CsvWriter

**Package**: `com.flightdelay.utils`

**File**: `src/main/scala/com/flightdelay/utils/CsvWriter.scala`

**Purpose**: Generic CSV writing utilities

#### Key Methods

```scala
def write(
  data: Seq[Seq[String]],
  outputPath: String,
  headers: Option[Seq[String]] = None
): Try[Unit]
```

**Purpose**: Write CSV with optional headers

---

## Summary by Package

| Package | Classes | Purpose |
|---------|---------|---------|
| `app` | 1 | Application entry point |
| `config` | 15+ | Configuration management |
| `data.loaders` | 2 | Data loading |
| `data.preprocessing` | 6 | Data preprocessing |
| `features` | 2 | Feature extraction |
| `features.pca` | 2 | PCA dimensionality reduction |
| `ml` | 2 | ML pipeline orchestration |
| `ml.training` | 2 | Training and CV |
| `ml.evaluation` | 1 | Metrics computation |
| `ml.models` | 3 | Model implementations |
| `ml.tracking` | 1 | MLflow tracking |
| `utils` | 2 | Utilities (CSV, metrics) |

---

## Next Steps

- **[Architecture Guide](04-project-architecture.md)** - System design overview
- **[Data Pipeline](06-data-pipeline.md)** - Data flow details
- **[ML Pipeline](08-ml-pipeline.md)** - Training pipeline details

---

**Code Reference Guide Complete! Complete class-by-class documentation.** 📚
