# 🏗️ Project Architecture

A comprehensive guide to the Flight Delay Prediction system architecture, design patterns, and component organization.

---

## Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Package Structure](#package-structure)
- [Data Flow](#data-flow)
- [Core Components](#core-components)
- [Design Patterns](#design-patterns)
- [Integration Points](#integration-points)

---

## Overview

The Flight Delay Prediction system follows a **modular, pipeline-based architecture** designed for:

- **Scalability**: Distributed processing with Apache Spark
- **Maintainability**: Clear separation of concerns
- **Extensibility**: Easy to add new models and features
- **Reproducibility**: Configuration-driven experiments
- **Observability**: Comprehensive logging and tracking

### Architecture Principles

1. **Separation of Concerns** - Each component has a single responsibility
2. **Configuration Over Code** - Experiments defined in YAML, not hardcoded
3. **Fail Fast** - Validation at each pipeline stage
4. **Immutability** - DataFrames are transformed, not modified
5. **Dependency Injection** - Implicit parameters for Spark and config

---

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Application Layer                             │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  FlightDelayPredictionApp (Main Entry Point)                │    │
│  │  - Orchestrates pipeline execution                          │    │
│  │  - Manages multiple experiments                             │    │
│  │  - Handles task execution (load, preprocess, train, etc.)   │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│                        Configuration Layer                            │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐   │
│  │ ConfigurationLoader│  │  AppConfiguration │  │ ExperimentConfig │   │
│  │ - Loads YAML       │  │  - Common config  │  │ - Per-experiment │   │
│  │ - Validates config │  │  - Data paths     │  │ - Hyperparams    │   │
│  └──────────────────┘  └──────────────────┘  └─────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│                           Data Layer                                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐   │
│  │ FlightDataLoader  │  │  Preprocessing   │  │ FlightFeature-  │   │
│  │ - Load CSVs       │  │  Pipeline        │  │ Extractor       │   │
│  │ - Schema mapping  │  │  - Cleaning      │  │ - PCA reduction │   │
│  │ - Validation      │  │  - Label gen     │  │ - Feature sel.  │   │
│  └──────────────────┘  └──────────────────┘  └─────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│                       Machine Learning Layer                          │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐   │
│  │   MLPipeline      │  │  CrossValidator   │  │  ModelEvaluator  │   │
│  │ - Train/test split│  │  - K-fold CV      │  │ - Metrics calc   │   │
│  │ - Model training  │  │  - Grid search    │  │ - ROC curves     │   │
│  │ - Evaluation      │  │  - Best params    │  │ - Confusion mat. │   │
│  └──────────────────┘  └──────────────────┘  └─────────────────┘   │
│  ┌──────────────────┐  ┌──────────────────┐                         │
│  │  ModelFactory     │  │  RandomForest-   │                         │
│  │ - Creates models  │  │  Model           │  [Extensible]           │
│  │ - Model registry  │  │ - Trains RF      │  [Add GBT, etc.]        │
│  └──────────────────┘  └──────────────────┘                         │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│                        Tracking & Utils Layer                         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐   │
│  │  MLFlowTracker    │  │  MetricsWriter    │  │  CsvWriter       │   │
│  │ - Log params      │  │  - Save CSV       │  │ - CSV I/O        │   │
│  │ - Log metrics     │  │  - ROC data       │  │ - Utilities      │   │
│  │ - Log artifacts   │  │  - PCA variance   │  │                  │   │
│  └──────────────────┘  └──────────────────┘  └─────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### Component Layers

| Layer | Purpose | Key Classes |
|-------|---------|-------------|
| **Application** | Orchestration and task execution | `FlightDelayPredictionApp` |
| **Configuration** | YAML loading and experiment management | `ConfigurationLoader`, `AppConfiguration` |
| **Data** | Loading, preprocessing, feature engineering | `FlightDataLoader`, `FlightPreprocessingPipeline` |
| **ML** | Model training, evaluation, cross-validation | `MLPipeline`, `CrossValidator`, `ModelFactory` |
| **Tracking** | Experiment tracking and metrics persistence | `MLFlowTracker`, `MetricsWriter` |

---

## Package Structure

### Source Code Organization

```
src/main/scala/com/flightdelay/
│
├── app/                              # Application entry point
│   └── FlightDelayPredictionApp      # Main orchestrator
│
├── config/                           # Configuration management
│   ├── ConfigurationLoader           # YAML config loader
│   ├── AppConfiguration              # Application-wide config
│   ├── ExperimentConfig              # Per-experiment config
│   ├── CommonConfig                  # Shared settings (seed, paths)
│   ├── DataConfig                    # Data source paths
│   ├── OutputConfig                  # Output paths
│   ├── MLFlowConfig                  # MLflow settings
│   ├── FeatureExtractionConfig       # Feature engineering config
│   ├── TrainConfig                   # Training parameters
│   ├── CrossValidationConfig         # CV settings
│   ├── GridSearchConfig              # Grid search settings
│   ├── HyperparametersConfig         # Model hyperparameters
│   ├── ModelConfig                   # Model-specific config
│   └── ExperimentModelConfig         # Per-experiment model config
│
├── data/                             # Data handling
│   ├── loaders/
│   │   ├── DataLoader                # Base data loader trait
│   │   └── FlightDataLoader          # Flight data CSV loader
│   ├── preprocessing/
│   │   ├── DataPreprocessor          # Base preprocessor trait
│   │   ├── FlightPreprocessingPipeline  # Complete preprocessing pipeline
│   │   ├── FlightDataCleaner         # Data cleaning (nulls, outliers)
│   │   ├── FlightLabelGenerator      # Generate delay labels
│   │   ├── FlightDataLeakageCleaner  # Remove data leakage columns
│   │   ├── FlightDataGenerator       # Feature generation
│   │   └── FlightDataBalancer        # Class balancing (SMOTE, etc.)
│   ├── models/
│   │   └── Flight                    # Flight case class
│   └── utils/
│       └── DataQualityMetrics        # Data quality validation
│
├── features/                         # Feature engineering
│   ├── FlightFeatureExtractor        # Main feature extractor
│   ├── pca/
│   │   └── PCAFeatureExtractor       # PCA dimensionality reduction
│   ├── pipelines/
│   │   ├── BasicFlightFeaturePipeline    # Basic feature pipeline
│   │   └── EnhancedFlightFeaturePipeline # Advanced features
│   └── selection/
│       └── HybridFeatureSelector     # Feature selection methods
│
├── ml/                               # Machine learning
│   ├── MLPipeline                    # Main ML pipeline orchestrator
│   ├── training/
│   │   ├── Trainer                   # Model training logic
│   │   └── CrossValidator            # K-fold CV + grid search
│   ├── evaluation/
│   │   └── ModelEvaluator            # Metrics calculation
│   ├── models/
│   │   ├── MLModel                   # Base model trait
│   │   ├── ModelFactory              # Model creation factory
│   │   └── RandomForestModel         # Random Forest implementation
│   └── tracking/
│       └── MLFlowTracker             # MLflow integration
│
└── utils/                            # Utilities
    ├── MetricsWriter                 # Metrics CSV writer
    └── CsvWriter                     # Generic CSV utilities
```

### Package Responsibilities

| Package | Responsibility | Dependencies |
|---------|---------------|--------------|
| `app` | Application orchestration | config, data, features, ml |
| `config` | Configuration loading/parsing | None (pure config) |
| `data` | Data loading and preprocessing | config |
| `features` | Feature extraction and engineering | config, data |
| `ml` | Model training and evaluation | config, features, utils |
| `utils` | Cross-cutting utilities | None |

---

## Data Flow

### Complete Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 1: DATA LOADING                                                 │
├─────────────────────────────────────────────────────────────────────┤
│ FlightDataLoader.loadFromConfiguration()                             │
│   ↓                                                                   │
│ • Load Flights CSV      (201201.csv)                                 │
│ • Load Weather TXT      (201201hourly.txt)                           │
│ • Load Airport Mapping  (wban_airport_timezone.csv)                  │
│   ↓                                                                   │
│ Raw DataFrame (~142K records, 21 flight columns + 44 weather cols)   │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 2: PREPROCESSING & FEATURE ENGINEERING                          │
├─────────────────────────────────────────────────────────────────────┤
│ FlightPreprocessingPipeline.execute()                                │
│   ↓                                                                   │
│ 1. FlightDataCleaner                                                 │
│    - Drop nulls in critical fields                                   │
│    - Handle outliers                                                 │
│    - Type conversions                                                │
│   ↓                                                                   │
│ 2. FlightLabelGenerator                                              │
│    - Generate label_is_delayed_15min                                 │
│    - Generate label_is_delayed_30min                                 │
│    - Generate label_is_delayed_45min                                 │
│    - Generate label_is_delayed_60min                                 │
│   ↓                                                                   │
│ 3. FlightDataLeakageCleaner                                          │
│    - Remove ArrDelay, DepDelay, etc.                                 │
│    - Keep only pre-flight features                                   │
│   ↓                                                                   │
│ 4. FlightDataGenerator                                               │
│    - Engineer derived features                                       │
│    - Time-based features                                             │
│    - Weather aggregations                                            │
│   ↓                                                                   │
│ Preprocessed DataFrame (~140K records, ~50 features + 4 labels)      │
│ Saved to: /output/common/data/processed_flights.parquet              │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│ FOR EACH EXPERIMENT                                                   │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 3: FEATURE EXTRACTION (Per Experiment)                          │
├─────────────────────────────────────────────────────────────────────┤
│ FlightFeatureExtractor.extract(data, experiment)                     │
│   ↓                                                                   │
│ • Select target label (e.g., label_is_delayed_15min)                 │
│ • Assemble feature vector (~50 features)                             │
│   ↓                                                                   │
│ IF PCA enabled:                                                       │
│   PCAFeatureExtractor.extract(data, variance_threshold=0.7)          │
│   - Standardize features (mean=0, std=1)                             │
│   - Apply PCA transformation                                         │
│   - Keep components explaining 70% variance (~12-15 components)      │
│   - Save variance analysis                                           │
│   ↓                                                                   │
│ Feature DataFrame (features: Vector, label: Double)                  │
│ Saved to: /output/{exp_name}/features/extracted_features/            │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 4: MODEL TRAINING (Per Experiment)                              │
├─────────────────────────────────────────────────────────────────────┤
│ MLPipeline.train(featuresData, experiment)                           │
│   ↓                                                                   │
│ 1. Initial Split (80/20)                                             │
│    Array(devData, testData) = data.randomSplit([0.8, 0.2])          │
│   ↓                                                                   │
│ 2. K-Fold Cross-Validation on Dev Set                               │
│    CrossValidator.validate(devData, experiment)                      │
│    - For each fold (1 to K=5):                                       │
│      • Split train/validation                                        │
│      • If grid search: try hyperparameter combinations               │
│      • Evaluate on validation set                                    │
│    - Compute mean ± std metrics                                      │
│    - Select best hyperparameters                                     │
│   ↓                                                                   │
│ 3. Train Final Model                                                 │
│    Trainer.trainFinal(devData, experiment, bestParams)               │
│    - Train on full 80% dev set                                       │
│    - Use best hyperparameters from CV                                │
│   ↓                                                                   │
│ 4. Hold-out Test Evaluation                                          │
│    predictions = model.transform(testData)                           │
│    ModelEvaluator.evaluate(predictions)                              │
│    - Accuracy, Precision, Recall, F1, AUC                            │
│    - Confusion matrix                                                │
│    - ROC curve data                                                  │
│   ↓                                                                   │
│ Trained Model + Metrics                                              │
│ Saved to: /output/{exp_name}/models/{model_type}_final/              │
│ Metrics to: /output/{exp_name}/metrics/*.csv                         │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 5: EXPERIMENT TRACKING                                          │
├─────────────────────────────────────────────────────────────────────┤
│ MLFlowTracker (throughout pipeline)                                  │
│   ↓                                                                   │
│ • Create/get experiment                                              │
│ • Start run                                                          │
│ • Log parameters (hyperparameters, config)                           │
│ • Log metrics (CV metrics, test metrics)                             │
│ • Log artifacts (model, CSVs, PCA analysis)                          │
│ • End run                                                            │
│   ↓                                                                   │
│ MLflow UI: http://localhost:5555                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Transformations

| Stage | Input | Transformation | Output |
|-------|-------|----------------|--------|
| **Load** | CSV files | Schema mapping, type casting | Raw DataFrame |
| **Clean** | Raw data | Null removal, outlier handling | Clean DataFrame |
| **Label** | Clean data | Generate 4 delay labels | Data + labels |
| **Leakage** | Labeled data | Remove post-flight columns | Pre-flight features only |
| **Generate** | Leakage-free data | Engineer derived features | Feature-rich DataFrame |
| **Extract** | Processed data | Assemble feature vector, select label | (features, label) |
| **PCA** | Feature vector | Standardize + PCA transform | Reduced-dimension features |
| **Train** | (features, label) | Fit model on train split | Trained model |
| **Evaluate** | Test data | Model predictions | Metrics + confusion matrix |

---

## Core Components

### 1. FlightDelayPredictionApp

**Purpose**: Main application orchestrator

**Responsibilities**:
- Initialize Spark session
- Load configuration from YAML
- Filter enabled experiments
- Execute pipeline tasks (load, preprocess, feature-extraction, train, evaluate)
- Handle errors per experiment (fail gracefully)
- Coordinate execution flow

**Key Methods**:
```scala
def main(args: Array[String]): Unit
  - Entry point
  - Creates Spark session
  - Loads configuration
  - Iterates over enabled experiments

private def runExperiment(
  experiment: ExperimentConfig,
  tasks: Set[String]
): Unit
  - Executes one experiment
  - Runs feature extraction
  - Runs model training
  - Handles experiment-specific errors
```

**Data Flow**:
1. Load configuration
2. Load and preprocess data (once for all experiments)
3. For each experiment:
   - Extract features (with optional PCA)
   - Train model (CV + hold-out)
   - Track to MLflow

---

### 2. ConfigurationLoader & AppConfiguration

**Purpose**: Configuration management

**ConfigurationLoader**:
- Loads YAML configuration files
- Validates configuration structure
- Handles environment-specific configs

**AppConfiguration**:
- Holds all configuration (common + experiments)
- Provides `enabledExperiments` filter
- Type-safe access to config values

**Configuration Hierarchy**:
```
AppConfiguration
├── environment: String
├── common: CommonConfig
│   ├── seed: Int
│   ├── data: DataConfig
│   ├── output: OutputConfig
│   └── mlflow: MLFlowConfig
└── experiments: Seq[ExperimentConfig]
    ├── name: String
    ├── description: String
    ├── enabled: Boolean
    ├── target: String
    ├── featureExtraction: FeatureExtractionConfig
    ├── model: ExperimentModelConfig
    └── train: TrainConfig
        ├── trainRatio: Double
        ├── crossValidation: CrossValidationConfig
        ├── gridSearch: GridSearchConfig
        └── hyperparameters: HyperparametersConfig
```

---

### 3. FlightDataLoader

**Purpose**: Load flight and weather data from CSV/TXT files

**Key Methods**:
```scala
def loadFromConfiguration()(implicit config: AppConfiguration): DataFrame
  - Loads flight CSV
  - Loads weather TXT
  - Loads airport mapping
  - Validates schemas
  - Returns combined DataFrame
```

**Schema Mapping**:
- Flight data: 21 columns (Year, Month, DayofMonth, Carrier, Origin, Dest, etc.)
- Weather data: 44 columns (Temperature, Visibility, Wind, Precipitation, etc.)
- Airport mapping: WBAN to airport code and timezone

---

### 4. FlightPreprocessingPipeline

**Purpose**: Complete preprocessing pipeline

**Pipeline Stages**:

1. **FlightDataCleaner**
   - Drop rows with nulls in critical fields
   - Handle outliers (clipping, capping)
   - Type conversions

2. **FlightLabelGenerator**
   - `label_is_delayed_15min = if (ArrDelay >= 15) 1.0 else 0.0`
   - Similar for 30, 45, 60 minutes

3. **FlightDataLeakageCleaner**
   - Remove: `ArrDelay`, `DepDelay`, `ArrTime`, `DepTime`
   - Keep only features available before flight

4. **FlightDataGenerator**
   - Time-based: hour of day, day of week, month
   - Weather aggregations: average temp, wind speed
   - Route features: origin-dest pairs

**Output**:
- Preprocessed DataFrame with ~50 features + 4 labels
- Saved to `/output/common/data/processed_flights.parquet`

---

### 5. FlightFeatureExtractor

**Purpose**: Feature extraction with optional PCA

**Key Methods**:
```scala
def extract(
  data: DataFrame,
  experiment: ExperimentConfig
)(implicit config: AppConfiguration): DataFrame
  - Selects target label
  - Assembles feature vector
  - Applies PCA if enabled
  - Saves extracted features
```

**PCA Pipeline** (when `featureExtraction.type == "pca"`):
1. Standardize features (StandardScaler)
2. Apply PCA transformation
3. Keep components explaining `pcaVarianceThreshold` variance (default: 70%)
4. Save variance analysis, loadings, and projections

---

### 6. MLPipeline

**Purpose**: Complete ML training pipeline

**Architecture**: Option B (Hold-out + K-fold CV)

**Key Methods**:
```scala
def train(
  data: DataFrame,
  experiment: ExperimentConfig
): MLResult
  - Split 80/20 (dev/test)
  - K-fold CV on dev set
  - Train final model on dev set
  - Evaluate on test set
  - Save model and metrics
```

**Pipeline Steps**:
1. **Initial split**: 80% dev, 20% test
2. **K-fold CV**: CrossValidator on dev set
3. **Final training**: Trainer on full dev set with best params
4. **Hold-out evaluation**: ModelEvaluator on test set
5. **Tracking**: MLFlowTracker logs everything

---

### 7. CrossValidator

**Purpose**: K-fold cross-validation with optional grid search

**Key Methods**:
```scala
def validate(
  data: DataFrame,
  experiment: ExperimentConfig
): CVResult
  - Performs K-fold split
  - If grid search: tries all hyperparameter combinations
  - Returns best params and per-fold metrics
```

**Output**:
```scala
case class CVResult(
  foldMetrics: Seq[EvaluationMetrics],
  avgMetrics: EvaluationMetrics,
  stdMetrics: EvaluationMetrics,
  bestHyperparameters: Map[String, Any],
  numFolds: Int
)
```

---

### 8. ModelFactory & MLModel

**Purpose**: Model creation and abstraction

**MLModel Trait**:
```scala
trait MLModel {
  def train(data: DataFrame): Transformer
  def getModel: Option[Transformer]
}
```

**ModelFactory**:
```scala
def createModel(
  modelType: String,
  hyperparameters: HyperparametersConfig
): MLModel
  - Factory method
  - Currently supports: "randomforest"
  - Extensible for GBT, LogisticRegression, etc.
```

**RandomForestModel**:
- Implements `MLModel`
- Configures RandomForestClassifier with hyperparameters
- Returns trained model

---

### 9. ModelEvaluator

**Purpose**: Compute evaluation metrics

**Key Methods**:
```scala
def evaluate(predictions: DataFrame): EvaluationMetrics
  - Computes accuracy, precision, recall, F1
  - Computes AUC-ROC, AUC-PR
  - Computes confusion matrix (TP, TN, FP, FN)
```

**Metrics**:
```scala
case class EvaluationMetrics(
  accuracy: Double,
  precision: Double,
  recall: Double,
  f1Score: Double,
  areaUnderROC: Double,
  areaUnderPR: Double,
  truePositives: Long,
  trueNegatives: Long,
  falsePositives: Long,
  falseNegatives: Long
)
```

---

### 10. MLFlowTracker

**Purpose**: MLflow experiment tracking integration

**Key Methods**:
```scala
def initialize(trackingUri: String, enabled: Boolean): Unit
  - Sets MLflow tracking URI
  - Creates MLflow client

def getOrCreateExperiment(name: String = "flight-delay-prediction"): Option[String]
  - Gets or creates MLflow experiment
  - Returns experiment ID

def startRun(experimentId: String, runName: String): Option[String]
  - Starts MLflow run
  - Returns run ID

def logParams(runId: String, params: Map[String, Any]): Unit
  - Logs hyperparameters and config

def logMetrics(runId: String, metrics: Map[String, Double]): Unit
  - Logs evaluation metrics

def logArtifact(runId: String, artifactPath: String): Unit
  - Logs model files, CSVs, etc.

def endRun(runId: String): Unit
  - Ends MLflow run
```

---

## Design Patterns

### 1. Factory Pattern

**Usage**: `ModelFactory`

**Purpose**: Create models dynamically based on configuration

```scala
ModelFactory.createModel(
  modelType = "randomforest",
  hyperparameters = config
)
```

**Benefits**:
- Easy to add new models (GBT, LogisticRegression)
- Centralized model creation
- Type-safe model instantiation

---

### 2. Trait-based Abstraction

**Usage**: `MLModel`, `DataLoader`, `DataPreprocessor`

**Purpose**: Define contracts for extensibility

```scala
trait MLModel {
  def train(data: DataFrame): Transformer
  def getModel: Option[Transformer]
}
```

**Benefits**:
- Polymorphism (different models with same interface)
- Testability (mock implementations)
- Clear contracts

---

### 3. Pipeline Pattern

**Usage**: `FlightPreprocessingPipeline`, Spark ML Pipelines

**Purpose**: Chain transformations

```scala
val pipeline = new Pipeline().setStages(Array(
  cleaner,
  labelGenerator,
  leakageCleaner,
  featureGenerator
))
```

**Benefits**:
- Composability
- Reusability
- Clear data flow

---

### 4. Dependency Injection

**Usage**: Implicit parameters throughout

**Purpose**: Pass Spark and config without explicit parameters

```scala
def train(data: DataFrame, experiment: ExperimentConfig)
  (implicit spark: SparkSession, config: AppConfiguration): MLResult
```

**Benefits**:
- Cleaner method signatures
- Consistent access to shared resources
- Testability (inject mock Spark session)

---

### 5. Configuration-Driven Execution

**Usage**: YAML configuration files

**Purpose**: Define experiments without code changes

```yaml
experiments:
  - name: "exp4_rf_pca_cv_15min"
    target: "label_is_delayed_15min"
    featureExtraction:
      type: pca
      pcaVarianceThreshold: 0.7
    train:
      hyperparameters:
        numTrees: [50, 100]
```

**Benefits**:
- Reproducibility
- Easy to run multiple experiments
- No code recompilation for config changes

---

### 6. Immutable Data Transformations

**Usage**: All DataFrame operations

**Purpose**: Spark DataFrames are immutable

```scala
val cleaned = data.filter("ArrDelay IS NOT NULL")
val labeled = cleaned.withColumn("label", ...)
```

**Benefits**:
- Thread safety
- Easier reasoning about data flow
- Spark optimization

---

## Integration Points

### 1. Spark Integration

**Entry Point**: `FlightDelayPredictionApp.main()`

```scala
implicit val spark: SparkSession = SparkSession.builder()
  .appName("Flight Delay Prediction App")
  .master("local[*]")
  .config("spark.sql.adaptive.enabled", "true")
  .getOrCreate()
```

**Usage**:
- All data processing uses Spark DataFrames
- Distributed computation across workers
- Lazy evaluation (transformations vs actions)

---

### 2. MLflow Integration

**Entry Point**: `MLFlowTracker.initialize()`

**Connection**:
```scala
MLFlowTracker.initialize(
  trackingUri = "http://mlflow-server:5000",
  enabled = true
)
```

**Logged Information**:
- **Parameters**: Hyperparameters, experiment config
- **Metrics**: CV metrics, test metrics
- **Artifacts**: Models, CSVs, PCA analysis
- **Tags**: Environment, description

**Access**: http://localhost:5555 (external port)

---

### 3. Docker Infrastructure Integration

**Container**: `spark-submit`

**Mounted Volumes**:
- `/data` → Host: `work/data/` (read-only data)
- `/output` → Host: `work/output/` (results)
- `/app` → Host: `work/apps/` (JARs)

**Network**: `spark-network` (shared with MLflow and Spark cluster)

**Job Submission**:
```bash
docker exec spark-submit spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 14G \
  --executor-memory 6G \
  /app/Emiasd-Flight-Data-Analysis.jar
```

---

### 4. File System Integration

**Input**:
- Flight CSV: `/data/FLIGHT-3Y/Flights/201201.csv`
- Weather TXT: `/data/FLIGHT-3Y/Weather/201201hourly.txt`
- Airport Mapping: `/data/FLIGHT-3Y/wban_airport_timezone.csv`

**Output**:
- Preprocessed data: `/output/common/data/processed_flights.parquet`
- Per-experiment:
  - Features: `/output/{exp_name}/features/extracted_features/`
  - Models: `/output/{exp_name}/models/{model_type}_final/`
  - Metrics: `/output/{exp_name}/metrics/*.csv`
  - PCA analysis: `/output/{exp_name}/metrics/pca_variance.csv`

---

## Extension Points

### Adding a New Model

1. Create class in `ml/models/` implementing `MLModel`
2. Register in `ModelFactory`
3. Update configuration with new model type
4. See [Adding Models Guide](10-adding-models.md)

### Adding a New Feature

1. Add feature generation logic to `FlightDataGenerator`
2. Feature automatically included in feature vector
3. PCA will automatically handle new dimensions

### Adding a New Preprocessing Step

1. Create preprocessor in `data/preprocessing/`
2. Add to `FlightPreprocessingPipeline`
3. Chain transformations

### Adding a New Evaluation Metric

1. Add metric calculation to `ModelEvaluator.evaluate()`
2. Update `EvaluationMetrics` case class
3. Log to MLflow in `MLPipeline`

---

## Next Steps

- **[Configuration Guide](05-configuration.md)** - Understand all configuration options
- **[Data Pipeline](06-data-pipeline.md)** - Deep dive into data loading and preprocessing
- **[Feature Engineering](07-feature-engineering.md)** - Learn about PCA and feature extraction
- **[ML Pipeline](08-ml-pipeline.md)** - Understand training and evaluation
- **[Adding Models](10-adding-models.md)** - Step-by-step guide to add new models

---

**Architecture Overview Complete! Ready to explore specific components.** 🏗️
