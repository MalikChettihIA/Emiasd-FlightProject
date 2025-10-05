# ✈️ Flight Delay Prediction using Weather Data

[![Scala](https://img.shields.io/badge/Scala-2.12.18-red.svg)](https://www.scala-lang.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.3-orange.svg)](https://spark.apache.org/)
[![MLflow](https://img.shields.io/badge/MLflow-3.4.0-blue.svg)](https://mlflow.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)](https://www.docker.com/)

A scalable machine learning system for predicting flight delays based on weather conditions using Apache Spark and Scala. This project implements the methodology from the academic paper ["Using Scalable Data Mining for Predicting Flight Delays"](https://www.dropbox.com/s/4rqnjueuqi5e0uo/TIST-Flight-Delay-final.pdf) (ACM TIST, 2016).

---

## 🎯 Project Overview

This system predicts flight delays by analyzing historical flight data combined with weather observations from origin and destination airports. The solution processes large-scale datasets using Apache Spark, implements sophisticated data preprocessing pipelines, and trains Random Forest classifiers with cross-validation and hyperparameter tuning.

### Key Features

- **✅ Scalable Data Processing** - Handles millions of flights with Spark distributed computing
- **✅ Advanced Feature Engineering** - PCA dimensionality reduction with variance-based selection
- **✅ Robust ML Pipeline** - K-fold cross-validation with grid search hyperparameter tuning
- **✅ Comprehensive Evaluation** - Multiple metrics, ROC curves, and detailed analysis
- **✅ Experiment Tracking** - MLflow integration for experiment management
- **✅ Docker Infrastructure** - Complete containerized environment with Spark cluster
- **✅ Visualization Tools** - Python scripts for metrics analysis and comparison

### Target Performance

- **Accuracy**: 85.8% for 60+ minute delays
- **Recall**: 86.9% for critical delay detection
- **Training Time**: < 5 minutes on 4-worker Spark cluster

---

## 📊 Datasets

The project uses three primary datasets:

| Dataset | Description | Size | Features |
|---------|-------------|------|----------|
| **Flights** | Historical flight records with delay information | ~142K flights | 21 features |
| **Weather** | Hourly weather observations at airports | Variable | 44 meteorological features |
| **Airport Mapping** | WBAN-to-Airport timezone mapping | 305 airports | Coordinate data |

**Data Source**: [Flight Delay Dataset](https://www.dropbox.com/sh/iasq7frk6cusSNfqYNYsnLGIXa)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Infrastructure                      │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Spark Master│  │ 4x Workers   │  │ MLflow Server│       │
│  │   :8080     │  │ :8081-8084   │  │   :5555      │       │
│  └─────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
                             ↓
┌─────────────────────────────────────────────────────────────┐
│                      ML Pipeline                              │
├─────────────────────────────────────────────────────────────┤
│  1. Data Loading → 2. Preprocessing → 3. Feature Engineering │
│  4. Model Training → 5. Evaluation → 6. MLflow Tracking      │
└─────────────────────────────────────────────────────────────┘
```

**Technology Stack**:
- **Language**: Scala 2.12.18
- **Big Data**: Apache Spark 3.5.3
- **ML Library**: Spark MLlib
- **Experiment Tracking**: MLflow 3.4.0
- **Containerization**: Docker & Docker Compose
- **Visualization**: Python (matplotlib, seaborn, scikit-learn)

---

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- 16GB+ RAM recommended
- 20GB+ free disk space

### Setup and Run

```bash
# 1. Clone the repository
git clone <repository-url>
cd Emiasd-FlightProject

# 2. Start Docker infrastructure (Spark + MLflow)
cd docker
./setup.sh

# 3. Submit your first experiment
./submit.sh

# 4. View results
# - Spark UI: http://localhost:8080
# - MLflow UI: http://localhost:5555
```

**That's it!** The system will automatically:
- Load and preprocess flight and weather data
- Generate features with PCA dimensionality reduction
- Train Random Forest model with 5-fold cross-validation
- Track all experiments in MLflow
- Save trained models and metrics

---

## 📖 Documentation

| Guide | Description |
|-------|-------------|
| [Quick Start](docs/MD/01-quick-start.md) | Get up and running in 5 minutes |
| [Installation](docs/MD/02-installation.md) | Detailed setup instructions |
| [Docker Infrastructure](docs/MD/03-docker-infrastructure.md) | Docker architecture and usage |
| [Project Architecture](docs/MD/04-project-architecture.md) | System design and components |
| [Configuration](docs/MD/05-configuration.md) | Configure experiments and parameters |
| [Data Pipeline](docs/MD/06-data-pipeline.md) | Data loading and preprocessing |
| [Feature Engineering](docs/MD/07-feature-engineering.md) | Feature extraction and PCA |
| [ML Pipeline](docs/MD/08-ml-pipeline.md) | Model training and evaluation |
| [MLflow Integration](docs/MD/09-mlflow-integration.md) | Experiment tracking with MLflow |
| [Adding Models](docs/MD/10-adding-models.md) | How to implement new models |
| [Code Reference](docs/MD/11-code-reference.md) | Class-by-class documentation |
| [Visualization](docs/MD/12-visualization.md) | Analyze and visualize results |

---

## 🔬 Experiments

The project supports running multiple experiments with different configurations:

```yaml
# Example: src/main/resources/local-config.yml
experiments:
  - name: "exp4_rf_pca_cv_15min"
    target: "label_is_delayed_15min"  # Predict 15+ min delays
    featureExtraction:
      type: pca
      pcaVarianceThreshold: 0.7       # Keep 70% variance
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
```

**Supported Delay Thresholds**:
- 15 minutes (`label_is_delayed_15min`)
- 30 minutes (`label_is_delayed_30min`)
- 45 minutes (`label_is_delayed_45min`)
- 60 minutes (`label_is_delayed_60min`)

---

## 📊 Results & Metrics

After training, the system generates:

### Metrics
- **Cross-Validation**: Mean ± Std for accuracy, precision, recall, F1, AUC
- **Hold-out Test**: Final performance on unseen data
- **Per-Fold Analysis**: Detailed breakdown of CV performance
- **ROC Curves**: Model discrimination ability

### Artifacts
- Trained Spark ML models (`.parquet` format)
- Feature importance rankings
- PCA variance analysis
- Confusion matrices
- Comparison visualizations

### Example Output

```
================================================================================
[ML PIPELINE] Completed for experiment: exp4_rf_pca_cv_15min
================================================================================

Cross-Validation Results (5 folds):
  Accuracy:   87.32% ± 1.23%
  Precision:  85.67% ± 2.10%
  Recall:     88.45% ± 1.87%
  F1-Score:   87.02% ± 1.56%
  AUC-ROC:    0.9234 ± 0.0156

Hold-out Test Metrics:
  Accuracy:   87.89%
  Precision:  86.12%
  Recall:     89.23%
  F1-Score:   87.65%
  AUC-ROC:    0.9301

Total pipeline time: 287.45 seconds
```

---

## 🐳 Docker Infrastructure

The project includes a complete Docker-based infrastructure:

### Services

| Service | Port | Description |
|---------|------|-------------|
| **spark-master** | 8080 | Spark Master Web UI |
| **spark-worker-1..4** | 8081-8084 | 4 Worker nodes (6GB RAM each) |
| **mlflow-server** | 5555 | MLflow Tracking Server |
| **jupyter** | 8888 | JupyterLab with PySpark |

### Management Scripts

```bash
cd docker

# Setup and start cluster
./setup.sh              # Interactive setup with cleanup option

# Manage cluster
./start.sh              # Start all services
./stop.sh               # Stop all services
./restart.sh            # Restart all services
./logs.sh [service]     # View logs

# Submit jobs
./submit.sh             # Run ML pipeline
./shell.sh              # Access Spark shell

# Cleanup
./cleanup.sh            # Remove stopped containers and volumes
```

**See [Docker Infrastructure Guide](docs/MD/03-docker-infrastructure.md) for details**

---

## 🧪 MLflow Integration

All experiments are automatically tracked in MLflow:

### Logged Information

**Parameters**:
- Experiment configuration (target, model type, etc.)
- Hyperparameters (numTrees, maxDepth, etc.)
- Feature extraction settings (PCA variance threshold)
- Random seeds and train/test splits

**Metrics**:
- Per-fold CV metrics (accuracy, precision, recall, F1, AUC)
- Aggregated CV metrics (mean ± std)
- Hold-out test metrics
- Training time

**Artifacts**:
- Trained models (Spark ML format)
- Metrics CSV files
- PCA analysis (variance, loadings, projections)
- ROC curve data

### MLflow UI

Access at **http://localhost:5555**

- Compare experiments side-by-side
- Filter by metrics (`test_f1 > 0.85`)
- Download models and artifacts
- Visualize metrics evolution

**See [MLflow Integration Guide](docs/MD/09-mlflow-integration.md) for details**

---

## 🔧 Configuration

Experiments are configured via YAML files in `src/main/resources/`:

- `local-config.yml` - Local development environment
- `lamsade-config.yml` - Production cluster configuration

### Key Configuration Sections

```yaml
common:
  seed: 42                    # Reproducibility
  data:                       # Dataset paths
    basePath: "/data"
  mlflow:                     # MLflow settings
    enabled: true
    trackingUri: "http://mlflow-server:5000"

experiments:                  # List of experiments
  - name: "exp_name"
    target: "label_..."       # Target variable
    featureExtraction:        # Feature engineering
      type: "pca"
    train:                    # Training configuration
      trainRatio: 0.8
      crossValidation:
        numFolds: 5
      hyperparameters:
        numTrees: [50]
```

**See [Configuration Guide](docs/MD/05-configuration.md) for all options**

---

## 🛠️ Development

### Project Structure

```
Emiasd-FlightProject/
├── docker/                      # Docker infrastructure
│   ├── docker-compose.yml       # Service definitions
│   ├── setup.sh                 # Setup script
│   └── submit.sh                # Job submission
├── src/main/scala/com/flightdelay/
│   ├── app/                     # Main application
│   ├── config/                  # Configuration classes
│   ├── data/                    # Data loading & preprocessing
│   │   ├── loaders/            # Data loaders
│   │   └── preprocessing/      # Preprocessing pipeline
│   ├── features/                # Feature engineering
│   │   ├── pipelines/          # Feature pipelines
│   │   └── pca/                # PCA implementation
│   ├── ml/                      # Machine learning
│   │   ├── models/             # Model implementations
│   │   ├── training/           # Training logic
│   │   ├── evaluation/         # Model evaluation
│   │   └── tracking/           # MLflow tracking
│   └── utils/                   # Utilities
├── work/                        # Working directory
│   ├── apps/                    # JARs and libraries
│   ├── scripts/                 # Python visualization scripts
│   ├── data/                    # Input data (mounted)
│   └── output/                  # Results (mounted)
└── docs/MD/                     # Documentation
```

### Adding a New Model

See [Adding Models Guide](docs/MD/10-adding-models.md) for step-by-step instructions.

Quick overview:

1. Create model class in `ml/models/` extending `MLModel` trait
2. Implement `train()` and `getModel()` methods
3. Register in `ModelFactory`
4. Update configuration with new model type
5. Test with experiments

---

## 📈 Visualization

Python scripts for analyzing results:

```bash
# Compare multiple experiments
python work/scripts/visualize_experiments_comparison.py /output

# Visualize single experiment metrics
python work/scripts/visualize_metrics.py /output/exp_name/metrics

# Analyze PCA components
python work/scripts/visualize_pca.py /output/exp_name/metrics/pca_analysis

# Cross-validation analysis
python work/scripts/visualize_cv.py /output/exp_name/metrics
```

**Generated Visualizations**:
- Performance comparison heatmaps
- ROC curves comparison
- Cross-validation stability charts
- Feature importance rankings
- PCA variance explained plots
- Confusion matrices

---

## 🤝 Contributing

Contributions are welcome! Areas for improvement:

- [ ] Implement additional models (GBT, Logistic Regression, etc.)
- [ ] Add feature selection methods
- [ ] Improve data balancing strategies
- [ ] Add real-time prediction API
- [ ] Enhance visualization dashboards
- [ ] Add unit and integration tests

---

## 📝 Citation

If you use this project in your research, please cite the original paper:

```bibtex
@article{flightdelay2016,
  title={Using Scalable Data Mining for Predicting Flight Delays},
  journal={ACM Transactions on Intelligent Systems and Technology (TIST)},
  year={2016}
}
```

---

## 📄 License

This project is for educational and research purposes.

---

## 🙏 Acknowledgments

- Based on the methodology from ACM TIST 2016 paper
- Built with Apache Spark and MLlib
- Uses MLflow for experiment tracking
- Docker infrastructure for reproducibility

---

## 📞 Support

For questions or issues:

1. Check the [documentation](docs/MD/)
2. Review [Code Reference](docs/MD/11-code-reference.md)
3. Open an issue on GitHub

---

**Happy Flight Delay Prediction! ✈️**
