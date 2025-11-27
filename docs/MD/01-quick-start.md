# 🚀 Quick Start Guide

Get up and running with Flight Delay Prediction in 5 minutes!

---

## Prerequisites

Before starting, ensure you have:

- **Docker Desktop** installed and running
  - Download from [docker.com](https://www.docker.com/products/docker-desktop)
- **24GB RAM** minimum (32GB recommended)
- **20GB free disk space**
- **Internet connection** (for downloading Docker images)

### Verify Docker Installation

```bash
docker --version
# Should show: Docker version 20.x or higher

docker-compose --version
# Should show: Docker Compose version 2.x or higher
```

---

## Step 1: Clone the Repository

```bash
git clone <repository-url>
cd Emiasd-FlightProject
```

---

## Step 2: Start the Infrastructure

The Docker infrastructure includes:
- Spark cluster (1 master + 4 workers)
- MLflow tracking server
- JupyterLab environment

```bash
cd docker
./setup.sh
```

**What this does:**
1. Creates necessary directories
2. Pulls Docker images (first time only, ~5-10 minutes)
3. Starts all services
4. Performs health checks

**Expected output:**
```
🚀 Configuration du cluster Spark avec 4 workers
=================================================
ℹ️  Vérification des fichiers requis...
ℹ️  Validation de la configuration Docker Compose...
✅ docker-compose.yml est valide
🧹 Voulez-vous faire un nettoyage complet ? (y/N): n
ℹ️  Création des répertoires...
✅ Répertoires créés
ℹ️  Démarrage des services...
✅ Cluster Spark démarré avec succès !

📊 Services disponibles:
  - Spark Master UI: http://localhost:8080
  - MLflow UI:       http://localhost:5555
  - Jupyter Lab:     http://localhost:8888
```

### Verify Services are Running

```bash
# Check all services are up
docker ps

# Should show 7 containers running:
# - spark-master
# - spark-worker-1, spark-worker-2, spark-worker-3, spark-worker-4
# - mlflow-server
# - jupyter-spark
```

**Access the UIs:**
- **Spark Master**: http://localhost:8080 (check cluster status)
- **MLflow**: http://localhost:5555 (experiment tracking)
- **Jupyter**: http://localhost:8888 (notebooks)

---

## Step 3: Submit Your First Experiment

Run the ML pipeline with default configuration:

```bash
# From the docker/ directory
./local-submit.sh
```

**What this does:**
1. Loads flight and weather data from `/data`
2. Preprocesses and cleans the data
3. Generates labels for delay prediction
4. Extracts features with PCA dimensionality reduction
5. Trains Random Forest model with 5-fold cross-validation
6. Evaluates on hold-out test set
7. Logs everything to MLflow

**Expected output:**
```
================================================================================
[STEP 1] Data Loading
================================================================================
Loading flight data from: /data/FLIGHT-3Y/Flights/201201.csv
  ✓ Loaded 142,030 flight records

================================================================================
[STEP 2] Data Preprocessing
================================================================================
[Preprocessing] Starting pipeline...
  ✓ Dropped 2,145 records with missing critical fields
  ✓ Generated 4 delay labels
  ✓ Removed data leakage columns

================================================================================
[STEP 3] Feature Extraction
================================================================================
[Feature Extraction] Using PCA with 70% variance threshold
  - Original features: 48
  - PCA components: 12
  - Variance explained: 71.23%

================================================================================
[STEP 4] Model Training
================================================================================
[ML PIPELINE] Starting for experiment: exp4_rf_pca_cv_15min

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

✓ Total pipeline time: 287.45 seconds
```

---

## Step 4: View Results

### Option A: MLflow UI (Recommended)

1. Open http://localhost:5555 in your browser
2. Click on the experiment that just ran
3. Explore:
   - **Metrics**: Accuracy, F1, AUC, etc.
   - **Parameters**: Model configuration
   - **Artifacts**: Trained models, CSV files

**MLflow Features:**
- Compare multiple experiments
- Download trained models
- View ROC curves
- Filter by metrics (`test_f1 > 0.85`)

### Option B: File System

Results are saved to `/output/<experiment_name>/`:

```bash
# From your host machine
ls -la work/output/exp4_rf_pca_cv_15min/

# You'll see:
# ├── features/
# │   └── extracted_features/   # Processed features
# ├── models/
# │   └── randomforest_final/   # Trained model
# └── metrics/
#     ├── cv_metrics.csv        # Cross-validation results
#     ├── holdout_metrics.csv   # Test set results
#     ├── pca_variance.csv      # PCA analysis
#     └── holdout_roc_data.csv  # ROC curve data
```

### Option C: Visualization Scripts

```bash
# Generate comparison visualizations
python work/scripts/visualize_experiments_comparison.py /output

# View PCA analysis
python work/scripts/visualize_pca.py /output/exp4_rf_pca_cv_15min/metrics/pca_analysis
```

---

## Step 5: Run More Experiments

### Modify Configuration

Edit `src/main/resources/local-config.yml`:

```yaml
experiments:
  # Experiment 1: Predict 15-minute delays
  - name: "exp4_rf_pca_cv_15min"
    enabled: true
    target: "label_is_delayed_15min"

  # Experiment 2: Predict 30-minute delays
  - name: "exp5_rf_pca_cv_30min"
    enabled: true  # ← Enable this
    target: "label_is_delayed_30min"
```

### Recompile and Run

```bash
# Rebuild the JAR
sbt package

# Run experiments
cd docker
./local-submit.sh
```

---

## Common Commands

### Start/Stop Cluster

```bash
cd docker

# Start all services
./local-start.sh

# Stop all services
./local-stop.sh

# Restart
./local-restart.sh

# View logs
./logs.sh mlflow-server
./logs.sh spark-master
```

### Submit Jobs

```bash
# Run with default config
./local-submit.sh

# Run specific tasks
./local-submit.sh load,preprocess
./local-submit.sh train
```

### Access Spark Shell

```bash
# Interactive Spark shell
./shell.sh
```

---

## Troubleshooting

### Problem: Docker services won't start

**Solution:**
```bash
# Check Docker is running
docker info

# Restart Docker Desktop

# Clean up old containers
cd docker
./cleanup.sh

# Try again
./setup.sh
```

### Problem: Out of memory errors

**Solution:**
Increase Docker memory allocation:
1. Docker Desktop → Settings → Resources
2. Set Memory to 16GB or more
3. Apply & Restart

### Problem: Port already in use

**Solution:**
```bash
# Find what's using port 8080
lsof -i :8080

# Kill the process or change port in docker-compose.yml
```

### Problem: MLflow shows no experiments

**Solution:**
1. Check MLflow is running: http://localhost:5555
2. Verify the experiment ran successfully (check logs)
3. Ensure MLflow is enabled in `local-config.yml`:
   ```yaml
   mlflow:
     enabled: true
     trackingUri: "http://mlflow-server:5000"
   ```

---

## Next Steps

Now that you have the basics working:

1. **📖 Read the Documentation**
   - [Docker Infrastructure](03-docker-infrastructure.md) - Detailed Docker guide
   - [Configuration](05-configuration.md) - All configuration options
   - [ML Pipeline](08-ml-pipeline.md) - Understanding the training pipeline

2. **🔬 Run More Experiments**
   - Try different delay thresholds (15, 30, 45, 60 minutes)
   - Adjust PCA variance threshold
   - Tune hyperparameters (numTrees, maxDepth)

3. **📊 Analyze Results**
   - Compare experiments in MLflow
   - Generate visualizations
   - Identify best performing model

4. **🛠️ Customize**
   - Add new models (see [Adding Models](10-adding-models.md))
   - Implement feature selection
   - Try different preprocessing strategies

---

## Quick Reference

| Task | Command |
|------|---------|
| Start cluster | `cd docker && ./setup.sh` |
| Stop cluster | `cd docker && ./stop.sh` |
| Run pipeline | `cd docker && ./submit.sh` |
| View logs | `cd docker && ./logs.sh <service>` |
| Rebuild JAR | `sbt package` |
| Access Spark UI | http://localhost:8080 |
| Access MLflow | http://localhost:5555 |
| Access Jupyter | http://localhost:8888 |

---

## Success Checklist

- ✅ Docker services running (7 containers)
- ✅ Spark Master UI accessible (http://localhost:8080)
- ✅ MLflow UI accessible (http://localhost:5555)
- ✅ First experiment completed successfully
- ✅ Results visible in MLflow
- ✅ Can run `./submit.sh` without errors

**You're ready to go! 🚀**

For detailed information on any topic, check the [full documentation](../MD/).
