# MLFlow Integration with Docker Spark Cluster

## Overview

This document explains how MLFlow is integrated into the Spark cluster for artifact logging.

## Problem Solved

**Initial Issue**: The Spark MLFlow JARs (`mlflow-client` and `mlflow-spark`) could log parameters and metrics to MLFlow via REST API, but **could not log artifacts** (models, CSV files, plots) because they require the Python MLFlow CLI.

**Error Before Fix**:
```
Failed to exec 'python -m mlflow.store.artifact.cli'
Please make sure mlflow is available on your local system path
```

## Solution

### 1. Custom Docker Image

Created `docker/Dockerfile.spark-mlflow` that extends `bitnami/spark:3.5.3` with:
- ✅ Python 3 and pip
- ✅ MLFlow Python 3.4.0 (matching MLFlow server version)
- ✅ All dependencies for artifact logging

**File**: `docker/Dockerfile.spark-mlflow`

```dockerfile
FROM bitnami/spark:3.5.3
USER root
RUN apt-get update && \
    apt-get install -y python3-pip python3-dev && \
    pip3 install mlflow==3.4.0
USER 1001
```

### 2. Docker Compose Changes

**Modified Services**:
- `spark-submit`: Uses custom image + mounted `/mlflow` volume
- `spark-worker-1/2/3/4`: Uses custom image (for distributed artifact logging)

**Key Changes in `docker-compose.yml`**:

```yaml
spark-submit:
  build:
    context: .
    dockerfile: Dockerfile.spark-mlflow
  image: custom-spark-mlflow:3.5.3
  volumes:
    - ../work/mlflow:/mlflow  # ← NEW: Shared with mlflow-server
```

**Why This Works**:
- Both `spark-submit` and `mlflow-server` now share the same `/mlflow` directory
- When Spark logs artifacts via Python MLFlow CLI, they're written to `/mlflow/artifacts`
- MLFlow server reads from the same `/mlflow/artifacts` directory
- ✅ Artifacts are immediately visible in MLFlow UI

## Architecture

```
┌─────────────────────┐         ┌──────────────────────┐
│   spark-submit      │         │   mlflow-server      │
│                     │         │                      │
│  JVM (Spark/Scala)  │         │  Python MLFlow UI    │
│        ↓            │         │         ↑            │
│  MLFlow Scala JARs  │         │         │            │
│        ↓            │         │         │            │
│  Python MLFlow CLI  │ ─────→  │  reads artifacts     │
│        ↓            │  writes │         │            │
│  /mlflow/artifacts ←┼─────────┼─────────┘            │
└─────────────────────┘  shared └──────────────────────┘
         ↑               volume
         │
    ../work/mlflow
    (host filesystem)
```

## Usage

### First Time Setup / After Changes

```bash
cd docker
./rebuild-and-local-restart.sh
```

This script will:
1. Stop all containers
2. Build the custom `custom-spark-mlflow:3.5.3` image
3. Start all services with new configuration
4. Wait for health checks

### Verify MLFlow Installation

```bash
docker exec spark-submit python3 -c "import mlflow; print(f'MLFlow {mlflow.__version__}')"
```

**Expected Output**:
```
MLFlow 3.4.0
```

### Run Your ML Pipeline

```bash
./local-submit.sh
```

**Expected Behavior**:
- ✅ No warnings about "Failed to log artifact"
- ✅ Artifacts visible in MLFlow UI (http://localhost:5555)
- ✅ Models, metrics CSV, ROC curves all logged

## File Structure

```
work/
├── mlflow/                      # Shared MLFlow directory
│   ├── mlflow.db               # MLFlow tracking database (SQLite)
│   └── artifacts/              # MLFlow artifacts storage
│       └── {experiment_id}/
│           └── {run_id}/
│               └── artifacts/
│                   ├── metrics/          # CSV files
│                   ├── models/           # Spark ML models
│                   └── plots/            # ROC curves, etc.
└── output/                      # Experiment outputs (local copy)
    └── Experience-X/
        ├── metrics/
        └── models/
```

## Troubleshooting

### Problem: Artifacts not visible in MLFlow UI

**Check 1**: Verify volume is mounted
```bash
docker exec spark-submit ls -la /mlflow/artifacts/
docker exec mlflow-server ls -la /mlflow/artifacts/
```

Both should show the same content.

**Check 2**: Verify MLFlow is installed
```bash
docker exec spark-submit python3 -m mlflow.store.artifact.cli --help
```

Should show help message, not "command not found".

### Problem: Build fails

**Solution**: Clear Docker cache and rebuild
```bash
cd docker
docker-compose build --no-cache spark-submit
```

### Problem: Containers won't start after rebuild

**Check**: Ensure old containers are stopped
```bash
docker-compose down
docker ps -a | grep spark
```

If containers still exist:
```bash
docker rm -f spark-submit spark-worker-1 spark-worker-2 spark-worker-3 spark-worker-4
```

## Performance Notes

- MLFlow Python CLI is only invoked at the **end of training** for artifact logging
- No performance impact during training/cross-validation
- Artifact upload is asynchronous (doesn't block pipeline completion)

## Version Compatibility

| Component | Version | Notes |
|-----------|---------|-------|
| Spark | 3.5.3 | Bitnami base image |
| MLFlow Server | 3.4.0 | Docker image |
| MLFlow Python | 3.4.0 | **Must match server version** |
| MLFlow Scala JARs | 3.4.0 | In `lib/` directory |

⚠️ **Important**: Keep all MLFlow versions synchronized to avoid API compatibility issues.

## References

- [MLFlow Documentation](https://mlflow.org/docs/latest/index.html)
- [MLFlow Artifact Stores](https://mlflow.org/docs/latest/tracking.html#artifact-stores)
- [Bitnami Spark Docker Image](https://hub.docker.com/r/bitnami/spark)

## Changelog

### 2025-10-19
- ✅ Created custom Dockerfile with MLFlow Python
- ✅ Updated docker-compose.yml for all Spark services
- ✅ Added shared `/mlflow` volume for artifact storage
- ✅ Created rebuild script for easy updates
- ✅ Tested end-to-end artifact logging
