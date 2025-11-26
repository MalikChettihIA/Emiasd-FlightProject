# 🐳 Docker Infrastructure Guide

Complete guide to the Docker-based infrastructure for Flight Delay Prediction.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Services](#services)
- [Docker Compose Configuration](#docker-compose-configuration)
- [Management Scripts](#management-scripts)
- [Networking](#networking)
- [Volumes and Data Persistence](#volumes-and-data-persistence)
- [Resource Management](#resource-management)
- [Troubleshooting](#troubleshooting)

---

## Overview

The project uses Docker Compose to orchestrate a complete distributed computing environment including:

- **Spark Cluster**: 1 master + 4 workers for distributed processing
- **MLflow Server**: Experiment tracking and model registry
- **Jupyter Lab**: Interactive notebook environment
- **Shared Storage**: Mounted volumes for data and results

**Benefits:**
- ✅ Consistent environment across machines
- ✅ Easy setup and teardown
- ✅ Isolated dependencies
- ✅ Scalable worker configuration
- ✅ Reproducible experiments

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Docker Network: spark-network              │
├──────────────────────────────────────────────────────────────┤
│                                                                │
│  ┌─────────────────┐         ┌─────────────────┐            │
│  │  Spark Master   │◄────────│  Spark Submit   │            │
│  │  :8080, :7077   │         │  (Job Client)   │            │
│  └────────┬────────┘         └─────────────────┘            │
│           │                                                   │
│           │ Manages                                          │
│           ▼                                                   │
│  ┌────────────────────────────────────────────┐             │
│  │          Spark Workers (x4)                 │             │
│  │  ┌──────────┐  ┌──────────┐               │             │
│  │  │ Worker-1 │  │ Worker-2 │               │             │
│  │  │  :8081   │  │  :8082   │               │             │
│  │  │  6GB RAM │  │  6GB RAM │               │             │
│  │  │  2 cores │  │  2 cores │               │             │
│  │  └──────────┘  └──────────┘               │             │
│  │  ┌──────────┐  ┌──────────┐               │             │
│  │  │ Worker-3 │  │ Worker-4 │               │             │
│  │  │  :8083   │  │  :8084   │               │             │
│  │  │  6GB RAM │  │  6GB RAM │               │             │
│  │  │  2 cores │  │  2 cores │               │             │
│  │  └──────────┘  └──────────┘               │             │
│  └────────────────────────────────────────────┘             │
│                                                                │
│  ┌─────────────────┐         ┌─────────────────┐            │
│  │  MLflow Server  │         │  Jupyter Lab    │            │
│  │  :5555          │         │  :8888          │            │
│  │  Tracking UI    │         │  Notebooks      │            │
│  └─────────────────┘         └─────────────────┘            │
│                                                                │
└──────────────────────────────────────────────────────────────┘
                        ▲                    ▲
                        │                    │
                   Host Ports          Mounted Volumes
                   (8080, 5555...)     (/data, /output)
```

---

## Services

### 1. Spark Master

**Purpose**: Cluster coordinator and resource manager

**Configuration:**
```yaml
spark-master:
  image: bitnami/spark:3.5.3
  container_name: spark-master
  hostname: spark-master
  ports:
    - "8080:8080"  # Web UI
    - "7077:7077"  # Master port
  environment:
    - SPARK_MODE=master
    - SPARK_RPC_AUTHENTICATION_ENABLED=no
    - SPARK_RPC_ENCRYPTION_ENABLED=no
```

**Responsibilities:**
- Accepts worker registrations
- Schedules jobs across workers
- Provides Web UI for cluster monitoring
- Manages resource allocation

**Access:**
- Web UI: http://localhost:8080
- Master URL: `spark://spark-master:7077`

---

### 2. Spark Workers (x4)

**Purpose**: Execute distributed computations

**Configuration per worker:**
```yaml
spark-worker-1:
  image: bitnami/spark:3.5.3
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_MEMORY=6G
    - SPARK_WORKER_CORES=2
    - SPARK_WORKER_WEBUI_PORT=8081
  ports:
    - "8081:8081"  # Worker 1 UI
```

**Resources per worker:**
- **Memory**: 6GB RAM
- **Cores**: 2 CPU cores
- **Total cluster**: 24GB RAM, 8 cores

**Worker UIs:**
- Worker 1: http://localhost:8081
- Worker 2: http://localhost:8082
- Worker 3: http://localhost:8083
- Worker 4: http://localhost:8084

---

### 3. Spark Submit

**Purpose**: Client for submitting Spark jobs

**Configuration:**
```yaml
spark-submit:
  image: bitnami/spark:3.5.3
  container_name: spark-submit
  environment:
    - SPARK_MODE=submit
    - SPARK_MASTER_URL=spark://spark-master:7077
  volumes:
    - ../work/data:/data
    - ../work/apps:/apps
    - ../work/output:/output
    - ../work/scripts:/scripts
  command: tail -f /dev/null  # Keep alive
```

**Usage:**
```bash
# Execute via management script
./docker/local-submit.sh

# Or directly
docker exec spark-submit spark-submit \
  --master spark://spark-master:7077 \
  --class com.flightdelay.app.FlightDelayPredictionApp \
  /apps/Emiasd-Flight-Data-Analysis.jar \
  local
```

---

### 4. MLflow Server

**Purpose**: Experiment tracking and model registry

**Configuration:**
```yaml
mlflow:
  image: ghcr.io/mlflow/mlflow:v3.4.0
  container_name: mlflow-server
  hostname: mlflow-server
  ports:
    - "5555:5000"
  volumes:
    - ../work/mlflow:/mlflow
    - ../work/mlflow/artifacts:/mlflow/artifacts
  command: >
    mlflow server
    --backend-store-uri sqlite:///mlflow/mlflow.db
    --default-artifact-root /mlflow/artifacts
    --host 0.0.0.0
    --port 5000
```

**Features:**
- **Backend Store**: SQLite database for metadata
- **Artifact Store**: Local filesystem for models/files
- **Web UI**: http://localhost:5555

**Integration:**
- Spark jobs connect via `http://mlflow-server:5000`
- Logs parameters, metrics, artifacts automatically

---

### 5. Jupyter Lab

**Purpose**: Interactive development environment

**Configuration:**
```yaml
jupyter:
  build:
    context: ..
    dockerfile: docker/Dockerfile.jupyter
  container_name: jupyter-spark
  ports:
    - "8888:8888"     # Jupyter UI
    - "4040-4050:4040-4050"  # Spark application UIs
  environment:
    - JUPYTER_ENABLE_LAB=yes
    - SPARK_MASTER_URL=spark://spark-master:7077
```

**Features:**
- PySpark kernel pre-configured
- Connected to Spark cluster
- MLflow Python client installed
- Access notebooks in `/home/jovyan/work`

**Access:**
- Web UI: http://localhost:8888
- Token: Check with `docker logs jupyter-spark`

---

## Docker Compose Configuration

### File Structure

```yaml
# docker/docker-compose.yml
services:
  spark-master:
    # Master configuration

  spark-worker-1:
    # Worker 1 configuration

  # ... workers 2-4

  spark-submit:
    # Submit client configuration

  mlflow:
    # MLflow server configuration

  jupyter:
    # Jupyter configuration

networks:
  spark-network:
    driver: bridge
```

### Key Configuration Sections

#### Service Dependencies

```yaml
spark-worker-1:
  depends_on:
    spark-master:
      condition: service_healthy  # Wait for health check
```

#### Health Checks

```yaml
spark-master:
  healthcheck:
    test: ["CMD-SHELL", "timeout 5 bash -c '</dev/tcp/localhost/8080'"]
    interval: 30s
    timeout: 10s
    retries: 3
    start_period: 60s
```

#### Environment Variables

```yaml
environment:
  - SPARK_MODE=worker
  - SPARK_MASTER_URL=spark://spark-master:7077
  - SPARK_WORKER_MEMORY=6G
  - SPARK_WORKER_CORES=2
```

---

## Management Scripts

All management scripts are in `docker/` directory.

### setup.sh

**Purpose**: Initial cluster setup

```bash
./setup.sh
```

**What it does:**
1. Validates `docker-compose.yml`
2. Optionally cleans Docker system
3. Creates required directories
4. Builds Jupyter image
5. Starts all services
6. Waits for health checks
7. Displays service URLs

**Options:**
- Clean build (removes old images/volumes)
- Skip cleanup (faster restart)

---

### start.sh / stop.sh / restart.sh

**Purpose**: Manage cluster lifecycle

```bash
# Start all services
./local-start.sh

# Stop all services (preserves data)
./local-stop.sh

# Restart all services
./local-restart.sh
```

---

### submit.sh

**Purpose**: Submit Spark jobs to cluster

```bash
# Run with default config
./local-submit.sh

# Run specific tasks
TASKS="load,preprocess" ./local-submit.sh
```

**Internal command:**
```bash
docker exec spark-submit spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --class com.flightdelay.app.FlightDelayPredictionApp \
  --driver-memory 14G \
  --driver-cores 10 \
  --jars /apps/mlflow-client-3.4.0.jar,/apps/mlflow-spark_2.13-3.4.0.jar \
  /apps/Emiasd-Flight-Data-Analysis.jar \
  local
```

---

### logs.sh

**Purpose**: View service logs

```bash
# View logs for specific service
./logs.sh spark-master
./logs.sh mlflow-server

# Follow logs in real-time
./logs.sh -f spark-submit
```

---

### shell.sh

**Purpose**: Interactive Spark shell

```bash
./shell.sh
```

Opens Spark shell connected to cluster:
```scala
scala> spark.version
res0: String = 3.5.3

scala> sc.master
res1: String = spark://spark-master:7077
```

---

### cleanup.sh

**Purpose**: Clean up Docker resources

```bash
./cleanup.sh
```

**What it does:**
- Stops all containers
- Removes containers
- Removes unused volumes
- Cleans build cache
- Optionally removes images

**⚠️ Warning**: This deletes temporary data but preserves mounted volumes.

---

## Networking

### Network Configuration

```yaml
networks:
  spark-network:
    driver: bridge
    name: spark-network
```

**All services are on the same network**, enabling:
- Service discovery by hostname
- Inter-container communication
- Isolated from host network

### Service Communication

| From | To | URL |
|------|-----|-----|
| Workers | Master | `spark://spark-master:7077` |
| Submit | Master | `spark://spark-master:7077` |
| Spark Jobs | MLflow | `http://mlflow-server:5000` |
| Jupyter | Master | `spark://spark-master:7077` |

### Port Mapping

| Service | Internal Port | External Port | Purpose |
|---------|--------------|---------------|---------|
| spark-master | 8080 | 8080 | Web UI |
| spark-master | 7077 | 7077 | Master port |
| spark-worker-1 | 8081 | 8081 | Worker UI |
| spark-worker-2 | 8082 | 8082 | Worker UI |
| spark-worker-3 | 8083 | 8083 | Worker UI |
| spark-worker-4 | 8084 | 8084 | Worker UI |
| mlflow | 5000 | 5555 | Web UI |
| jupyter | 8888 | 8888 | JupyterLab |

---

## Volumes and Data Persistence

### Mounted Volumes

```yaml
volumes:
  - ../work/data:/data           # Input datasets (read-only)
  - ../work/apps:/apps           # JARs and libraries
  - ../work/output:/output       # Results and models
  - ../work/mlflow:/mlflow       # MLflow database
  - ../work/scripts:/scripts     # Python scripts
```

### Data Flow

```
Host Machine                    Docker Container
─────────────                  ─────────────────
work/
├── data/           ───────────► /data              (input)
├── apps/           ───────────► /apps              (code)
├── output/         ◄────────── /output            (results)
├── mlflow/         ◄─────────► /mlflow            (tracking)
└── scripts/        ───────────► /scripts          (utilities)
```

### Persistence

**Persistent Data** (survives container restarts):
- `/data` - Input datasets
- `/output` - Trained models and metrics
- `/mlflow` - Experiment tracking database

**Ephemeral Data** (lost on cleanup):
- Container filesystems
- Temporary Spark files
- Worker local directories

---

## Resource Management

### Worker Resource Allocation

**Per Worker:**
- Memory: 6GB
- Cores: 2
- Local temp: 2GB

**Total Cluster:**
- Memory: 24GB (4 workers × 6GB)
- Cores: 8 (4 workers × 2 cores)

### Adjusting Resources

Edit `docker-compose.yml`:

```yaml
spark-worker-1:
  environment:
    - SPARK_WORKER_MEMORY=8G    # Increase to 8GB
    - SPARK_WORKER_CORES=4      # Increase to 4 cores
```

**Note**: Ensure Docker Desktop has sufficient resources allocated.

### Scaling Workers

**Add more workers:**

```yaml
spark-worker-5:
  image: bitnami/spark:3.5.3
  container_name: spark-worker-5
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_MEMORY=6G
    - SPARK_WORKER_CORES=2
    - SPARK_WORKER_WEBUI_PORT=8085
  ports:
    - "8085:8085"
  depends_on:
    spark-master:
      condition: service_healthy
  volumes:
    - ../work/data:/data
    - ../work/apps:/apps
    - ../work/output:/output
  networks:
    - spark-network
```

Then restart:
```bash
docker-compose up -d
```

---

## Troubleshooting

### Services Won't Start

**Check Docker is running:**
```bash
docker info
```

**Check logs:**
```bash
docker-compose logs spark-master
docker-compose logs mlflow
```

**Common issues:**
- Insufficient memory (increase Docker Desktop allocation)
- Port conflicts (check `lsof -i :8080`)
- Corrupted images (run `docker-compose down` then `docker-compose up`)

### Workers Not Connecting

**Check master URL:**
```bash
docker logs spark-master | grep "Starting Spark master"
# Should show: spark://spark-master:7077
```

**Check worker logs:**
```bash
docker logs spark-worker-1
# Should show: Successfully registered with master
```

**Fix:**
```bash
docker-compose restart spark-worker-1
```

### MLflow Connection Refused

**Verify MLflow is running:**
```bash
curl http://localhost:5555/health
# Should return: OK
```

**Test from Spark container:**
```bash
docker exec spark-submit curl http://mlflow-server:5000/health
# Should return: OK
```

**If fails:**
```bash
docker-compose restart mlflow
```

### Out of Memory

**Symptoms:**
- Jobs fail with `OutOfMemoryError`
- Docker Desktop crashes

**Solutions:**
1. Increase Docker memory (Settings → Resources)
2. Reduce worker memory in `docker-compose.yml`
3. Reduce driver memory in `submit.sh`

### Slow Performance

**Check resource usage:**
```bash
docker stats
```

**Optimize:**
- Reduce number of workers
- Increase parallelism in Spark config
- Use PCA to reduce feature dimensions
- Sample data for testing

---

## Best Practices

### 1. Resource Allocation

- Set Docker Desktop to at least 16GB RAM
- Leave 4-8GB for host OS
- Monitor with `docker stats`

### 2. Data Management

- Keep input data in `work/data/`
- Results automatically save to `work/output/`
- Backup MLflow database regularly

### 3. Development Workflow

```bash
# 1. Make code changes
vim src/main/scala/...

# 2. Rebuild JAR
sbt package

# 3. Submit job
cd docker
./local-submit.sh

# 4. Check results
# - MLflow UI: http://localhost:5555
# - Output: work/output/
```

### 4. Debugging

```bash
# Check all services
docker-compose ps

# View logs
./logs.sh -f spark-submit

# Access container
docker exec -it spark-submit /bin/bash

# Check Spark UI
# http://localhost:8080
```

---

## Advanced Configuration

### Custom Spark Configuration

Create `docker/spark-defaults.conf`:

```properties
spark.sql.shuffle.partitions 100
spark.default.parallelism 100
spark.memory.fraction 0.8
spark.driver.maxResultSize 4g
```

Mount in `docker-compose.yml`:

```yaml
volumes:
  - ./spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf
```

### Using External Database for MLflow

Replace SQLite with PostgreSQL:

```yaml
mlflow:
  environment:
    - BACKEND_STORE_URI=postgresql://user:pass@postgres:5432/mlflow

postgres:
  image: postgres:14
  environment:
    - POSTGRES_USER=mlflow
    - POSTGRES_PASSWORD=mlflow
    - POSTGRES_DB=mlflow
```

---

## Summary

**Docker Infrastructure Benefits:**
- ✅ Complete environment in minutes
- ✅ Consistent across machines
- ✅ Easy scaling (add workers)
- ✅ Integrated MLflow tracking
- ✅ Simple management scripts

**Key Files:**
- `docker/docker-compose.yml` - Service definitions
- `docker/setup.sh` - Initial setup
- `docker/submit.sh` - Job submission
- `docker/Dockerfile.jupyter` - Jupyter custom image

**Next Steps:**
- [Configuration Guide](05-configuration.md) - Configure experiments
- [ML Pipeline](08-ml-pipeline.md) - Understand the training pipeline
- [MLflow Integration](09-mlflow-integration.md) - Track experiments

---

**Your Docker infrastructure is ready! 🐳**
