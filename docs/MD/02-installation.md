# 📦 Installation Guide

Complete installation instructions for the Flight Delay Prediction system.

---

## Table of Contents

- [System Requirements](#system-requirements)
- [Docker Installation (Recommended)](#docker-installation-recommended)
- [Manual Installation](#manual-installation)
- [Data Setup](#data-setup)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)

---

## System Requirements

### Minimum Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| **RAM** | 16 GB | 24 GB |
| **CPU** | 4 cores | 8+ cores |
| **Disk** | 20 GB free | 50 GB free |
| **OS** | macOS, Linux, Windows 10+ | macOS, Linux |

### Software Dependencies

**For Docker Installation (Recommended):**
- Docker Desktop 20.10+
- Docker Compose 2.0+

**For Manual Installation:**
- Java JDK 17
- Scala 2.12.18
- Apache Spark 3.5.3
- Python 3.9+
- sbt 1.10+

---

## Docker Installation (Recommended)

The easiest way to get started. Docker handles all dependencies automatically.

### 1. Install Docker Desktop

#### macOS
```bash
# Using Homebrew
brew install --cask docker

# Or download from:
# https://www.docker.com/products/docker-desktop
```

#### Linux (Ubuntu/Debian)
```bash
# Update package index
sudo apt-get update

# Install dependencies
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

# Add Docker's official GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Set up repository
echo \
  "deb [arch=$(dpkg --print-architecture) \
  signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add user to docker group
sudo usermod -aG docker $USER
newgrp docker
```

#### Windows
Download and install Docker Desktop from:
https://www.docker.com/products/docker-desktop

### 2. Verify Docker Installation

```bash
docker --version
# Expected: Docker version 20.10.x or higher

docker-compose --version
# Expected: Docker Compose version 2.x or higher

# Test Docker
docker run hello-world
# Should download and run successfully
```

### 3. Configure Docker Resources

**Increase Docker memory allocation:**

1. Open Docker Desktop
2. Go to Settings → Resources
3. Set Memory to **16GB minimum** (24GB recommended)
4. Set CPUs to **4 minimum** (8 recommended)
5. Click **Apply & Restart**

### 4. Clone Repository

```bash
git clone <repository-url>
cd Emiasd-FlightProject
```

### 5. Initial Setup

```bash
cd docker
./setup.sh
```

This will:
- Create required directories
- Pull Docker images
- Start all services
- Run health checks

**Expected output:**
```
🚀 Configuration du cluster Spark avec 4 workers
=================================================
✅ Cluster Spark démarré avec succès !

📊 Services disponibles:
  - Spark Master UI: http://localhost:8080
  - MLflow UI:       http://localhost:5555
  - Jupyter Lab:     http://localhost:8888
```

---

## Manual Installation

For advanced users who want to run without Docker.

### 1. Install Java

```bash
# macOS (using Homebrew)
brew install openjdk@17

# Linux (Ubuntu/Debian)
sudo apt-get install -y openjdk-17-jdk

# Verify
java -version
# Should show: openjdk version "17.x.x"
```

### 2. Install Scala

```bash
# macOS
brew install scala@2.12

# Linux
curl -fL https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-linux.gz | \
  gzip -d > cs && chmod +x cs && ./cs setup

# Verify
scala -version
# Should show: Scala code runner version 2.12.18
```

### 3. Install sbt (Scala Build Tool)

```bash
# macOS
brew install sbt

# Linux
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | \
  sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | \
  sudo apt-key add
sudo apt-get update
sudo apt-get install sbt

# Verify
sbt --version
# Should show: sbt version 1.10.x
```

### 4. Install Apache Spark

```bash
# Download Spark 3.5.3
cd /opt
sudo wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
sudo tar -xzf spark-3.5.3-bin-hadoop3.tgz
sudo mv spark-3.5.3-bin-hadoop3 spark

# Set environment variables
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc
source ~/.bashrc

# Verify
spark-submit --version
# Should show: version 3.5.3
```

### 5. Install Python Dependencies

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Or manually install key packages
pip install \
  mlflow==3.4.0 \
  pandas \
  numpy \
  matplotlib \
  seaborn \
  scikit-learn \
  plotly
```

### 6. Install MLflow

```bash
pip install mlflow==3.4.0

# Verify
mlflow --version
# Should show: mlflow, version 3.4.0
```

### 7. Build Project

```bash
# Clone repository
git clone <repository-url>
cd Emiasd-FlightProject

# Build with sbt
sbt clean compile package

# Verify JAR created
ls -la work/apps/Emiasd-Flight-Data-Analysis.jar
```

### 8. Start Local Spark Cluster

```bash
# Start master
start-master.sh

# Start worker (in another terminal)
start-worker.sh spark://localhost:7077

# Verify at http://localhost:8080
```

### 9. Start MLflow Server

```bash
# Start MLflow tracking server
mlflow server \
  --backend-store-uri sqlite:///work/mlflow/mlflow.db \
  --default-artifact-root work/mlflow/artifacts \
  --host 0.0.0.0 \
  --port 5555

# Verify at http://localhost:5555
```

---

## Data Setup

### Download Datasets

1. **Download from source:**
   - [Flight Delay Dataset](https://www.dropbox.com/sh/iasq7frk6cusSNfqYNYsnLGIXa)

2. **Organize data structure:**

```bash
# Expected structure
work/data/
├── FLIGHT-3Y/
│   ├── Flights/
│   │   └── 201201.csv
│   ├── Weather/
│   │   └── 201201hourly.txt
│   └── wban_airport_timezone.csv
```

### Verify Data Files

```bash
# Check files exist
ls -lh work/data/FLIGHT-3Y/Flights/201201.csv
ls -lh work/data/FLIGHT-3Y/Weather/201201hourly.txt
ls -lh work/data/FLIGHT-3Y/wban_airport_timezone.csv
```

### Data Format Validation

```bash
# Check flight data (should have ~142K rows)
wc -l work/data/FLIGHT-3Y/Flights/201201.csv

# Check headers
head -1 work/data/FLIGHT-3Y/Flights/201201.csv
# Should show: Year,Month,DayofMonth,DayOfWeek,...

head -1 work/data/FLIGHT-3Y/Weather/201201hourly.txt
# Should show: WBAN,Date,Time,StationType,...
```

---

## Verification

### Test Docker Installation

```bash
cd docker

# Check all containers are running
docker ps
# Should show 7 running containers

# Test Spark connection
./shell.sh
# Should open Spark shell
# Type :quit to exit

# Test job submission
./local-submit.sh
# Should start the ML pipeline
```

### Test Manual Installation

```bash
# Test Spark
spark-shell
# Should open Spark shell
# Type :quit to exit

# Test sbt
sbt test
# Should run tests

# Test MLflow
mlflow ui
# Should start UI at http://localhost:5000
```

### Verify Services

Open in browser:
- **Spark Master**: http://localhost:8080
  - Should show cluster status
  - Check workers are connected

- **MLflow**: http://localhost:5555
  - Should show MLflow UI
  - Check experiments appear after running

- **Jupyter** (Docker only): http://localhost:8888
  - Should show JupyterLab
  - Check PySpark kernel available

---

## Troubleshooting

### Docker Issues

#### Problem: Docker daemon not running
```bash
# macOS
open -a Docker

# Linux
sudo systemctl start docker
sudo systemctl enable docker
```

#### Problem: Permission denied
```bash
# Linux only
sudo usermod -aG docker $USER
newgrp docker
```

#### Problem: Out of disk space
```bash
# Clean up Docker
docker system prune -a --volumes

# Check disk usage
docker system df
```

### Build Issues

#### Problem: sbt compilation fails
```bash
# Clean and rebuild
sbt clean
sbt compile
sbt package
```

#### Problem: Missing dependencies
```bash
# Update dependencies
sbt update

# Check for conflicts
sbt evicted
```

### Spark Issues

#### Problem: Java heap space error
```bash
# Increase driver memory in docker-compose.yml
environment:
  - SPARK_DRIVER_MEMORY=14G  # Increase this

# Or in spark-local-submit.sh
--driver-memory 14G
```

#### Problem: Worker not connecting
```bash
# Check master URL
docker logs spark-master | grep "Starting Spark master"

# Check worker logs
docker logs spark-worker-1
```

### MLflow Issues

#### Problem: MLflow not tracking experiments
```bash
# Check MLflow is running
curl http://localhost:5555/health

# Check configuration
cat src/main/resources/local-config.yml | grep mlflow

# Verify container can reach MLflow
docker exec spark-submit curl http://mlflow-server:5000/health
```

#### Problem: Database locked
```bash
# Stop MLflow
docker-compose stop mlflow

# Remove lock file
rm work/mlflow/mlflow.db-shm
rm work/mlflow/mlflow.db-wal

# Restart
docker-compose start mlflow
```

---

## Post-Installation Steps

### 1. Configure Experiments

Edit `src/main/resources/local-config.yml`:
- Set data paths
- Configure experiments
- Adjust hyperparameters

See [Configuration Guide](05-configuration.md) for details.

### 2. Run First Experiment

```bash
cd docker
./local-submit.sh
```

### 3. Verify Results

- Check MLflow UI: http://localhost:5555
- Check output directory: `work/output/`
- Review logs: `docker logs -f spark-submit`

---

## Environment Variables

### Docker Environment

Set in `docker-compose.yml`:

```yaml
environment:
  - SPARK_MASTER_URL=spark://spark-master:7077
  - SPARK_DRIVER_MEMORY=14G
  - SPARK_WORKER_MEMORY=6G
  - SPARK_WORKER_CORES=2
```

### Local Environment

Set in `~/.bashrc` or `~/.zshrc`:

```bash
export SPARK_HOME=/opt/spark
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

---

## Uninstallation

### Docker Installation

```bash
# Stop and remove all containers
cd docker
./cleanup.sh

# Remove Docker images
docker rmi bitnami/spark:3.5.3
docker rmi ghcr.io/mlflow/mlflow:v3.4.0

# Remove data (optional)
rm -rf work/output
rm -rf work/mlflow
```

### Manual Installation

```bash
# Remove Spark
sudo rm -rf /opt/spark

# Remove project
rm -rf Emiasd-FlightProject

# Uninstall packages (macOS)
brew uninstall scala@2.12 sbt openjdk@17

# Uninstall packages (Linux)
sudo apt-get remove openjdk-17-jdk sbt
```

---

## Next Steps

- ✅ Installation complete
- ✅ Services verified
- ✅ Data prepared

**Continue to:**
- [Quick Start](01-quick-start.md) - Run your first experiment
- [Docker Infrastructure](03-docker-infrastructure.md) - Learn Docker setup
- [Configuration](05-configuration.md) - Configure experiments

---

**Installation complete! Ready to predict flight delays! ✈️**
