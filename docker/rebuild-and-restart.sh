#!/bin/bash
# rebuild-and-local-restart.sh
# Script to rebuild Spark images with MLFlow and restart containers

set -e  # Exit on error

echo "=========================================="
echo "Rebuilding Spark Cluster with MLFlow"
echo "=========================================="

cd "$(dirname "$0")"

echo ""
echo "[Step 1/4] Stopping existing containers..."
docker-compose down

echo ""
echo "[Step 2/4] Building custom Spark+MLFlow image..."
docker-compose build spark-submit spark-worker-1 spark-worker-2 spark-worker-3 spark-worker-4

echo ""
echo "[Step 3/4] Starting all services..."
docker-compose up -d

echo ""
echo "[Step 4/4] Waiting for services to be healthy..."
sleep 10

echo ""
echo "=========================================="
echo "✅ Spark Cluster Rebuilt Successfully!"
echo "=========================================="
echo ""
echo "Services:"
echo "  - Spark Master UI:  http://localhost:8080"
echo "  - MLFlow UI:        http://localhost:5555"
echo "  - Jupyter:          http://localhost:8888"
echo ""
echo "To verify MLFlow installation:"
echo "  docker exec spark-submit python3 -c 'import mlflow; print(f\"MLFlow {mlflow.__version__}\")'"
echo ""
echo "To run your pipeline:"
echo "  ./submit.sh"
echo ""
