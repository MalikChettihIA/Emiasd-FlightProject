#!/bin/bash

# Configuration
LOG_DIR="$HOME/workspace/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/flight-app-${TIMESTAMP}.log"
LATEST_LOG="$LOG_DIR/latest.log"

# Créer le répertoire de logs s'il n'existe pas
mkdir -p "$LOG_DIR"

# Couleurs pour l'affichage
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Banner
echo -e "${BLUE}================================================================================================${NC}"
echo -e "${GREEN}Flight Delay Prediction - Execution Started${NC}"
echo -e "${BLUE}================================================================================================${NC}"
echo "Timestamp: $(date)"
echo "Log file: $LOG_FILE"
echo -e "${BLUE}================================================================================================${NC}\n"

# Clean directories
hdfs dfs -rm -r -skipTrash /students/p6emiasd2025/mchettih/output/Experience*
#hdfs dfs -rm -r -skipTrash /students/p6emiasd2025/mchettih/output/common
hdfs dfs -rm -r -skipTrash /students/p6emiasd2025/mchettih/output/spark-checkpoints/*
#hdfs dfs -rm -r -skipTrash /students/p6emiasd2025/mchettih/output/spark-events/*
echo "Timestamp: $(date)"
echo "HDFS Files cleaned"
echo -e "${BLUE}================================================================================================${NC}\n"

# Définir les tâches (peut être surchargé par argument)
TASKS="${1:-data-pipeline,feature-extraction,train}"
EXPERIENCE="${2:-prodlamsade-d2_60_0_0}"

echo -e "${GREEN}Tasks to execute:${NC} $TASKS"
echo -e "${GREEN}Experience to execute:${NC} $EXPERIENCE"
echo ""

# Fonction pour nettoyer à la sortie
cleanup() {
    echo -e "\n${BLUE}================================================================================================${NC}"
    echo -e "${GREEN}Execution finished at:${NC} $(date)"
    echo -e "${GREEN}Log saved to:${NC} $LOG_FILE"
    echo -e "${BLUE}================================================================================================${NC}"
}

trap cleanup EXIT

# Exécuter spark-submit avec logging
spark-submit \
  --master yarn \
  --deploy-mode client \
  --class com.flightdelay.app.FlightDelayPredictionApp \
  --driver-memory 16G \
  --driver-cores 4 \
  --executor-memory 8G \
  --num-executors 4 \
  --conf spark.driver.maxResultSize=2g \
  --conf spark.yarn.stagingDir=hdfs:///students/p6emiasd2025/${USER}/.sparkStaging \
  --jars ./apps/mlflow-client-3.4.0.jar,./apps/mlflow-spark_2.13-3.4.0.jar \
  ./apps/Emiasd-Flight-Data-Analysis.jar \
  $EXPERIENCE "$TASKS" 2>&1 | tee "$LOG_FILE"

# Créer un lien vers le dernier log
ln -sf "$LOG_FILE" "$LATEST_LOG"

# Résumé de l'exécution
EXIT_CODE=${PIPESTATUS[0]}
echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ Execution completed successfully${NC}"
else
    echo -e "${RED}✗ Execution failed with exit code: $EXIT_CODE${NC}"
fi
