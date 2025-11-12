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

# Définir les tâches (peut être surchargé par argument)
TASKS="${1:-data-pipeline,feature-extraction,train}"
echo -e "${GREEN}Tasks to execute:${NC} $TASKS"
echo ""

# Nettoyage du répertoire HDFS output
echo -e "${BLUE}Cleaning HDFS output directory...${NC}"
HDFS_OUTPUT="/students/p6emiasd2025/hbalamou/output"

if /opt/shared/hadoop-current/bin/hdfs dfs -test -d "$HDFS_OUTPUT"; then
    echo "Removing existing output directory: $HDFS_OUTPUT"
    /opt/shared/hadoop-current/bin/hdfs dfs -rm -r -skipTrash "$HDFS_OUTPUT" 2>&1 | tee -a "$LOG_FILE"
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo -e "${GREEN}✓ Output directory cleaned${NC}"
    else
        echo -e "${RED}✗ Failed to clean output directory${NC}"
    fi
else
    echo "Output directory does not exist, skipping cleanup"
fi
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
/opt/shared/spark-current/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --class com.flightdelay.app.FlightDelayPredictionApp \
  --files $HOME/workspace/config/prodlamsade-config.yml \
  --driver-memory 32G \
  --driver-cores 8 \
  --executor-memory 8G \
  --num-executors 6 \
  --conf spark.driver.maxResultSize=8g \
  --conf spark.sql.shuffle.partitions=400 \
  --conf spark.default.parallelism=400 \
  --conf spark.kryoserializer.buffer.max=1024m \
  --conf spark.memory.fraction=0.8 \
  --conf spark.rpc.message.maxSize=2047 \
  --conf spark.sql.debug.maxToStringFields=1000 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.network.timeout=800s \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2g \
  --conf spark.memory.storageFraction=0.3 \
  --jars $HOME/workspace/apps/mlflow-client-3.4.0.jar,$HOME/workspace/apps/mlflow-spark_2.13-3.4.0.jar \
  $HOME/workspace/apps/Emiasd-Flight-Data-Analysis.jar \
  prodlamsade "$TASKS" 2>&1 | tee "$LOG_FILE"

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