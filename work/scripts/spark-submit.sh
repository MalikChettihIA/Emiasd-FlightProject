#!/bin/bash

# Configuration des workers (2w ou 4w)
WORKERS="${1:-4w}"
TASKS="${2:-data-pipeline,feature-extraction,train}"

if [[ "$WORKERS" != "2w" && "$WORKERS" != "4w" ]]; then
    echo "❌ Argument invalide. Utilisation: $0 [2w|4w] [tasks]"
    echo "   2w: 2 workers × 20G × 6 cores"
    echo "   4w: 4 workers × 10G × 3 cores (défaut)"
    echo ""
    echo "Exemples:"
    echo "  $0 2w"
    echo "  $0 4w data-pipeline,train"
    exit 1
fi

echo "🔧 Configuration cluster: $WORKERS"

# ============================================================================
# CONFIGURE SPARK PARAMETERS BASED ON WORKERS
# ============================================================================
if [ "$WORKERS" = "2w" ]; then
    # Configuration 2 workers × 20G × 6 cores
    EXECUTOR_MEMORY="18G"
    EXECUTOR_CORES="6"
    NUM_EXECUTORS="2"
    SHUFFLE_PARTITIONS="200"
    echo "✅ Spark config: 2 executors × 18G × 6 cores (GROS EXECUTORS pour RF)"
    echo "   Mémoire pour RandomForest: ~7.2G par executor"
else
    # Configuration 4 workers × 10G × 3 cores
    EXECUTOR_MEMORY="9G"
    EXECUTOR_CORES="3"
    NUM_EXECUTORS="4"
    SHUFFLE_PARTITIONS="200"
    echo "✅ Spark config: 4 executors × 9G × 3 cores (PETITS EXECUTORS)"
    echo "   Mémoire pour RandomForest: ~3.6G par executor"
fi

# ============================================================================
# OPTIMIZED CONFIGURATION FOR MAC M4 PRO (14 cores, 48GB RAM)
# Auto-adapted based on cluster topology
# ============================================================================

spark-submit \
  --master "$SPARK_MASTER_URL" \
  --deploy-mode client \
  --class com.flightdelay.app.FlightDelayPredictionApp \
  \
  `# ========================================================================` \
  `# MEMORY CONFIGURATION - Auto-detected from cluster` \
  `# ========================================================================` \
  --driver-memory 28G \
  --driver-cores 2 \
  --executor-memory "$EXECUTOR_MEMORY" \
  --executor-cores "$EXECUTOR_CORES" \
  --num-executors "$NUM_EXECUTORS" \
  \
  `# ========================================================================` \
  `# SPARK MEMORY TUNING` \
  `# ========================================================================` \
  --conf spark.driver.maxResultSize=4g \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.5 \
  \
  `# ========================================================================` \
  `# PARALLELISM CONFIGURATION - Auto-adapted` \
  `# ========================================================================` \
  --conf spark.sql.shuffle.partitions="$SHUFFLE_PARTITIONS" \
  --conf spark.default.parallelism="$SHUFFLE_PARTITIONS" \
  \
  `# ========================================================================` \
  `# SERIALIZATION & NETWORK` \
  `# ========================================================================` \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer.max=512m \
  --conf spark.rpc.message.maxSize=1024 \
  --conf spark.network.timeout=600s \
  --conf spark.executor.heartbeatInterval=30s \
  \
  `# ========================================================================` \
  `# BROADCAST & MODEL HANDLING` \
  `# ========================================================================` \
  --conf spark.broadcast.compress=true \
  --conf spark.io.compression.codec=lz4 \
  --conf spark.sql.autoBroadcastJoinThreshold=10m \
  \
  `# ========================================================================` \
  `# GARBAGE COLLECTION (Important for RandomForest)` \
  `# ========================================================================` \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxGCPauseMillis=200" \
  --conf spark.driver.extraJavaOptions="-XX:+UseG1GC" \
  \
  `# ========================================================================` \
  `# MISC` \
  `# ========================================================================` \
  --conf spark.sql.debug.maxToStringFields=1000 \
  \
  --jars /apps/mlflow-client-3.4.0.jar,/apps/mlflow-spark_2.13-3.4.0.jar \
  /apps/Emiasd-Flight-Data-Analysis.jar \
  local-d2_60_3_3_hyperparmaters "$TASKS"