#!/bin/bash

# =====================================================================
# Lancement de l'app Flight Delay Prediction en mode Spark standalone
# Modes de cluster :
#   1w : 1 worker  (1 exécuteur)
#   2w : 2 workers (2 exécuteurs)
#   4w : 4 workers (4 exécuteurs, plus petits)
# =====================================================================

WORKERS="${1:-2w}"
TASKS="${2:-data-pipeline,feature-extraction,train}"
EXPERIENCE="${3:-local-d2_60_0_0}"

if [[ "$WORKERS" != "1w" && "$WORKERS" != "2w" && "$WORKERS" != "4w" ]]; then
    echo "❌ Argument invalide. Utilisation: $0 [1w|2w|4w] [tasks] [experience]"
    echo "   1w: 1 worker  × 8G  × 4 cores"
    echo "   2w: 2 workers × 8G  × 4 cores"
    echo "   4w: 4 workers × 4G  × 3 cores (défaut, plus granulaire)"
    echo ""
    echo "Exemples:"
    echo "  $0 2w"
    echo "  $0 4w data-pipeline,train"
    echo "  $0 2w data-pipeline,feature-extraction,train local-optimized-d2_60_0_0"
    exit 1
fi

echo "🔧 Configuration cluster: $WORKERS"
echo "📋 Tasks: $TASKS"
echo "🧪 Experience: $EXPERIENCE"

# ============================================================================
# CONFIGURE SPARK PARAMETERS BASED ON WORKERS
# => Objectif : rester raisonnable avec un Mac 48 Go
#    Docker + OS + overhead JVM -> viser ~24–26 Go pour Spark
# ============================================================================

if [ "$WORKERS" = "1w" ]; then
    # 1 worker → 1 exécuteur "moyen"
    EXECUTOR_MEMORY="8G"
    EXECUTOR_CORES="4"
    NUM_EXECUTORS="1"
    SHUFFLE_PARTITIONS="160"
    echo "✅ Spark config: 1 executor × 8G × 4 cores"
    echo "   Mode debug / développement (simple et stable)"
elif [ "$WORKERS" = "2w" ]; then
    # 2 workers → 2 exécuteurs "moyens"
    EXECUTOR_MEMORY="10G"
    EXECUTOR_CORES="4"
    NUM_EXECUTORS="2"
    SHUFFLE_PARTITIONS="200"
    echo "✅ Spark config: 2 executors × 8G × 4 cores"
    echo "   Bon compromis parallélisme / mémoire (~24G total avec le driver)"
else
    # 4 workers → 4 petits exécuteurs (plus de parallélisme, moins de mémoire chacun)
    EXECUTOR_MEMORY="4G"
    EXECUTOR_CORES="3"
    NUM_EXECUTORS="4"
    SHUFFLE_PARTITIONS="240"
    echo "✅ Spark config: 4 executors × 4G × 3 cores"
    echo "   Mode plus granulaire, adapté aux jobs plus légers ou très parallélisables"
fi

# ============================================================================
# CONFIG MAC M4 PRO (14 cores, 48GB RAM)
# deploy-mode client : le driver tourne sur le Mac (spark-submit)
# ============================================================================

spark-submit \
  --master "$SPARK_MASTER_URL" \
  --deploy-mode client \
  --class com.flightdelay.app.FlightDelayPredictionApp \
  \
  `# ========================================================================` \
  `# MEMORY CONFIGURATION` \
  `# ========================================================================` \
  --driver-memory 8G \
  --driver-cores 2 \
  --executor-memory "$EXECUTOR_MEMORY" \
  --executor-cores "$EXECUTOR_CORES" \
  --num-executors "$NUM_EXECUTORS" \
  \
  `# ========================================================================` \
  `# SPARK MEMORY TUNING` \
  `# (on reste relativement proche des valeurs par défaut)` \
  `# ========================================================================` \
  --conf spark.driver.maxResultSize=4g \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.5 \
  \
  `# ========================================================================` \
  `# PARALLELISM CONFIGURATION` \
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
  `# BROADCAST & COMPRESSION` \
  `# ========================================================================` \
  --conf spark.broadcast.compress=true \
  --conf spark.io.compression.codec=lz4 \
  --conf spark.sql.autoBroadcastJoinThreshold=10m \
  \
  `# ========================================================================` \
  `# GARBAGE COLLECTION (RF / GBT)` \
  `# ========================================================================` \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxGCPauseMillis=200" \
  --conf spark.driver.extraJavaOptions="-XX:+UseG1GC" \
  \
  `# ========================================================================` \
  `# MISC` \
  `# ========================================================================` \
  --conf spark.sql.debug.maxToStringFields=1000 \
  \
  `# ========================================================================` \
  `# EVENT LOGGING (pour Spark History Server)` \
  `# ========================================================================` \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/spark-events \
  --conf spark.eventLog.compress=true \
  \
  --jars /apps/mlflow-client-3.4.0.jar,/apps/mlflow-spark_2.13-3.4.0.jar \
  /apps/Emiasd-Flight-Data-Analysis.jar \
  "$EXPERIENCE" "$TASKS"