#!/bin/bash

# Tasks to execute (comma-separated): load,preprocess,feature-extraction,train
# Default: all tasks

#TASKS="${1:-data-pipeline}"
#TASKS="${1:-data-pipeline,feature-extraction}"
#TASKS="${1:-data-pipeline,feature-extraction,train}"

TASKS="${1:-data-pipeline}"

spark-submit \
  --master "$SPARK_MASTER_URL" \
  --deploy-mode client \
  --class com.flightdelay.app.FlightDelayPredictionApp \
  --driver-memory 40G \
  --driver-cores 10 \
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
  --conf spark.memory.offHeap.size=10g \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/spark-events \
  --conf spark.eventLog.compress=true \
  --jars /apps/mlflow-client-3.4.0.jar,/apps/mlflow-spark_2.13-3.4.0.jar \
  /apps/Emiasd-Flight-Data-Analysis.jar \
  local "$TASKS"
