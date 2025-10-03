#!/bin/bash

# Tasks to execute (comma-separated): load,preprocess,feature-extraction,train,evaluate
# Default: all tasks

#TASKS="${1:-load}"
#TASKS="${1:-load,preprocess}"
#TASKS="${1:-load,preprocess,feature-extraction}"
#TASKS="${1:-load,preprocess,feature-extraction,train}"
TASKS="${1:-load,preprocess,feature-extraction,train,evaluate}"

spark-submit \
  --master "$SPARK_MASTER_URL" \
  --deploy-mode client \
  --class com.flightdelay.app.FlightDelayPredictionApp \
  --driver-memory 14G \
  --driver-cores 10 \
  --conf spark.driver.maxResultSize=4g \
  --conf spark.sql.shuffle.partitions=100 \
  --conf spark.default.parallelism=100 \
  --conf spark.kryoserializer.buffer.max=512m \
  --conf spark.memory.fraction=0.8 \
  --conf spark.rpc.message.maxSize=128 \
  --conf spark.sql.debug.maxToStringFields=1000 \
  /apps/Emiasd-Flight-Data-Analysis.jar \
  local "$TASKS"