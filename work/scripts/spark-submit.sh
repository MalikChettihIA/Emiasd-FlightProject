#!/bin/bash

spark-submit \
  --master "$SPARK_MASTER_URL" \
  --deploy-mode client \
  --class com.flightdelay.app.FlightDelayPredictionApp \
  --conf spark.executor.instances=4 \
  --executor-cores=2 \
  --executor-memory=6G \
  --conf spark.executor.memoryOverhead=512m \
  /apps/Emiasd-Flight-Data-Analysis.jar \
  local