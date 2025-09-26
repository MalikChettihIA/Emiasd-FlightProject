#!/bin/bash
spark-submit \
    --deploy-mode client \
    --master "$SPARK_MASTER_URL" \
    --executor-cores 4 \
    --executor-memory 2G \
    --num-executors 1 \
    --class "MainApp" \
    "apps/Emiasd-Flight-Data-Analysis.jar" \
    "data/Iris.csv" \
    "output"