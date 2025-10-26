#!/bin/bash
echo "--> Build Spark job..."
sbt clean package
echo "--> Spark job built."

echo "--> Submitting Spark job..."
docker exec -it spark-submit chmod +x /scripts/spark-submit.sh
docker exec -it spark-submit /scripts/spark-submit.sh
echo "--> Spark job submitted."