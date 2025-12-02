#!/bin/bash

set -euo pipefail

# Configuration
PROJECT_ID="tough-artwork-475804-h0"
WORKFLOW_NAME="flight-delay-workflow"
REGION="europe-west1"
ZONE="europe-west1-b"
BUCKET="gs://tough-artwork-475804-h0-flight-data"

APP_JAR="Emiasd-Flight-Data-Analysis.jar"
MLFLOW_CLIENT_JAR="mlflow-client-3.4.0.jar"
MLFLOW_SPARK_JAR="mlflow-spark_2.13-3.4.0.jar"
CONFIG_FILE="proddataproc-config.yml"
CONFIG_NAME="proddataproc"
TASKS="${1:-data-pipeline,feature-extraction,train}"

echo "=== Dataproc Deployment (Quota 24 vCPUs) ==="
echo "Configuration: 1 master n1-standard-8 + 4 workers n1-standard-4"
echo "Total: 8 + 16 = 24 vCPUs (within default quota)"
echo ""

# 1. Build JAR
echo "1. Building JAR..."
sbt clean package
if [ ! -f "./work/apps/$APP_JAR" ]; then
    echo "[ERROR] JAR not found: ./work/apps/$APP_JAR" >&2
    exit 1
fi
echo "✓ Built"

# 2. Upload JARs
echo "2. Uploading JARs..."
gsutil -m cp ./work/apps/$APP_JAR ./work/apps/$MLFLOW_CLIENT_JAR ./work/apps/$MLFLOW_SPARK_JAR \
    $BUCKET/jars/
echo "✓ Uploaded"

# 3. Upload config
echo "3. Uploading config..."
gsutil cp src/main/resources/$CONFIG_FILE $BUCKET/config/$CONFIG_FILE
echo "✓ Config uploaded"

# 4. Clear outputs
echo "4. Clearing previous outputs..."
gsutil -m rm -r $BUCKET/output/ 2>/dev/null || true
echo "✓ Cleared"

# 5. Delete existing template
echo "5. Preparing workflow template..."
gcloud dataproc workflow-templates delete "$WORKFLOW_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --quiet 2>/dev/null || true

# 6. Create template
gcloud dataproc workflow-templates create "$WORKFLOW_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID"
echo "✓ Template created"

# 7. Configure managed cluster (optimized for 24 vCPUs quota)
echo "7. Configuring cluster (24 vCPUs total)..."
gcloud dataproc workflow-templates set-managed-cluster $WORKFLOW_NAME \
    --region=$REGION \
    --project=$PROJECT_ID \
    --cluster-name=flight-temp-cluster \
    --zone=$ZONE \
    --master-machine-type=n1-standard-8 \
    --master-boot-disk-size=100GB \
    --num-workers=4 \
    --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=100GB \
    --image-version=2.2-debian12
echo "✓ Cluster configured"

# 8. Add Spark job with optimized parameters
echo "8. Adding Spark job..."
gcloud dataproc workflow-templates add-job spark \
    --workflow-template=$WORKFLOW_NAME \
    --region=$REGION \
    --project=$PROJECT_ID \
    --step-id=flight-delay-job \
    --class=com.flightdelay.app.FlightDelayPredictionApp \
    --jars=$BUCKET/jars/$APP_JAR,$BUCKET/jars/$MLFLOW_CLIENT_JAR,$BUCKET/jars/$MLFLOW_SPARK_JAR \
    --files=$BUCKET/config/$CONFIG_FILE \
    --properties="\
spark.driver.memory=20g,\
spark.driver.cores=6,\
spark.driver.maxResultSize=6g,\
spark.driver.memoryOverhead=2g,\
spark.executor.memory=8g,\
spark.executor.cores=3,\
spark.executor.memoryOverhead=2g,\
spark.sql.shuffle.partitions=300,\
spark.default.parallelism=300,\
spark.memory.fraction=0.75,\
spark.memory.storageFraction=0.5,\
spark.serializer=org.apache.spark.serializer.KryoSerializer,\
spark.kryoserializer.buffer.max=1024m,\
spark.rpc.message.maxSize=2047,\
spark.sql.debug.maxToStringFields=1000,\
spark.network.timeout=800s,\
spark.executor.heartbeatInterval=60s,\
spark.dynamicAllocation.enabled=false,\
spark.sql.adaptive.enabled=true,\
spark.sql.adaptive.coalescePartitions.enabled=true,\
spark.sql.adaptive.skewJoin.enabled=true,\
spark.executorEnv.MLFLOW_TRACKING_URI=$BUCKET/mlflow,\
spark.yarn.appMasterEnv.MLFLOW_TRACKING_URI=$BUCKET/mlflow" \
    -- $CONFIG_NAME "$TASKS"
echo "✓ Spark job configured"

# 9. Execute workflow
echo ""
echo "9. Executing workflow..."
WORKFLOW_OUTPUT=$(gcloud dataproc workflow-templates instantiate $WORKFLOW_NAME \
    --region=$REGION \
    --project=$PROJECT_ID 2>&1)

echo "$WORKFLOW_OUTPUT"

OPERATION_ID=$(echo "$WORKFLOW_OUTPUT" | grep -oP 'operations/\K[^\s]+' || \
               echo "$WORKFLOW_OUTPUT" | grep -oP 'Waiting on operation \[\K[^\]]+')

if [ -z "$OPERATION_ID" ]; then
    echo "✗ Failed to extract operation ID"
    exit 1
fi

echo ""
echo "=========================================="
echo "✓ Workflow started successfully!"
echo "=========================================="
echo "Operation ID: $OPERATION_ID"
echo ""
echo "Cluster details:"
echo "  - Master: n1-standard-8 (8 vCPUs, 30 GB RAM)"
echo "  - Workers: 4× n1-standard-4 (16 vCPUs, 60 GB RAM total)"
echo "  - Executors: ~8 (3 cores, 8GB each)"
echo "  - Total cores: 24 executor cores"
echo ""
echo "Monitor workflow:"
echo "  → https://console.cloud.google.com/dataproc/workflows/instances/$OPERATION_ID?region=$REGION&project=$PROJECT_ID"
echo ""
echo "Check outputs:"
echo "  → $BUCKET/output/"
echo ""
echo "Cluster lifecycle:"
echo "  ✓ Created automatically (~2-3 min)"
echo "  ✓ Executes job"
echo "  ✓ Deleted automatically after completion"
echo "=========================================="