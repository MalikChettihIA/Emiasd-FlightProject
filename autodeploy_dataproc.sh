#!/bin/bash

# ====================================================================
# Google Cloud Dataproc Deployment Script
# ====================================================================
# This script deploys the Flight Delay Prediction application to
# Google Cloud Dataproc using a managed cluster workflow.
# ====================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env.dataproc"

# Check if .env.dataproc exists
if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Configuration file not found: $ENV_FILE"
    echo ""
    echo "Please run the configuration script first:"
    echo "  ./configure_dataproc.sh"
    echo ""
    echo "Or copy the example file and edit it manually:"
    echo "  cp .env.dataproc.example .env.dataproc"
    echo "  nano .env.dataproc"
    exit 1
fi

# Load configuration
echo "Loading configuration from $ENV_FILE..."
source "$ENV_FILE"

# Validate required variables
required_vars=(
    "GCP_PROJECT_ID"
    "GCP_REGION"
    "GCP_ZONE"
    "GCS_BUCKET"
    "WORKFLOW_NAME"
    "CONFIG_FILE"
    "CONFIG_NAME"
)

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "❌ Error: Required variable $var is not set in $ENV_FILE"
        exit 1
    fi
done

# Allow tasks to be overridden via command line
TASKS="${1:-${DEFAULT_TASKS}}"

echo ""
echo "======================================================================"
echo "  Deploying to Google Cloud Dataproc"
echo "======================================================================"
echo "Project:   $GCP_PROJECT_ID"
echo "Region:    $GCP_REGION"
echo "Zone:      $GCP_ZONE"
echo "Bucket:    $GCS_BUCKET"
echo "Workflow:  $WORKFLOW_NAME"
echo "Config:    $CONFIG_FILE ($CONFIG_NAME)"
echo "Tasks:     $TASKS"
echo "======================================================================"
echo ""

# Assign to shorter variable names for compatibility
PROJECT_ID="$GCP_PROJECT_ID"
REGION="$GCP_REGION"
ZONE="$GCP_ZONE"
BUCKET="$GCS_BUCKET"

echo "Starting deployment with managed cluster..."

# 1. Build JAR
echo "1. Building JAR..."
sbt clean package
echo "✓ Built"

# 2. Upload JARs
echo "2. Uploading JARs..."
gsutil cp ./work/apps/$APP_JAR $BUCKET/jars/$APP_JAR
gsutil cp ./work/apps/$MLFLOW_CLIENT_JAR $BUCKET/jars/$MLFLOW_CLIENT_JAR
gsutil cp ./work/apps/$MLFLOW_SPARK_JAR $BUCKET/jars/$MLFLOW_SPARK_JAR
echo "✓ Uploaded"

# 3. Upload config
echo "3. Uploading config..."
gsutil cp src/main/resources/$CONFIG_FILE $BUCKET/config/$CONFIG_FILE
echo "✓ Config uploaded"

# 4. Clear outputs
echo "4. Clearing outputs..."
gsutil -m rm -r $BUCKET/output/ 2>/dev/null || echo "No previous outputs"

# 5. Delete existing workflow template if exists
echo "5. Preparing workflow template..."
if gcloud dataproc workflow-templates describe $WORKFLOW_NAME \
    --region=$REGION \
    --project=$PROJECT_ID &>/dev/null; then
    echo "  Deleting existing template..."
    gcloud dataproc workflow-templates delete $WORKFLOW_NAME \
        --region=$REGION \
        --project=$PROJECT_ID \
        --quiet
else
    echo "  No existing template found"
fi

# 6. Create workflow template (idempotent)
echo "6. Creating workflow template (idempotent)..."
if gcloud dataproc workflow-templates describe $WORKFLOW_NAME \
    --region=$REGION \
    --project=$PROJECT_ID &>/dev/null; then
    echo "  Template already exists, skipping creation"
else
    gcloud dataproc workflow-templates create $WORKFLOW_NAME \
        --region=$REGION \
        --project=$PROJECT_ID
fi

# 7. Configure managed cluster
echo "7. Configuring managed cluster..."
gcloud dataproc workflow-templates set-managed-cluster $WORKFLOW_NAME \
    --region=$REGION \
    --project=$PROJECT_ID \
    --cluster-name=${CLUSTER_NAME} \
    --zone=$ZONE \
    --master-machine-type=${MASTER_MACHINE_TYPE} \
    --master-boot-disk-size=${MASTER_BOOT_DISK_SIZE} \
    --num-workers=${NUM_WORKERS} \
    --worker-machine-type=${WORKER_MACHINE_TYPE} \
    --worker-boot-disk-size=${WORKER_BOOT_DISK_SIZE} \
    --image-version=${IMAGE_VERSION}

# 8. Add Spark job to template
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
spark.driver.memory=${SPARK_DRIVER_MEMORY},\
spark.driver.cores=${SPARK_DRIVER_CORES},\
spark.driver.maxResultSize=${SPARK_DRIVER_MAX_RESULT_SIZE},\
spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS},\
spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM},\
spark.kryoserializer.buffer.max=1024m,\
spark.memory.fraction=0.8,\
spark.rpc.message.maxSize=2047,\
spark.sql.debug.maxToStringFields=1000,\
spark.serializer=org.apache.spark.serializer.KryoSerializer,\
spark.network.timeout=800s,\
spark.executor.heartbeatInterval=60s,\
spark.memory.offHeap.enabled=true,\
spark.memory.offHeap.size=${SPARK_MEMORY_OFF_HEAP_SIZE},\
spark.memory.storageFraction=0.3,\
spark.executor.memory=${SPARK_EXECUTOR_MEMORY},\
spark.executor.cores=${SPARK_EXECUTOR_CORES},\
spark.executorEnv.MLFLOW_TRACKING_URI=$BUCKET/mlflow,\
spark.yarn.appMasterEnv.MLFLOW_TRACKING_URI=$BUCKET/mlflow" \
    -- $CONFIG_NAME "$TASKS"


# 9. Execute workflow
echo "9. Executing workflow..."
WORKFLOW_OUTPUT=$(gcloud dataproc workflow-templates instantiate $WORKFLOW_NAME \
    --region=$REGION \
    --project=$PROJECT_ID 2>&1)

echo "$WORKFLOW_OUTPUT"

# Extract operation ID
OPERATION_ID=$(echo "$WORKFLOW_OUTPUT" | grep -oP 'operations/\K[^\s]+' || echo "$WORKFLOW_OUTPUT" | grep -oP 'Waiting on operation \[\K[^\]]+')

if [ -z "$OPERATION_ID" ]; then
    echo "✗ Failed to extract operation ID"
    exit 1
fi

echo "✓ Workflow started: $OPERATION_ID"
echo "Monitor: https://console.cloud.google.com/dataproc/workflows/instances/$OPERATION_ID?region=$REGION&project=$PROJECT_ID"

echo "✓ Cluster will be created → job executed → cluster deleted automatically"
echo "✓ Check outputs at: $BUCKET/output/"