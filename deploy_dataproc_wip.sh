#!/bin/bash

# Safer bash defaults
set -euo pipefail

# Configuration
PROJECT_ID="tough-artwork-475804-h0"
WORKFLOW_NAME="flight-delay-workflow"
REGION="europe-west1"
ZONE="europe-west1-b"
BUCKET="gs://tough-artwork-475804-h0-flight-data"

# JARs
# Use only the standard sbt package output (non-assembly): work/apps/Emiasd-Flight-Data-Analysis.jar
APP_JAR="Emiasd-Flight-Data-Analysis.jar"
MLFLOW_CLIENT_JAR="mlflow-client-3.4.0.jar"
MLFLOW_SPARK_JAR="mlflow-spark_2.13-3.4.0.jar"

CONFIG_FILE="prod-config.yml"
CONFIG_NAME="prod"
TASKS="${1:-data-pipeline,feature-extraction,train}"
# TASKS="${1:-data-pipeline,feature-extraction,train}"

echo "Starting deployment with managed cluster..."

# 1. Build JAR
echo "1. Building JAR..."
sbt clean package
if [ ! -f "./work/apps/$APP_JAR" ]; then
    echo "[ERROR] Expected application JAR ./work/apps/$APP_JAR not found after build." >&2
    ls -l ./work/apps 2>/dev/null || echo "(work/apps directory missing)"
    exit 1
fi
echo "✓ Built: $APP_JAR"

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

echo "5. Preparing workflow template..."

# Ensure gcloud is authenticated and the right project is selected (best-effort, non-fatal)
gcloud --version >/dev/null 2>&1 || { echo "gcloud is not installed or not in PATH"; exit 1; }
gsutil version -l >/dev/null 2>&1 || { echo "gsutil is not installed or not in PATH"; exit 1; }

# Try to set project to avoid context drift after re-auth
gcloud config set project "$PROJECT_ID" >/dev/null 2>&1 || true
gcloud config set dataproc/region "$REGION" >/dev/null 2>&1 || true

# Helper: create template; if it already exists (possibly due to delayed auth), delete and recreate
create_or_recreate_template() {
    local create_out
    set +e
    create_out=$(gcloud dataproc workflow-templates create "$WORKFLOW_NAME" \
            --region="$REGION" \
            --project="$PROJECT_ID" 2>&1)
    local create_rc=$?
    set -e
    if [[ $create_rc -eq 0 ]]; then
        echo "  Template created"
        return 0
    fi
    if echo "$create_out" | grep -q "ALREADY_EXISTS"; then
        echo "  Template already exists; ensuring clean state (delete → create)"
        # Delete may fail if not found or due to perms; try once and continue on failure
        gcloud dataproc workflow-templates delete "$WORKFLOW_NAME" \
            --region="$REGION" \
            --project="$PROJECT_ID" \
            --quiet >/dev/null 2>&1 || true
        # Recreate (now that auth context is warmed up)
        gcloud dataproc workflow-templates create "$WORKFLOW_NAME" \
            --region="$REGION" \
            --project="$PROJECT_ID"
        echo "  Template recreated"
        return 0
    fi
    echo "$create_out"
    return $create_rc
}

# First, try to delete quietly to avoid duplicate steps if it exists, ignore errors (e.g., not found or pre-auth)
gcloud dataproc workflow-templates delete "$WORKFLOW_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --quiet >/dev/null 2>&1 || true

echo "6. Creating workflow template (idempotent)..."
create_or_recreate_template

# 7. Configure managed cluster
echo "7. Configuring managed cluster..."
gcloud dataproc workflow-templates set-managed-cluster $WORKFLOW_NAME \
    --region=$REGION \
    --project=$PROJECT_ID \
    --cluster-name=flight-temp-cluster \
    --zone=$ZONE \
    --master-machine-type=n1-standard-16 \
    --master-boot-disk-size=100GB \
    --num-workers=2 \
    --worker-machine-type=n1-standard-8 \
    --worker-boot-disk-size=100GB \
    --image-version=2.2-debian12

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
spark.driver.memory=40g,\
spark.driver.cores=10,\
spark.driver.maxResultSize=8g,\
spark.sql.shuffle.partitions=400,\
spark.default.parallelism=400,\
spark.kryoserializer.buffer.max=1024m,\
spark.memory.fraction=0.8,\
spark.rpc.message.maxSize=2047,\
spark.sql.debug.maxToStringFields=1000,\
spark.serializer=org.apache.spark.serializer.KryoSerializer,\
spark.network.timeout=800s,\
spark.executor.heartbeatInterval=60s,\
spark.memory.offHeap.enabled=true,\
spark.memory.offHeap.size=10g,\
spark.memory.storageFraction=0.3,\
spark.executor.memory=16g,\
spark.executor.cores=6,\
spark.executorEnv.MLFLOW_TRACKING_URI=$BUCKET/mlflow,\
spark.yarn.appMasterEnv.MLFLOW_TRACKING_URI=$BUCKET/mlflow" \
    -- $CONFIG_NAME "$TASKS"


# 9. Execute workflow
echo "9. Executing workflow..."
WORKFLOW_OUTPUT=$(gcloud dataproc workflow-templates instantiate $WORKFLOW_NAME \
    --region=$REGION \
    --project=$PROJECT_ID 2>&1)

echo "$WORKFLOW_OUTPUT"

# Print the output of the workflow execution for debugging
echo "Workflow Output: $WORKFLOW_OUTPUT"

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