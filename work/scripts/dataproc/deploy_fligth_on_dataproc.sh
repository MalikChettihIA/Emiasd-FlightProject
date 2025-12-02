#!/bin/bash

set -euo pipefail

# ============================================================================
# CONFIGURATION
# ============================================================================

# Valeurs par défaut (modifiables via variables d'environnement)
PROJECT_ID="${GCP_PROJECT_ID:-tough-artwork-475804-h0}"
REGION="${GCP_REGION:-europe-west1}"
ZONE="${GCP_ZONE:-europe-west1-b}"
BUCKET="gs://${PROJECT_ID}-flight-data"
CONFIG_FILE="${CONFIG_FILE:-proddataproc-config.yml}"
CONFIG_NAME="${CONFIG_NAME:-proddataproc}"
TASKS="${1:-data-pipeline,feature-extraction,train}"

WORKFLOW_NAME="flight-delay-workflow"
APP_JAR="Emiasd-Flight-Data-Analysis.jar"
MLFLOW_CLIENT_JAR="mlflow-client-3.4.0.jar"
MLFLOW_SPARK_JAR="mlflow-spark_2.13-3.4.0.jar"

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ============================================================================
# VÉRIFICATIONS PRÉLIMINAIRES
# ============================================================================

echo -e "${BLUE}=== Flight Delay Prediction - Dataproc Deployment ===${NC}"
echo "Project: $PROJECT_ID | Region: $REGION"
echo ""

# Vérifier gcloud
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}✗ gcloud CLI non installé${NC}"
    echo "Installation: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

if ! command -v gsutil &> /dev/null; then
    echo -e "${RED}✗ gsutil non installé${NC}"
    exit 1
fi

# Vérifier authentification
AUTH_ACCOUNT=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null || echo "")
if [ -z "$AUTH_ACCOUNT" ]; then
    echo -e "${RED}✗ Non authentifié${NC}"
    echo "Exécutez: ${BLUE}gcloud auth login${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Authentifié: ${AUTH_ACCOUNT}${NC}"

# Configurer projet et région
ACTIVE_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
if [ "$ACTIVE_PROJECT" != "$PROJECT_ID" ]; then
    echo "Configuration du projet $PROJECT_ID..."
    gcloud config set project "$PROJECT_ID" >/dev/null 2>&1
fi
gcloud config set dataproc/region "$REGION" >/dev/null 2>&1
echo -e "${GREEN}✓ Projet: ${PROJECT_ID}${NC}"

# Vérifier bucket
if ! gsutil ls "$BUCKET" &> /dev/null; then
    echo -e "${YELLOW}⚠ Bucket inexistant: ${BUCKET}${NC}"
    read -p "Créer le bucket ? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        gsutil mb -l "$REGION" "$BUCKET"
        echo -e "${GREEN}✓ Bucket créé${NC}"
    else
        exit 1
    fi
else
    echo -e "${GREEN}✓ Bucket accessible${NC}"
fi

# Vérifier APIs
REQUIRED_APIS=("dataproc.googleapis.com" "storage.googleapis.com")
for api in "${REQUIRED_APIS[@]}"; do
    if ! gcloud services list --enabled --filter="name:$api" --format="value(name)" 2>/dev/null | grep -q "$api"; then
        echo "Activation de $api..."
        gcloud services enable "$api"
    fi
done
echo -e "${GREEN}✓ APIs activées${NC}"

echo ""

# ============================================================================
# DÉPLOIEMENT
# ============================================================================

echo "1. Building JAR..."
sbt clean package
if [ ! -f "./work/apps/$APP_JAR" ]; then
    echo -e "${RED}✗ JAR manquant${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Built${NC}"

echo "2. Uploading artifacts..."
gsutil -m cp ./work/apps/$APP_JAR ./work/apps/$MLFLOW_CLIENT_JAR ./work/apps/$MLFLOW_SPARK_JAR \
    $BUCKET/jars/
gsutil cp src/main/resources/$CONFIG_FILE $BUCKET/config/$CONFIG_FILE
echo -e "${GREEN}✓ Uploaded${NC}"

echo "3. Clearing outputs..."
gsutil -m rm -r $BUCKET/output/ 2>/dev/null || true
echo -e "${GREEN}✓ Cleared${NC}"

echo "4. Configuring workflow..."
gcloud dataproc workflow-templates delete "$WORKFLOW_NAME" \
    --region="$REGION" --project="$PROJECT_ID" --quiet 2>/dev/null || true

gcloud dataproc workflow-templates create "$WORKFLOW_NAME" \
    --region="$REGION" --project="$PROJECT_ID"

gcloud dataproc workflow-templates set-managed-cluster $WORKFLOW_NAME \
    --region=$REGION --project=$PROJECT_ID \
    --cluster-name=flight-temp-cluster --zone=$ZONE \
    --master-machine-type=n1-standard-8 --master-boot-disk-size=100GB \
    --num-workers=4 --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=100GB --image-version=2.2-debian12

gcloud dataproc workflow-templates add-job spark \
    --workflow-template=$WORKFLOW_NAME --region=$REGION --project=$PROJECT_ID \
    --step-id=flight-delay-job \
    --class=com.flightdelay.app.FlightDelayPredictionApp \
    --jars=$BUCKET/jars/$APP_JAR,$BUCKET/jars/$MLFLOW_CLIENT_JAR,$BUCKET/jars/$MLFLOW_SPARK_JAR \
    --files=$BUCKET/config/$CONFIG_FILE \
    --properties="\
spark.driver.memory=20g,spark.driver.cores=6,spark.driver.maxResultSize=6g,\
spark.driver.memoryOverhead=2g,spark.executor.memory=8g,spark.executor.cores=3,\
spark.executor.memoryOverhead=2g,spark.sql.shuffle.partitions=300,\
spark.default.parallelism=300,spark.memory.fraction=0.75,\
spark.memory.storageFraction=0.5,\
spark.serializer=org.apache.spark.serializer.KryoSerializer,\
spark.kryoserializer.buffer.max=1024m,spark.rpc.message.maxSize=2047,\
spark.sql.debug.maxToStringFields=1000,spark.network.timeout=800s,\
spark.executor.heartbeatInterval=60s,spark.dynamicAllocation.enabled=false,\
spark.sql.adaptive.enabled=true,spark.sql.adaptive.coalescePartitions.enabled=true,\
spark.sql.adaptive.skewJoin.enabled=true,\
spark.executorEnv.MLFLOW_TRACKING_URI=$BUCKET/mlflow,\
spark.yarn.appMasterEnv.MLFLOW_TRACKING_URI=$BUCKET/mlflow" \
    -- $CONFIG_NAME "$TASKS"

echo -e "${GREEN}✓ Configured${NC}"

echo "5. Executing workflow..."
WORKFLOW_OUTPUT=$(gcloud dataproc workflow-templates instantiate $WORKFLOW_NAME \
    --region=$REGION --project=$PROJECT_ID 2>&1)

OPERATION_ID=$(echo "$WORKFLOW_OUTPUT" | grep -oP 'operations/\K[^\s]+' || \
               echo "$WORKFLOW_OUTPUT" | grep -oP 'Waiting on operation \[\K[^\]]+')

if [ -z "$OPERATION_ID" ]; then
    echo -e "${RED}✗ Échec${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✓ Workflow démarré: ${OPERATION_ID}${NC}"
echo "Monitor: https://console.cloud.google.com/dataproc/workflows/instances/$OPERATION_ID?region=$REGION&project=$PROJECT_ID"
echo "Outputs: $BUCKET/output/"