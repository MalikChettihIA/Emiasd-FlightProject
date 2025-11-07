#!/bin/bash

# ====================================================================
# Interactive Dataproc Configuration Script
# ====================================================================
# This script helps users configure their Google Cloud Dataproc settings
# ====================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env.dataproc"
ENV_EXAMPLE="$SCRIPT_DIR/.env.dataproc.example"

echo "======================================================================"
echo "  Google Cloud Dataproc Configuration"
echo "======================================================================"
echo ""

# Check if .env.dataproc already exists
if [ -f "$ENV_FILE" ]; then
    echo "⚠️  Configuration file already exists: $ENV_FILE"
    read -p "Do you want to reconfigure? (y/N): " -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Configuration cancelled. Using existing configuration."
        exit 0
    fi
fi

# Copy example file
if [ ! -f "$ENV_EXAMPLE" ]; then
    echo "❌ Error: .env.dataproc.example file not found!"
    exit 1
fi

cp "$ENV_EXAMPLE" "$ENV_FILE"

echo "Let's configure your Google Cloud Dataproc settings."
echo "Press Enter to keep the default value shown in brackets."
echo ""

# Function to prompt for input with default value
prompt_input() {
    local var_name=$1
    local prompt_text=$2
    local default_value=$3
    local value
    
    read -p "$prompt_text [$default_value]: " value
    value=${value:-$default_value}
    
    # Update .env file
    sed -i "s|^${var_name}=.*|${var_name}=\"${value}\"|" "$ENV_FILE"
}

# Function to get current value from .env file
get_current_value() {
    local var_name=$1
    grep "^${var_name}=" "$ENV_FILE" | cut -d'"' -f2
}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  1. Google Cloud Project Settings"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "You can find your project ID at: https://console.cloud.google.com"
prompt_input "GCP_PROJECT_ID" "Enter your GCP Project ID" "$(get_current_value GCP_PROJECT_ID)"

echo ""
echo "Common regions: europe-west1, us-central1, asia-east1"
prompt_input "GCP_REGION" "Enter your GCP Region" "$(get_current_value GCP_REGION)"

echo ""
prompt_input "GCP_ZONE" "Enter your GCP Zone" "$(get_current_value GCP_ZONE)"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  2. Google Cloud Storage"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Enter your GCS bucket name (format: gs://your-bucket-name)"
echo "Create a bucket at: https://console.cloud.google.com/storage"
prompt_input "GCS_BUCKET" "Enter your GCS Bucket" "$(get_current_value GCS_BUCKET)"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  3. Workflow Configuration"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
prompt_input "WORKFLOW_NAME" "Enter workflow name" "$(get_current_value WORKFLOW_NAME)"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  4. Cluster Configuration"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Machine types: n1-standard-4, n1-standard-8, n1-standard-16, n1-highmem-8, etc."
echo "See: https://cloud.google.com/compute/docs/machine-types"
echo ""
prompt_input "MASTER_MACHINE_TYPE" "Master machine type" "$(get_current_value MASTER_MACHINE_TYPE)"
prompt_input "MASTER_BOOT_DISK_SIZE" "Master boot disk size" "$(get_current_value MASTER_BOOT_DISK_SIZE)"

echo ""
prompt_input "NUM_WORKERS" "Number of workers" "$(get_current_value NUM_WORKERS)"
prompt_input "WORKER_MACHINE_TYPE" "Worker machine type" "$(get_current_value WORKER_MACHINE_TYPE)"
prompt_input "WORKER_BOOT_DISK_SIZE" "Worker boot disk size" "$(get_current_value WORKER_BOOT_DISK_SIZE)"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  5. Application Configuration"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Config file should exist in src/main/resources/"
prompt_input "CONFIG_FILE" "Configuration file name" "$(get_current_value CONFIG_FILE)"
prompt_input "CONFIG_NAME" "Configuration name" "$(get_current_value CONFIG_NAME)"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "✅ Configuration saved to: $ENV_FILE"
echo ""
echo "Next steps:"
echo "  1. Make sure you're authenticated with gcloud:"
echo "     gcloud auth login"
echo "     gcloud config set project $(get_current_value GCP_PROJECT_ID)"
echo ""
echo "  2. Enable required APIs:"
echo "     gcloud services enable dataproc.googleapis.com"
echo "     gcloud services enable storage.googleapis.com"
echo ""
echo "  3. Create your GCS bucket if it doesn't exist:"
echo "     gsutil mb -l $(get_current_value GCP_REGION) $(get_current_value GCS_BUCKET)"
echo ""
echo "  4. Upload your data to the bucket:"
echo "     gsutil cp -r work/data/* $(get_current_value GCS_BUCKET)/data/"
echo ""
echo "  5. Deploy to Dataproc:"
echo "     ./deploy_to_dataproc.sh"
echo ""
echo "======================================================================"
