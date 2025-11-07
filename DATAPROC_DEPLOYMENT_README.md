# 🚀 Google Cloud Dataproc Deployment Guide

This guide explains how to deploy the Flight Delay Prediction application to Google Cloud Dataproc.

## 📋 Prerequisites

Before deploying to Dataproc, ensure you have:

1. **Google Cloud Account** with billing enabled
2. **Google Cloud SDK (gcloud)** installed on your machine
   ```bash
   # Install gcloud CLI
   # https://cloud.google.com/sdk/docs/install
   ```
3. **Google Cloud Project** created
4. **Sufficient permissions** to:
   - Create Dataproc clusters
   - Create Cloud Storage buckets
   - Upload files to Cloud Storage

## 🛠️ Setup Instructions

### Step 1: Configure Google Cloud

```bash
# 1. Authenticate with Google Cloud
gcloud auth login

# 2. Set your project (replace with your project ID)
gcloud config set project YOUR_PROJECT_ID

# 3. Enable required APIs
gcloud services enable dataproc.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable compute.googleapis.com
```

### Step 2: Configure Deployment Settings

We provide an interactive configuration script to set up your deployment:

```bash
# Run the configuration wizard
./configure_dataproc.sh
```

This script will:
- Guide you through all configuration options
- Create a `.env.dataproc` file with your settings
- Provide clear instructions for next steps

#### What You'll Need to Configure:

1. **Google Cloud Project Settings**
   - Project ID (e.g., `my-project-12345`)
   - Region (e.g., `europe-west1`, `us-central1`)
   - Zone (e.g., `europe-west1-b`)

2. **Cloud Storage Bucket**
   - Bucket name (e.g., `gs://my-flight-data-bucket`)
   - This will store your data, JARs, and results

3. **Cluster Configuration**
   - Master machine type (default: `n1-standard-16`)
   - Worker machine type (default: `n1-standard-8`)
   - Number of workers (default: `2`)
   - Boot disk sizes

4. **Application Settings**
   - Config file to use (e.g., `prod-config.yml`)
   - Tasks to run (e.g., `data-pipeline,feature-extraction,train`)

#### Manual Configuration (Alternative)

If you prefer to configure manually:

```bash
# Copy the example configuration
cp .env.dataproc.example .env.dataproc

# Edit the configuration file
nano .env.dataproc  # or use your preferred editor
```

### Step 3: Create Cloud Storage Bucket

```bash
# Get your bucket name from configuration
source .env.dataproc

# Create the bucket (if it doesn't exist)
gsutil mb -l $GCP_REGION $GCS_BUCKET

# Verify bucket creation
gsutil ls | grep $GCS_BUCKET
```

### Step 4: Upload Data to Cloud Storage

```bash
# Upload flight and weather data
gsutil -m cp -r work/data/* $GCS_BUCKET/data/

# Verify upload
gsutil ls -r $GCS_BUCKET/data/
```

Expected structure:
```
gs://your-bucket/data/
  ├── flights/
  │   └── *.parquet files
  ├── weather/
  │   └── *.parquet files
  └── airport-mapping/
      └── *.parquet files
```

### Step 5: Deploy to Dataproc

```bash
# Deploy with default tasks (data-pipeline, feature-extraction, train)
./autodeploy_dataproc.sh

# Or specify custom tasks
./autodeploy_dataproc.sh "data-pipeline,feature-extraction"
```

## 📊 What Happens During Deployment

The deployment script performs the following steps:

1. **Build Application** - Compiles Scala code and creates JAR
2. **Upload JARs** - Uploads application and MLflow JARs to GCS
3. **Upload Configuration** - Uploads your config YAML to GCS
4. **Clear Previous Outputs** - Removes old results from GCS
5. **Create Workflow Template** - Defines the Dataproc workflow
6. **Configure Managed Cluster** - Sets up temporary cluster specifications
7. **Add Spark Job** - Configures the Spark job with all parameters
8. **Execute Workflow** - Starts the workflow (creates cluster, runs job, deletes cluster)

## 🎯 Monitoring Execution

### View Workflow Progress

The deployment script will output a monitoring URL:
```
Monitor: https://console.cloud.google.com/dataproc/workflows/instances/...
```

Click the link to view:
- Cluster creation progress
- Job execution logs
- Resource utilization
- Any errors or warnings

### Check Job Logs

```bash
# View workflow instances
gcloud dataproc operations list \
    --region=$GCP_REGION \
    --project=$GCP_PROJECT_ID

# Get specific operation details
gcloud dataproc operations describe OPERATION_ID \
    --region=$GCP_REGION \
    --project=$GCP_PROJECT_ID
```

### Monitor via Console

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Navigate to **Dataproc** → **Workflows**
3. Click on your workflow instance
4. View logs, metrics, and cluster information

## 📦 Retrieving Results

After the job completes, retrieve your results:

```bash
# Download all results
gsutil -m cp -r $GCS_BUCKET/output/* ./work/output/

# View results structure
gsutil ls -r $GCS_BUCKET/output/

# Download specific experiment results
gsutil -m cp -r $GCS_BUCKET/output/exp_name/* ./work/output/exp_name/
```

### Expected Output Structure

```
output/
  ├── {experiment_name}/
  │   ├── model/           # Trained Spark ML model
  │   ├── metrics/         # Performance metrics CSV
  │   ├── pca/            # PCA analysis results
  │   └── feature-importance/  # Feature rankings
  └── mlflow/             # MLflow tracking data
```

## 💰 Cost Estimation

Approximate costs for a typical run:

| Resource | Configuration | Duration | Estimated Cost* |
|----------|--------------|----------|-----------------|
| Master Node | n1-standard-16 | ~30 min | $0.38 |
| Worker Nodes (2x) | n1-standard-8 | ~30 min | $0.38 |
| Cloud Storage | 50 GB | 1 month | $0.01 |
| **Total** | | **~30 min** | **~$0.80** |

*Costs are approximate and vary by region. See [GCP Pricing](https://cloud.google.com/pricing) for details.

### Cost Optimization Tips

1. **Use Preemptible Workers** - 80% cheaper but can be interrupted
   ```bash
   # Add to cluster configuration in .env.dataproc:
   --num-preemptible-workers=2
   ```

2. **Use Smaller Machines** for testing
   ```bash
   MASTER_MACHINE_TYPE="n1-standard-8"
   WORKER_MACHINE_TYPE="n1-standard-4"
   ```

3. **Delete Resources** when not in use
   ```bash
   # Bucket cleanup (careful!)
   gsutil -m rm -r $GCS_BUCKET/output/old-experiments/
   ```

4. **Set Budget Alerts** in Google Cloud Console

## 🐛 Troubleshooting

### Common Issues

#### 1. Authentication Error
```
ERROR: (gcloud.auth.login) You do not currently have an active account selected.
```
**Solution:**
```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

#### 2. API Not Enabled
```
ERROR: API [dataproc.googleapis.com] not enabled
```
**Solution:**
```bash
gcloud services enable dataproc.googleapis.com
```

#### 3. Insufficient Permissions
```
ERROR: Permission 'dataproc.clusters.create' denied
```
**Solution:** Ask your GCP admin to grant you the "Dataproc Editor" role

#### 4. Bucket Does Not Exist
```
ERROR: gs://your-bucket does not exist
```
**Solution:**
```bash
gsutil mb -l $GCP_REGION $GCS_BUCKET
```

#### 5. Configuration File Not Found
```
❌ Configuration file not found: .env.dataproc
```
**Solution:**
```bash
./configure_dataproc.sh
```

#### 6. Out of Memory Errors

If you see `java.lang.OutOfMemoryError`:

**Solution:** Increase memory allocation in `.env.dataproc`:
```bash
SPARK_DRIVER_MEMORY="60g"
SPARK_EXECUTOR_MEMORY="24g"
MASTER_MACHINE_TYPE="n1-highmem-16"
```

#### 7. Workflow Template Already Exists

The script automatically deletes existing templates, but if you get an error:

```bash
# Manually delete the template
gcloud dataproc workflow-templates delete $WORKFLOW_NAME \
    --region=$GCP_REGION \
    --quiet
```

### Getting Help

If you encounter issues:

1. **Check the logs** in Google Cloud Console
2. **Review configuration** in `.env.dataproc`
3. **Verify permissions** with `gcloud auth list`
4. **Check quotas** in GCP Console → IAM & Admin → Quotas
5. **Contact support** or open an issue on GitHub

## 🔐 Security Best Practices

1. **Never commit** `.env.dataproc` to version control (it's in `.gitignore`)
2. **Use service accounts** for automated deployments
3. **Enable VPC Service Controls** for production environments
4. **Encrypt sensitive data** in Cloud Storage
5. **Regularly rotate credentials**

## 🔄 CI/CD Integration

For automated deployments, you can use this in your CI/CD pipeline:

```yaml
# Example GitHub Actions workflow
name: Deploy to Dataproc

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v0
        with:
          service_account_key: ${{ secrets.GCP_SA_KEY }}
          project_id: ${{ secrets.GCP_PROJECT_ID }}
      
      - name: Create config from secrets
        run: |
          echo "GCP_PROJECT_ID=${{ secrets.GCP_PROJECT_ID }}" > .env.dataproc
          echo "GCS_BUCKET=${{ secrets.GCS_BUCKET }}" >> .env.dataproc
          # ... add other config variables
      
      - name: Deploy
        run: ./deploy_dataproc_wip.sh
```

## 📚 Additional Resources

- [Google Cloud Dataproc Documentation](https://cloud.google.com/dataproc/docs)
- [Dataproc Workflow Templates Guide](https://cloud.google.com/dataproc/docs/concepts/workflows/overview)
- [GCS Best Practices](https://cloud.google.com/storage/docs/best-practices)
- [Spark on Dataproc](https://cloud.google.com/dataproc/docs/tutorials/spark-scala)
- [MLflow on GCP](https://cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning)

## ✅ Quick Reference

```bash
# Complete deployment from scratch
./configure_dataproc.sh           # Configure settings
gsutil mb -l $GCP_REGION $GCS_BUCKET  # Create bucket
gsutil -m cp -r work/data/* $GCS_BUCKET/data/  # Upload data
./deploy_dataproc_wip.sh          # Deploy!

# Check status
gcloud dataproc operations list --region=$GCP_REGION

# Download results
gsutil -m cp -r $GCS_BUCKET/output/* ./work/output/

# Cleanup
gsutil -m rm -r $GCS_BUCKET/output/  # Remove outputs
```

---

**Ready to deploy? Start with `./configure_dataproc.sh`** 🚀
