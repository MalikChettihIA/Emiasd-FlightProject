#!/bin/bash

# ================================================================================================
# Configuration - Modify these values according to your environment
# ================================================================================================

# Cluster Configuration
CLUSTER_HOST="${CLUSTER_HOST:-ssh.lamsade.dauphine.fr}"
CLUSTER_USER="${CLUSTER_USER:-hbalamou}"
SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/id_ed25519_hbalamou.key}"
SSH_PORT="${SSH_PORT:-5022}"

# Workspace Configuration
WORKSPACE="${WORKSPACE:-~/workspace}"
CLUSTER_WORKSPACE="${CLUSTER_WORKSPACE:-/opt/cephfs/users/students/p6emiasd2025/$CLUSTER_USER/workspace}"

# Local Paths
LOCAL_PROJECT_DIR="${LOCAL_PROJECT_DIR:-$(cd "$(dirname "$0")" && pwd)}"
LOCAL_LOG_DIR="${LOCAL_LOG_DIR:-$LOCAL_PROJECT_DIR/logs}"

# HDFS Configuration
HDFS_BASE="${HDFS_BASE:-/students/p6emiasd2025/$CLUSTER_USER}"

# Application Configuration
APP_NAME="${APP_NAME:-Emiasd-Flight-Data-Analysis}"
APP_MAIN_CLASS="${APP_MAIN_CLASS:-com.flightdelay.app.FlightDelayPredictionApp}"
CONFIG_FILE="${CONFIG_FILE:-prodlamsade-config.yml}"

# Spark Configuration
SPARK_MASTER="${SPARK_MASTER:-yarn}"
SPARK_DEPLOY_MODE="${SPARK_DEPLOY_MODE:-client}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-32G}"
SPARK_DRIVER_CORES="${SPARK_DRIVER_CORES:-8}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-8G}"
SPARK_NUM_EXECUTORS="${SPARK_NUM_EXECUTORS:-6}"
SPARK_DRIVER_MAX_RESULT_SIZE="${SPARK_DRIVER_MAX_RESULT_SIZE:-8g}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-400}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-400}"

# Default Tasks
DEFAULT_TASKS="${DEFAULT_TASKS:-data-pipeline,feature-extraction,train}"

# Load custom configuration if exists
CONFIG_FILE_PATH="$LOCAL_PROJECT_DIR/.deployment.config"
if [ -f "$CONFIG_FILE_PATH" ]; then
    echo "Loading configuration from $CONFIG_FILE_PATH"
    source "$CONFIG_FILE_PATH"
fi
# Function to create remote directories if they don't exist
create_remote_dirs() {
    ssh -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" << EOF
        cd $WORKSPACE
        mkdir -p "apps"
        mkdir -p "config"
EOF
}
# Create remote directories
create_remote_dirs
REMOTE_APPS_DIR="$WORKSPACE/apps"
REMOTE_CONFIG_DIR="$WORKSPACE/config"


echo $LOCAL_LOG_DIR
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Fonction pour afficher les étapes
step() {
    echo -e "\n${BLUE}[STEP] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
    exit 1
}

success() {
    echo -e "${GREEN}[SUCCESS] $1${NC}"
}

# Menu principal
show_menu() {
    echo -e "${BLUE}================================================================================================${NC}"
    echo -e "${BLUE}Flight Delay App - Deployment & Execution Manager${NC}"
    echo -e "${BLUE}================================================================================================${NC}"
    echo "1. Full deployment (compile + deploy + run)"
    echo "2. Compile only (sbt clean package)"
    echo "3. Deploy JAR to cluster"
    echo "4. Deploy config to cluster"
    echo "5. Upload data to HDFS"
    echo "6. Run on cluster"
    echo "7. Download logs from cluster"
    echo "8. Tail remote logs (live)"
    echo "9. Stop running application"
    echo "0. Exit"
    echo -e "${BLUE}================================================================================================${NC}"
}

# 1. Compilation locale
compile() {
    step "Compiling with SBT..."
    cd "$LOCAL_PROJECT_DIR" || error "Project directory not found"
    
    sbt clean package || error "Compilation failed"
    
    # Trouver les JAR générés
    JAR_PATH=$(find work/apps -name "$APP_NAME.jar" )
    
    if [ -z "$JAR_PATH" ]; then
        error "JAR file not found after compilation: $APP_NAME.jar"
    fi
    
    success "Compilation complete: $JAR_PATH"
}

# 2. Déployer le JAR
deploy_jar() {
    step "Deploying JAR to cluster..."
    # Copier tous les JARs
    for jar in $(find work/apps -name "*.jar"); do
        jar_name=$(basename "$jar")
        echo "Copying $jar_name..."
        scp -i "$SSH_KEY_PATH" -P "$SSH_PORT" "$jar" "$CLUSTER_USER@$CLUSTER_HOST:$REMOTE_APPS_DIR/$jar_name" || error "Failed to copy JAR: $jar_name"
    done
    success "JAR deployed"
}

# 3. Déployer la configuration
deploy_config() {
    step "Deploying configuration to cluster..."
    
    if [ ! -f "$LOCAL_PROJECT_DIR/src/main/resources/$CONFIG_FILE" ]; then
        error "Configuration file not found: $LOCAL_PROJECT_DIR/src/main/resources/$CONFIG_FILE"
    fi
    
    scp -i "$SSH_KEY_PATH" -P "$SSH_PORT" "$LOCAL_PROJECT_DIR/src/main/resources/$CONFIG_FILE" "$CLUSTER_USER@$CLUSTER_HOST:$REMOTE_CONFIG_DIR/" || error "Failed to copy config"
    success "Configuration deployed"
}

# 4. Déployer les données
deploy_data() {
    step "Deploying data to cluster..."
    
    if [ ! -d "$LOCAL_PROJECT_DIR/data" ]; then
        error "Data directory not found: $LOCAL_PROJECT_DIR/data"
    fi
    
    scp -i "$SSH_KEY_PATH" -P "$SSH_PORT" -r "$LOCAL_PROJECT_DIR/data" "$CLUSTER_USER@$CLUSTER_HOST:$WORKSPACE" || error "Failed to copy data"
    success "Data deployed"
}

# 5. Upload data to HDFS
upload_data_to_hdfs() {
    step "Uploading data to HDFS..."
    
    ssh -i "$SSH_KEY_PATH" -p "$SSH_PORT" -t "$CLUSTER_USER@$CLUSTER_HOST" << EOF

        echo "Checking HDFS directory:"
        
        # Check if HDFS directory exists and is not empty
        if /opt/shared/hadoop-current/bin/hdfs dfs -test -d $HDFS_BASE/data; then
            echo "HDFS directory $HDFS_BASE/data already exists"
            # Check if directory is empty
            if /opt/shared/hadoop-current/bin/hdfs dfs -count $HDFS_BASE/data | awk '{print $2}' | grep -q "^0$"; then
                echo "HDFS directory is empty, uploading data..."
                /opt/shared/hadoop-current/bin/hdfs dfs -put data/* $HDFS_BASE/data
            else
                echo "HDFS directory already contains data, skipping upload"
            fi
        else
            echo "Creating HDFS directory $HDFS_BASE/data"
            /opt/shared/hadoop-current/bin/hdfs dfs -mkdir -p $HDFS_BASE/data
            /opt/shared/hadoop-current/bin/hdfs dfs -put data/* $HDFS_BASE/data
        fi
EOF
    
    if [ $? -ne 0 ]; then
        error "Failed to upload data to HDFS"
    fi
    
    success "Data uploaded to HDFS: $HDFS_BASE/data"
}

# 5. Exécuter sur le cluster
run_on_cluster() {
    step "Running application on cluster..."
    
    TASKS="${1:-$DEFAULT_TASKS}"
    
    echo "Tasks: $TASKS"
    echo "Configuration:"
    echo "  - Spark Master: $SPARK_MASTER"
    echo "  - Deploy Mode: $SPARK_DEPLOY_MODE"
    echo "  - Driver Memory: $SPARK_DRIVER_MEMORY"
    echo "  - Executor Memory: $SPARK_EXECUTOR_MEMORY"
    echo "  - Number of Executors: $SPARK_NUM_EXECUTORS"
    echo "Remote execution will create timestamped logs automatically"
    
    ssh -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" bash << EOF
LOG_DIR=\$HOME/workspace/logs
mkdir -p \$LOG_DIR
TIMESTAMP=\$(date +%Y%m%d_%H%M%S)
LOG_FILE=\$LOG_DIR/flight-app-\${TIMESTAMP}.log
LATEST_LOG=\$LOG_DIR/latest.log
nohup /opt/shared/spark-current/bin/spark-submit \\
  --master $SPARK_MASTER \\
  --deploy-mode $SPARK_DEPLOY_MODE \\
  --class $APP_MAIN_CLASS \\
  --files \$HOME/workspace/config/$CONFIG_FILE \\
  --driver-memory $SPARK_DRIVER_MEMORY \\
  --driver-cores $SPARK_DRIVER_CORES \\
  --executor-memory $SPARK_EXECUTOR_MEMORY \\
  --num-executors $SPARK_NUM_EXECUTORS \\
  --conf spark.driver.maxResultSize=$SPARK_DRIVER_MAX_RESULT_SIZE \\
  --conf spark.sql.shuffle.partitions=$SPARK_SQL_SHUFFLE_PARTITIONS \\
  --conf spark.default.parallelism=$SPARK_DEFAULT_PARALLELISM \\
  --conf spark.kryoserializer.buffer.max=1024m \\
  --conf spark.memory.fraction=0.8 \\
  --conf spark.rpc.message.maxSize=2047 \\
  --conf spark.sql.debug.maxToStringFields=1000 \\
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\
  --conf spark.network.timeout=800s \\
  --conf spark.executor.heartbeatInterval=60s \\
  --conf spark.memory.offHeap.enabled=true \\
  --conf spark.memory.offHeap.size=2g \\
  --conf spark.memory.storageFraction=0.3 \\
  --jars \$HOME/workspace/apps/mlflow-client-3.4.0.jar,\$HOME/workspace/apps/mlflow-spark_2.13-3.4.0.jar \\
  \$HOME/workspace/apps/$APP_NAME.jar \\
  prodlamsade $TASKS > \$LOG_FILE 2>&1 &
ln -sf \$LOG_FILE \$LATEST_LOG
echo "Job started in background. Log file: \$LOG_FILE"
echo "To view logs in real-time on the server, run: tail -F \$LOG_FILE"
EOF
    
    success "Application started. Use option 8 to tail logs or option 7 to download them."
}

# 6. Télécharger les logs
download_logs() {
    step "Downloading logs from cluster..."
    
    LOCAL_LOG_DIR="$LOCAL_PROJECT_DIR/logs-from-cluster"
    mkdir -p "$LOCAL_LOG_DIR"
    
    # Télécharger tous les logs ou juste le dernier ?
    echo "1. Download latest log only"
    echo "2. Download all logs"
    read -p "Choice [1]: " choice
    choice=${choice:-1}
    
    if [ "$choice" == "1" ]; then
        scp "$CLUSTER_USER@$CLUSTER_HOST:$CLUSTER_WORKSPACE/logs/latest.log" "$LOCAL_LOG_DIR/" || error "Failed to download log"
        success "Latest log downloaded to: $LOCAL_LOG_DIR/latest.log"
    else
        scp -r "$CLUSTER_USER@$CLUSTER_HOST:$CLUSTER_WORKSPACE/logs/*" "$LOCAL_LOG_DIR/" || error "Failed to download logs"
        success "All logs downloaded to: $LOCAL_LOG_DIR/"
    fi
}

# 7. Suivre les logs en temps réel
tail_remote_logs() {
    step "Tailing remote logs (Ctrl+C to exit)..."
    
    echo "1. Tail latest.log (symbolic link)"
    echo "2. List all logs and choose one"
    read -p "Choice [1]: " choice
    choice=${choice:-1}
    
    if [ "$choice" == "1" ]; then
        ssh -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" bash --noprofile --norc << 'EOF'
tail -F /opt/cephfs/users/students/p6emiasd2025/hbalamou/workspace/logs/latest.log
EOF
    else
        # Lister les logs disponibles
        echo "Available log files:"
        ssh -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" bash --noprofile --norc << 'EOF'
ls -lht /opt/cephfs/users/students/p6emiasd2025/hbalamou/workspace/logs/*.log | head -10
EOF
        
        read -p "Enter log filename (e.g., flight-app-20251112_202612.log): " log_file
        
        if [ -n "$log_file" ]; then
            ssh -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" bash --noprofile --norc << EOF
tail -F /opt/cephfs/users/students/p6emiasd2025/hbalamou/workspace/logs/$log_file
EOF
        else
            error "No log file specified"
        fi
    fi
}

# 8. Arrêter l'application
stop_app() {
    step "Stopping application on cluster..."
    
    ssh "$CLUSTER_USER@$CLUSTER_HOST" << 'EOF'
        cd /opt/cephfs/users/students/p6emiasd2025/$CLUSTER_USER/workspace
        if [ -f stop-flight-app.sh ]; then
            ./stop-flight-app.sh << ANSWERS
1
ANSWERS
        else
            echo "Stopping via YARN..."
            yarn application -list | grep "Flight Delay" | awk '{print $1}' | xargs -I {} yarn application -kill {}
        fi
EOF
    
    success "Stop command executed"
}

# 9. Déploiement complet
full_deployment() {
    compile
    deploy_jar
    deploy_config
    # Deploy local data directory to remote workspace before HDFS upload
    deploy_data
    
    echo -e "\n${YELLOW}Ready to run. Execute now? (y/n)${NC}"
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        run_on_cluster
    fi
}

# Menu interactif
while true; do
    show_menu
    read -p "Choose option: " option
    
    case $option in
        1) full_deployment ;;
        2) compile ;;
        3) deploy_jar ;;
        4) deploy_config ;;
        5) upload_data_to_hdfs ;;
        6) 
            read -p "Tasks [$DEFAULT_TASKS]: " tasks
            run_on_cluster "$tasks"
            ;;
        7) download_logs ;;
        8) tail_remote_logs ;;
        9) stop_app ;;
        0) exit 0 ;;
        *) error "Invalid option" ;;
    esac
    
    echo ""
    read -p "Press Enter to continue..."
done
