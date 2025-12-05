#!/bin/bash

# Configuration
CLUSTER_HOST="ssh.lamsade.dauphine.fr"
CLUSTER_USER="hbalamou"
WORKSPACE="~/workspace"
CLUSTER_WORKSPACE="/opt/cephfs/users/students/p6emiasd2025/$CLUSTER_USER/workspace"


LOCAL_PROJECT_DIR="/home/nribal/psl/06-flight/02-git/Emiasd-FlightProject" 
LOCAL_LOG_DIR="/home/nribal/psl/06-flight/02-git/Emiasd-FlightProject/logs"
SSH_KEY_PATH="/home/nribal/.ssh/id_ed25519_hbalamou.key"
SSH_PORT="5022"
# Base HDFS path used on the remote cluster (parameterized for easier maintenance)
HDFS_BASE="/students/p6emiasd2025/$CLUSTER_USER"
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
    echo "10. Run client DER script (workspace/run_client_der.sh)"
    echo "11. Transfer client DER logs (auto-detect latest)"
    echo "0. Exit"
    echo -e "${BLUE}================================================================================================${NC}"
}

# 1. Compilation locale
compile() {
    step "Compiling with SBT..."
    cd "$LOCAL_PROJECT_DIR" || error "Project directory not found"
    
    sbt clean package || error "Compilation failed"
    
    # Trouver les JAR générés
    JAR_PATH=$(find work/apps -name "Emiasd-Flight-Data-Analysis.jar" )
    
    if [ -z "$JAR_PATH" ]; then
        error "JAR file not found after compilation"
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
    
    if [ ! -f "$LOCAL_PROJECT_DIR/src/main/resources/prodlamsade-config.yml" ]; then
        error "Configuration file not found: $LOCAL_PROJECT_DIR/src/main/resources/prodlamsade-config.yml"
    fi
    
    scp -i "$SSH_KEY_PATH" -P "$SSH_PORT" "$LOCAL_PROJECT_DIR/src/main/resources/prodlamsade-config.yml" "$CLUSTER_USER@$CLUSTER_HOST:$REMOTE_CONFIG_DIR/" || error "Failed to copy config"
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
    
    ssh -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" << 'EOF'
        set -e
        SCRIPT_PATH="$CLUSTER_WORKSPACE/run_client_der.sh"
        
        [ -f "$SCRIPT_PATH" ] || { echo "ERROR: Script not found"; exit 1; }
        
        if pgrep -f "run_client_der.sh" > /dev/null; then
            echo "ERROR: Already running (PID: $(pgrep -f run_client_der.sh))"
            exit 1
        fi
        
        cd "$CLUSTER_WORKSPACE"
        nohup bash "$SCRIPT_PATH" > /dev/null 2>&1 &
        PID=$!
        
        sleep 1
        ps -p $PID > /dev/null || { echo "ERROR: Failed to start"; exit 1; }
        echo "Started with PID $PID"
EOF
    
    [ $? -eq 0 ] && success "Application started" || error "Failed to start"
}

# 6. Télécharger les logs
download_logs() {
    step "Downloading logs from cluster..."
    
    LOCAL_LOG_DIR="$LOCAL_PROJECT_DIR/logs-from-cluster"
    mkdir -p "$LOCAL_LOG_DIR"
    
    # Télécharger tous les logs ou juste le dernier ?
    echo "1. Download latest log directory/file"
    echo "2. Download all logs"
    read -p "Choice [1]: " choice
    choice=${choice:-1}
    
    if [ "$choice" == "1" ]; then
        # Essayer de trouver le dernier dossier de logs
        echo "Searching for latest log directory..."
        
        # Use LogLevel=QUIET to suppress SSH banner completely
        # This is the most reliable way to get clean output
        LATEST_DIR_NAME=$(ssh -o LogLevel=QUIET -n -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" "cd $CLUSTER_WORKSPACE/logs 2>/dev/null && ls -td */ 2>/dev/null | head -n 1 | tr -d '/'")
        
        # Clean up any remaining whitespace
        LATEST_DIR_NAME=$(echo "$LATEST_DIR_NAME" | tr -d '\r\n\t ')

        echo "Debug: Extracted directory name: '$LATEST_DIR_NAME'"

        if [ -n "$LATEST_DIR_NAME" ]; then
            LATEST_REMOTE_PATH="$CLUSTER_WORKSPACE/logs/$LATEST_DIR_NAME"
            echo "Found latest directory: $LATEST_DIR_NAME"
            echo "Full remote path: $LATEST_REMOTE_PATH"
            scp -i "$SSH_KEY_PATH" -P "$SSH_PORT" -r "$CLUSTER_USER@$CLUSTER_HOST:$LATEST_REMOTE_PATH" "$LOCAL_LOG_DIR/" || error "Failed to download log directory"
            success "Latest log directory downloaded to: $LOCAL_LOG_DIR/$LATEST_DIR_NAME"
        else
            # Fallback : essayer latest.log (fichier)
            echo "No log directory found. Trying latest.log file..."
            scp -i "$SSH_KEY_PATH" -P "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST:$CLUSTER_WORKSPACE/logs/latest.log" "$LOCAL_LOG_DIR/" || error "Failed to download log"
            success "Latest log downloaded to: $LOCAL_LOG_DIR/latest.log"
        fi
    else
        scp -i "$SSH_KEY_PATH" -P "$SSH_PORT" -r "$CLUSTER_USER@$CLUSTER_HOST:$CLUSTER_WORKSPACE/logs/*" "$LOCAL_LOG_DIR/" || error "Failed to download logs"
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
    
    ssh -i "$SSH_KEY_PATH" -p "$SSH_PORT" -T "$CLUSTER_USER@$CLUSTER_HOST" bash << 'EOF'
echo "=========================================="
echo "Searching for running Spark applications..."
echo "=========================================="

# Method 1: Kill spark-submit process (for client mode with nohup)
echo -e "\n[1] Killing spark-submit processes..."
SPARK_PIDS=$(ps aux | grep "[s]park-submit.*FlightDelayPredictionApp" | awk '{print $2}')

if [ -n "$SPARK_PIDS" ]; then
    echo "Found spark-submit PIDs: $SPARK_PIDS"
    for PID in $SPARK_PIDS; do
        echo "Killing PID $PID..."
        kill -15 $PID 2>/dev/null || kill -9 $PID 2>/dev/null
    done
    sleep 2
    echo "Spark processes killed"
else
    echo "No spark-submit processes found"
fi

# Method 2: Kill YARN applications
echo -e "\n[2] Checking YARN applications..."
YARN_APPS=$(/opt/shared/hadoop-current/bin/yarn application -list 2>/dev/null | grep -i "FlightDelayPredictionApp\|flight" | awk '{print $1}')

if [ -n "$YARN_APPS" ]; then
    echo "Found YARN applications:"
    echo "$YARN_APPS"
    for APP_ID in $YARN_APPS; do
        echo "Killing YARN application: $APP_ID"
        /opt/shared/hadoop-current/bin/yarn application -kill $APP_ID
    done
else
    echo "No YARN applications found"
fi

# Method 3: Verification
echo -e "\n[3] Verification..."
sleep 2
REMAINING=$(ps aux | grep "[s]park-submit.*FlightDelayPredictionApp")
if [ -z "$REMAINING" ]; then
    echo "✓ All Spark processes stopped successfully"
else
    echo "⚠ Some processes may still be running:"
    echo "$REMAINING"
fi

echo "=========================================="
echo "Stop operation completed"
echo "=========================================="
EOF
    
    if [ $? -eq 0 ]; then
        success "Stop command executed successfully"
    else
        error "Failed to stop application"
    fi
}

# 10. Run client DER script
run_client_der() {
    step "Executing run_client_der.sh from workspace on cluster..."
    
    ssh -i "$SSH_KEY_PATH" -p "$SSH_PORT" -T "$CLUSTER_USER@$CLUSTER_HOST" bash << EOF
        SCRIPT_PATH="$CLUSTER_WORKSPACE/run_client_der.sh"
        LOGS_DIR="$CLUSTER_WORKSPACE/logs"
        
        if [ ! -f "\$SCRIPT_PATH" ]; then
            echo "ERROR: run_client_der.sh not found at \$SCRIPT_PATH"
            exit 1
        fi
        
        # Check if script is already running
        if ps aux | grep "[r]un_client_der.sh" > /dev/null; then
            echo "WARNING: run_client_der.sh is already running"
            echo "Existing process:"
            ps aux | grep "[r]un_client_der.sh"
            exit 1
        fi
        
        # Ensure logs directory exists
        mkdir -p "\$LOGS_DIR"
        
        # Execute script in background and capture output
        echo "Starting run_client_der.sh from \$SCRIPT_PATH..."
        cd "$CLUSTER_WORKSPACE"
        nohup bash "\$SCRIPT_PATH" > "\$LOGS_DIR/run_client_der_\$(date +%Y%m%d_%H%M%S).out" 2>&1 &
        SCRIPT_PID=\$!
        echo "Script started with PID: \$SCRIPT_PID"
        echo "Monitor with: ps aux | grep run_client_der.sh"
EOF
    
    if [ $? -eq 0 ]; then
        success "Client DER script started successfully"
        echo "Logs will be created in $CLUSTER_WORKSPACE/logs/ with format: optimized-client-YYYYMMDD_HHMMSS"
    else
        error "Failed to start client DER script"
    fi
}

# 11. Transfer client DER logs (auto-detect latest, wait for completion)
transfer_client_logs() {
    step "Transferring latest client DER log directory..."
    
    # Check if script is still running
    echo "Checking if run_client_der.sh is still running..."
    IS_RUNNING=$(ssh -o LogLevel=QUIET -n -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" "ps aux | grep '[r]un_client_der.sh' > /dev/null && echo 'yes' || echo 'no'")
    
    if [ "$IS_RUNNING" = "yes" ]; then
        echo -e "${YELLOW}[WARNING] run_client_der.sh is still running.${NC}"
        echo "Process running message - waiting for script to complete..."
        
        # Show running process info
        ssh -o LogLevel=QUIET -n -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" "ps aux | grep '[r]un_client_der.sh'"
        
        # Wait for script to finish (with timeout)
        MAX_WAIT=3600  # 1 hour max wait
        WAIT_COUNT=0
        CHECK_INTERVAL=5  # Check every 5 seconds
        
        echo "Waiting for completion (max ${MAX_WAIT}s)..."
        while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
            IS_RUNNING=$(ssh -o LogLevel=QUIET -n -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" "ps aux | grep '[r]un_client_der.sh' > /dev/null && echo 'yes' || echo 'no'")
            
            if [ "$IS_RUNNING" = "no" ]; then
                echo ""
                echo "Script completed!"
                break
            fi
            
            WAIT_COUNT=$((WAIT_COUNT + CHECK_INTERVAL))
            if [ $((WAIT_COUNT % 30)) -eq 0 ]; then
                echo -n " [${WAIT_COUNT}s]"
            else
                echo -n "."
            fi
            sleep $CHECK_INTERVAL
        done
        
        if [ "$IS_RUNNING" = "yes" ]; then
            echo ""
            echo -e "${YELLOW}[WARNING] Script still running after ${MAX_WAIT}s timeout. Transfer cancelled.${NC}"
            echo "You can manually check status and transfer later."
            return 1
        fi
    else
        echo "Script is not running. Proceeding with transfer..."
    fi
    
    # Find the most recent log directory matching the pattern
    echo "Detecting latest log directory..."
    echo "Searching in: $CLUSTER_WORKSPACE/logs"
    
    # Use absolute path directly to avoid variable expansion issues
    REMOTE_LOGS_DIR="$CLUSTER_WORKSPACE/logs"
    
    # Direct approach: Execute command and filter output aggressively
    # The pattern 'optimized-client-YYYYMMDD_HHMMSS' should be unique enough to extract even with banners
    RAW_OUTPUT=$(ssh -q -o LogLevel=ERROR -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -n -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" "cd '$REMOTE_LOGS_DIR' 2>/dev/null && ls -1td optimized-client-* 2>/dev/null" 2>&1)
    
    # Extract directory names - the pattern should match: optimized-client-20251129_101841
    # Use grep to find all matches, then filter to get only valid ones
    LATEST_DIR_NAME=$(echo "$RAW_OUTPUT" | grep -oE 'optimized-client-[0-9]{8}_[0-9]{6}' | head -n 1)
    
    # If still empty, try with more permissive pattern (in case of extra chars)
    if [ -z "$LATEST_DIR_NAME" ]; then
        # Try matching with word boundaries or line boundaries
        LATEST_DIR_NAME=$(echo "$RAW_OUTPUT" | grep -oE '(^|[^a-zA-Z0-9_-])optimized-client-[0-9]{8}_[0-9]{6}([^a-zA-Z0-9_-]|$)' | grep -oE 'optimized-client-[0-9]{8}_[0-9]{6}' | head -n 1)
    fi
    
    # Debug: if still empty, show what we got
    if [ -z "$LATEST_DIR_NAME" ]; then
        echo "Debug: Attempting to extract from raw output..."
        echo "Raw output length: ${#RAW_OUTPUT} characters"
        echo "First 200 chars of output:"
        echo "$RAW_OUTPUT" | head -c 200
        echo ""
        echo "Looking for pattern 'optimized-client-' in output:"
        echo "$RAW_OUTPUT" | grep -o 'optimized-client-' | head -5
        echo ""
    fi
    
    if [ -z "$LATEST_DIR_NAME" ]; then
        # Try alternative: use find command with direct output
        echo "Trying alternative detection method with find..."
        RAW_OUTPUT=$(ssh -q -o LogLevel=ERROR -o BatchMode=yes -n -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" "cd '$REMOTE_LOGS_DIR' 2>/dev/null && find . -maxdepth 1 -type d -name 'optimized-client-*' -printf '%T@ %f\n' 2>/dev/null | sort -rn | head -n 1 | awk '{print \$2}'" 2>&1)
        LATEST_DIR_NAME=$(echo "$RAW_OUTPUT" | grep -oE 'optimized-client-[0-9]{8}_[0-9]{6}' | head -n 1)
    fi
    
    if [ -z "$LATEST_DIR_NAME" ]; then
        # Last resort: try with full path
        echo "Trying with full path listing..."
        RAW_OUTPUT=$(ssh -q -o LogLevel=ERROR -o BatchMode=yes -n -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" "ls -1d '$REMOTE_LOGS_DIR'/optimized-client-* 2>/dev/null" 2>&1)
        # Extract directory name from full paths
        LATEST_DIR_NAME=$(echo "$RAW_OUTPUT" | sed "s|.*/||" | grep -oE 'optimized-client-[0-9]{8}_[0-9]{6}' | head -n 1)
    fi
    
    if [ -z "$LATEST_DIR_NAME" ]; then
        echo "Error: Could not find directory. Verifying path and listing contents..."
        # First check if directory exists
        DIR_CHECK=$(ssh -q -o LogLevel=ERROR -o BatchMode=yes -n -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" "test -d '$REMOTE_LOGS_DIR' && echo 'EXISTS' || echo 'NOT_EXISTS'" 2>&1 | grep -E 'EXISTS|NOT_EXISTS' | head -1)
        echo "Logs directory check: $DIR_CHECK"
        
        if [ "$DIR_CHECK" = "EXISTS" ]; then
            echo "Listing all contents of $REMOTE_LOGS_DIR:"
            # List all contents and show them
            LIST_OUTPUT=$(ssh -q -o LogLevel=ERROR -o BatchMode=yes -n -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" "ls -la '$REMOTE_LOGS_DIR' 2>&1" 2>&1)
            # Filter out banner and show only file/directory listings
            echo "$LIST_OUTPUT" | grep -E '^[d-]|optimized-client|total' | head -20
            
            # Try to extract directory names from the listing
            LATEST_DIR_NAME=$(echo "$LIST_OUTPUT" | grep -oE 'optimized-client-[0-9]{8}_[0-9]{6}' | head -n 1)
            
            if [ -z "$LATEST_DIR_NAME" ]; then
                echo ""
                echo "Trying to list only directories matching pattern:"
                PATTERN_OUTPUT=$(ssh -q -o LogLevel=ERROR -o BatchMode=yes -n -T -i "$SSH_KEY_PATH" -p "$SSH_PORT" "$CLUSTER_USER@$CLUSTER_HOST" "ls -1d '$REMOTE_LOGS_DIR'/optimized-client-* 2>&1" 2>&1)
                echo "$PATTERN_OUTPUT" | grep -vE '^[[:space:]]*$|Pseudo-terminal|^_' | head -10
                LATEST_DIR_NAME=$(echo "$PATTERN_OUTPUT" | grep -oE 'optimized-client-[0-9]{8}_[0-9]{6}' | head -n 1)
            fi
        else
            echo "Warning: Logs directory does not exist at $REMOTE_LOGS_DIR"
        fi
        
        if [ -z "$LATEST_DIR_NAME" ]; then
            error "No log directory found matching pattern 'optimized-client-YYYYMMDD_HHMMSS' in $REMOTE_LOGS_DIR"
        fi
    fi
    
    echo "Found latest log directory: $LATEST_DIR_NAME"
    
    # Prepare local destination
    LOCAL_LOG_DIR="$LOCAL_PROJECT_DIR/logs-from-cluster"
    mkdir -p "$LOCAL_LOG_DIR"
    
    # Transfer the directory using the full workspace path
    REMOTE_LOG_PATH="$CLUSTER_WORKSPACE/logs/$LATEST_DIR_NAME"
    echo "Transferring from: $REMOTE_LOG_PATH"
    echo "Transferring to: $LOCAL_LOG_DIR/"
    
    scp -i "$SSH_KEY_PATH" -P "$SSH_PORT" -r "$CLUSTER_USER@$CLUSTER_HOST:$REMOTE_LOG_PATH" "$LOCAL_LOG_DIR/" || error "Failed to transfer log directory"
    
    success "Log directory transferred successfully: $LOCAL_LOG_DIR/$LATEST_DIR_NAME"
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
            read -p "Tasks [data-pipeline,feature-extraction,train]: " tasks
            run_on_cluster "$tasks"
            ;;
        7) download_logs ;;
        8) tail_remote_logs ;;
        9) stop_app ;;
        10) run_client_der ;;
        11) transfer_client_logs ;;
        0) exit 0 ;;
        *) error "Invalid option" ;;
    esac
    
    echo ""
    read -p "Press Enter to continue..."
done
