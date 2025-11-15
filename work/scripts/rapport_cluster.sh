#!/bin/bash

echo "========================================="
echo "INSPECTION DU CLUSTER HADOOP"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="

# ============================================================================
# 1. INFORMATIONS SYSTÈME
# ============================================================================
echo -e "\n1. VERSIONS ET CONFIGURATION"
echo "----------------------------------------"
echo "Version Hadoop:"
hadoop version 2>/dev/null | head -n 1

echo -e "\nVersion Java:"
java -version 2>&1 | head -n 1

if command -v spark-submit &> /dev/null; then
    echo -e "\nVersion Spark:"
    spark-submit --version 2>&1 | grep "version" | head -n 1
fi

echo -e "\nResource Manager:"
yarn node -list 2>&1 | grep "Connecting to ResourceManager" | head -n 1

# ============================================================================
# 2. NŒUDS YARN - STATISTIQUES GLOBALES
# ============================================================================
echo -e "\n========================================="
echo "2. NŒUDS YARN - STATISTIQUES GLOBALES"
echo "========================================="

TOTAL_NODES=$(yarn node -list -all 2>&1 | grep -v "INFO" | tail -n +4 | wc -l)
RUNNING_NODES=$(yarn node -list 2>&1 | grep "RUNNING" | wc -l)
LOST_NODES=$(yarn node -list -all 2>&1 | grep "LOST" | wc -l)
UNHEALTHY_NODES=$(yarn node -list -all 2>&1 | grep "UNHEALTHY" | wc -l)

echo "Total des nœuds: ${TOTAL_NODES}"
echo "Nœuds actifs (RUNNING): ${RUNNING_NODES}"
echo "Nœuds perdus (LOST): ${LOST_NODES}"
echo "Nœuds malsains (UNHEALTHY): ${UNHEALTHY_NODES}"

# ============================================================================
# 3. RESSOURCES TOTALES DU CLUSTER
# ============================================================================
echo -e "\n========================================="
echo "3. AGRÉGATION DES RESSOURCES"
echo "========================================="

# Extraire uniquement les Node-Ids des nœuds RUNNING
NODE_IDS_LIST=$(yarn node -list 2>&1 | grep "RUNNING" | awk '{print $1}')

echo "Nombre de nœuds à analyser: $(echo "$NODE_IDS_LIST" | wc -w)"

TOTAL_MEMORY=0
TOTAL_VCORES=0
TOTAL_MEMORY_USED=0
TOTAL_VCORES_USED=0
TOTAL_CONTAINERS=0

NODE_COUNT=0
declare -a MEMORY_ARRAY
declare -a VCORES_ARRAY

echo "Collecte des métriques en cours..."

# Boucler sur chaque Node-Id
for NODE_ID in $NODE_IDS_LIST; do
    echo "  Interrogation de ${NODE_ID}..."
    NODE_DETAILS=$(yarn node -status "$NODE_ID" 2>/dev/null)
    
    MEMORY=$(echo "$NODE_DETAILS" | grep "Memory-Capacity" | sed 's/.*: \([0-9]*\)MB/\1/')
    VCORES=$(echo "$NODE_DETAILS" | grep "CPU-Capacity" | sed 's/.*: \([0-9]*\) vcores/\1/')
    MEMORY_USED=$(echo "$NODE_DETAILS" | grep "Memory-Used" | sed 's/.*: \([0-9]*\)MB/\1/')
    VCORES_USED=$(echo "$NODE_DETAILS" | grep "CPU-Used" | sed 's/.*: \([0-9]*\) vcores/\1/')
    CONTAINERS=$(echo "$NODE_DETAILS" | grep "Containers" | sed 's/.*: \([0-9]*\)/\1/')
    
    # Initialiser à 0 si vide
    [ -z "$MEMORY" ] && MEMORY=0
    [ -z "$VCORES" ] && VCORES=0
    [ -z "$MEMORY_USED" ] && MEMORY_USED=0
    [ -z "$VCORES_USED" ] && VCORES_USED=0
    [ -z "$CONTAINERS" ] && CONTAINERS=0
    
    if [ "$MEMORY" -gt 0 ] && [ "$VCORES" -gt 0 ]; then
        TOTAL_MEMORY=$((TOTAL_MEMORY + MEMORY))
        TOTAL_VCORES=$((TOTAL_VCORES + VCORES))
        TOTAL_MEMORY_USED=$((TOTAL_MEMORY_USED + MEMORY_USED))
        TOTAL_VCORES_USED=$((TOTAL_VCORES_USED + VCORES_USED))
        TOTAL_CONTAINERS=$((TOTAL_CONTAINERS + CONTAINERS))
        
        MEMORY_ARRAY[$NODE_COUNT]=$MEMORY
        VCORES_ARRAY[$NODE_COUNT]=$VCORES
        NODE_COUNT=$((NODE_COUNT + 1))
    fi
done

echo "Métriques collectées pour ${NODE_COUNT} nœuds."
echo ""

TOTAL_MEMORY_GB=$((TOTAL_MEMORY / 1024))
TOTAL_MEMORY_USED_GB=$((TOTAL_MEMORY_USED / 1024))

echo "Mémoire totale: ${TOTAL_MEMORY_GB} GB (${TOTAL_MEMORY} MB)"
echo "Mémoire utilisée: ${TOTAL_MEMORY_USED_GB} GB (${TOTAL_MEMORY_USED} MB)"
if [ "$TOTAL_MEMORY" -gt 0 ] 2>/dev/null; then
    MEM_USAGE_PERCENT=$((TOTAL_MEMORY_USED * 100 / TOTAL_MEMORY))
    echo "Taux d'utilisation mémoire: ${MEM_USAGE_PERCENT}%"
fi

echo -e "\nVCores totaux: ${TOTAL_VCORES}"
echo "VCores utilisés: ${TOTAL_VCORES_USED}"
if [ "$TOTAL_VCORES" -gt 0 ] 2>/dev/null; then
    CPU_USAGE_PERCENT=$((TOTAL_VCORES_USED * 100 / TOTAL_VCORES))
    echo "Taux d'utilisation CPU: ${CPU_USAGE_PERCENT}%"
fi

echo -e "\nConteneurs actifs totaux: ${TOTAL_CONTAINERS}"

# Statistiques de distribution
if [ $NODE_COUNT -gt 0 ]; then
    echo -e "\n--- Distribution des ressources ---"
    
    # Tri des arrays pour calculer min/max/médiane
    IFS=$'\n' MEMORY_SORTED=($(sort -n <<<"${MEMORY_ARRAY[*]}"))
    IFS=$'\n' VCORES_SORTED=($(sort -n <<<"${VCORES_ARRAY[*]}"))
    unset IFS
    
    MEMORY_MIN=${MEMORY_SORTED[0]}
    MEMORY_MAX=${MEMORY_SORTED[$((NODE_COUNT-1))]}
    VCORES_MIN=${VCORES_SORTED[0]}
    VCORES_MAX=${VCORES_SORTED[$((NODE_COUNT-1))]}
    
    MEMORY_AVG=$((TOTAL_MEMORY / NODE_COUNT))
    VCORES_AVG=$((TOTAL_VCORES / NODE_COUNT))
    
    echo "Mémoire par nœud: Min=${MEMORY_MIN}MB, Max=${MEMORY_MAX}MB, Moyenne=${MEMORY_AVG}MB"
    echo "VCores par nœud: Min=${VCORES_MIN}, Max=${VCORES_MAX}, Moyenne=${VCORES_AVG}"
fi

# ============================================================================
# 4. CLASSIFICATION DES NŒUDS
# ============================================================================
echo -e "\n========================================="
echo "4. CLASSIFICATION DES NŒUDS"
echo "========================================="

HIGH_PERF_COUNT=0
MEDIUM_PERF_COUNT=0
LOW_PERF_COUNT=0

HIGH_PERF_NODES=""
MEDIUM_PERF_NODES=""
LOW_PERF_NODES=""

# Réutiliser la même liste de nœuds
for NODE_ID in $NODE_IDS_LIST; do
    NODE_DETAILS=$(yarn node -status "$NODE_ID" 2>/dev/null)
    
    VCORES=$(echo "$NODE_DETAILS" | grep "CPU-Capacity" | awk '{print $3}' | sed 's/vcores//')
    MEMORY=$(echo "$NODE_DETAILS" | grep "Memory-Capacity" | awk '{print $3}' | sed 's/MB//')
    
    if [ ! -z "$VCORES" ] && [ ! -z "$MEMORY" ]; then
        if [ $VCORES -ge 16 ]; then
            HIGH_PERF_COUNT=$((HIGH_PERF_COUNT + 1))
            HIGH_PERF_NODES="${HIGH_PERF_NODES}  ${NODE_ID} (${VCORES} vcores, ${MEMORY}MB)\n"
        elif [ $VCORES -ge 8 ]; then
            MEDIUM_PERF_COUNT=$((MEDIUM_PERF_COUNT + 1))
            MEDIUM_PERF_NODES="${MEDIUM_PERF_NODES}  ${NODE_ID} (${VCORES} vcores, ${MEMORY}MB)\n"
        else
            LOW_PERF_COUNT=$((LOW_PERF_COUNT + 1))
            LOW_PERF_NODES="${LOW_PERF_NODES}  ${NODE_ID} (${VCORES} vcores, ${MEMORY}MB)\n"
        fi
    fi
done

echo "Haute performance (≥16 vcores): ${HIGH_PERF_COUNT} nœuds"
if [ ! -z "$HIGH_PERF_NODES" ]; then
    echo -e "$HIGH_PERF_NODES"
fi

echo "Performance moyenne (8-15 vcores): ${MEDIUM_PERF_COUNT} nœuds"
if [ ! -z "$MEDIUM_PERF_NODES" ]; then
    echo -e "$MEDIUM_PERF_NODES"
fi

echo "Basse performance (<8 vcores): ${LOW_PERF_COUNT} nœuds"
if [ ! -z "$LOW_PERF_NODES" ]; then
    echo -e "$LOW_PERF_NODES"
fi

# ============================================================================
# 5. DÉTAIL DES NŒUDS ACTIFS
# ============================================================================
echo -e "\n========================================="
echo "5. DÉTAIL DES RESSOURCES YARN PAR NŒUD"
echo "========================================="

NODE_IDS=$(yarn node -list -all 2>&1 | \
           grep -v "INFO" | \
           tail -n +4 | \
           awk '{print $1}')

if [ -z "$NODE_IDS" ]; then
    echo "⚠️ Aucun Node-Id YARN trouvé."
else
    for NODE_ID in $NODE_IDS; do
        echo -e "\n--- Rapport détaillé pour : **${NODE_ID}** ---"
        yarn node -status $NODE_ID 2>/dev/null | grep -v "INFO client"
        echo -e "-----------------------------------------"
    done
fi

# ============================================================================
# 6. HDFS - CAPACITÉ ET UTILISATION
# ============================================================================
echo -e "\n========================================="
echo "6. SYSTÈME DE FICHIERS HDFS"
echo "========================================="

echo "--- Capacité globale ---"
hdfs dfsadmin -report 2>/dev/null | grep -A 5 "Configured Capacity"

echo -e "\n--- Informations Datanodes ---"
DATANODE_COUNT=$(hdfs dfsadmin -report 2>/dev/null | grep -c "^Name:")
echo "Nombre de datanodes: ${DATANODE_COUNT}"

# Extraction des informations sur les datanodes (si accessible)
echo -e "\n--- Répartition du stockage ---"
hdfs dfsadmin -report 2>/dev/null | grep -A 1 "Name:\|DFS Used:\|DFS Remaining:" | head -n 30

# ============================================================================
# 7. ÉTAT DU RÉSEAU HDFS
# ============================================================================
echo -e "\n========================================="
echo "7. SANTÉ DU CLUSTER HDFS"
echo "========================================="

echo "--- État des réplications ---"
hdfs fsck / 2>/dev/null | grep -E "Total size:|Total blocks:|Minimally replicated blocks:|Under-replicated blocks:|Corrupt blocks:|Missing blocks:"

# ============================================================================
# 8. APPLICATIONS YARN
# ============================================================================
echo -e "\n========================================="
echo "8. APPLICATIONS YARN"
echo "========================================="

echo "--- Applications en cours ---"
RUNNING_APPS=$(yarn application -list -appStates RUNNING 2>/dev/null | grep "application_" | wc -l)
echo "Nombre d'applications en cours: ${RUNNING_APPS}"

if [ $RUNNING_APPS -gt 0 ]; then
    yarn application -list -appStates RUNNING 2>/dev/null | grep "application_"
fi

echo -e "\n--- Applications acceptées (en attente) ---"
ACCEPTED_APPS=$(yarn application -list -appStates ACCEPTED 2>/dev/null | grep "application_" | wc -l)
echo "Nombre d'applications en attente: ${ACCEPTED_APPS}"

echo -e "\n--- Historique récent (10 dernières) ---"
yarn application -list -appStates FINISHED,FAILED,KILLED 2>/dev/null | grep "application_" | tail -n 10

# ============================================================================
# 9. FILES D'ATTENTE YARN
# ============================================================================
echo -e "\n========================================="
echo "9. CONFIGURATION DES FILES D'ATTENTE"
echo "========================================="

yarn queue -status default 2>/dev/null

# ============================================================================
# 10. TOPOLOGIE DU CLUSTER
# ============================================================================
echo -e "\n========================================="
echo "10. TOPOLOGIE DU CLUSTER"
echo "========================================="

echo "--- Racks configurés ---"
yarn node -list 2>/dev/null | grep "RUNNING" | awk '{print $2}' | sort | uniq -c

# ============================================================================
# 11. MÉTRIQUES DU RESOURCE MANAGER
# ============================================================================
echo -e "\n========================================="
echo "11. MÉTRIQUES DU RESOURCE MANAGER"
echo "========================================="

# Tentative d'accès aux métriques via l'API REST (si disponible)
RM_HOST=$(yarn node -list 2>&1 | grep "Connecting to ResourceManager" | sed 's/.*at //;s/:.*//')
if [ ! -z "$RM_HOST" ]; then
    echo "Resource Manager: ${RM_HOST}"
    
    # Test de connectivité à l'interface web
    if command -v curl &> /dev/null; then
        echo -e "\n--- Statistiques du cluster (API REST) ---"
        curl -s "http://${RM_HOST}:8088/ws/v1/cluster/metrics" 2>/dev/null | \
            python3 -m json.tool 2>/dev/null | \
            grep -E "totalMB|allocatedMB|availableMB|totalVirtualCores|allocatedVirtualCores|availableVirtualCores|containersAllocated|appsRunning|appsPending"
    fi
fi

# ============================================================================
# 12. INFORMATIONS DE RÉPLICATION HDFS
# ============================================================================
echo -e "\n========================================="
echo "12. CONFIGURATION HDFS"
echo "========================================="

echo "--- Facteur de réplication ---"
hdfs getconf -confKey dfs.replication 2>/dev/null

echo -e "\n--- Taille de bloc HDFS ---"
hdfs getconf -confKey dfs.blocksize 2>/dev/null | awk '{print $1/1024/1024 " MB"}'

echo -e "\n--- Répertoire NameNode ---"
hdfs getconf -confKey dfs.namenode.name.dir 2>/dev/null

# ============================================================================
# 13. RÉSUMÉ EXÉCUTIF
# ============================================================================
echo -e "\n========================================="
echo "13. RÉSUMÉ EXÉCUTIF DU CLUSTER"
echo "========================================="

echo "Nœuds de calcul:"
echo "  • Total: ${RUNNING_NODES} nœuds actifs"
echo "  • Haute performance: ${HIGH_PERF_COUNT}"
echo "  • Performance moyenne: ${MEDIUM_PERF_COUNT}"
echo "  • Basse performance: ${LOW_PERF_COUNT}"

echo -e "\nRessources totales:"
echo "  • Mémoire: ${TOTAL_MEMORY_GB} GB"
echo "  • VCores: ${TOTAL_VCORES}"
echo "  • Conteneurs actifs: ${TOTAL_CONTAINERS}"

echo -e "\nUtilisation actuelle:"
if [ $TOTAL_MEMORY -gt 0 ]; then
    echo "  • Mémoire: ${MEM_USAGE_PERCENT}% (${TOTAL_MEMORY_USED_GB}/${TOTAL_MEMORY_GB} GB)"
fi
if [ $TOTAL_VCORES -gt 0 ]; then
    echo "  • CPU: ${CPU_USAGE_PERCENT}% (${TOTAL_VCORES_USED}/${TOTAL_VCORES} vcores)"
fi

echo -e "\nApplications:"
echo "  • En cours: ${RUNNING_APPS}"
echo "  • En attente: ${ACCEPTED_APPS}"

echo -e "\nStockage HDFS:"
hdfs dfsadmin -report 2>/dev/null | grep "DFS Used%\|DFS Remaining"

echo -e "\n========================================="
echo "Inspection terminée: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="
