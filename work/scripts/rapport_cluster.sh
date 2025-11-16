#!/bin/bash
set -euo pipefail

# =============================================================================
# Script: rapport_cluster.sh
# Objet : Inspection détaillée du cluster Hadoop/Spark/YARN + HDFS
# Améliorations :
#   - Parsing d'options (JSON, fichier sortie, limite des nœuds, quiet)
#   - Refactorisation par fonctions
#   - Réduction appels redondants
#   - Sortie JSON synthétique (--json)
#   - Recommandations basic
# =============================================================================

VERSION="2.0"
DATE_RUN=$(date '+%Y-%m-%d %H:%M:%S')

OUTPUT_FILE=""
JSON_MODE=0
QUIET=0
LIMIT_NODES=""

usage() {
    cat <<EOF
Rapport Cluster Hadoop/Spark v${VERSION}
Usage: $0 [options]

Options:
    -h, --help           Affiche cette aide
    -j, --json           Imprime uniquement le résumé exécutif en JSON
    -o, --output <f>     Écrit le JSON (si -j) ou le rapport texte dans <f>
    -q, --quiet          Réduit la verbosité (masque détails par nœud)
    -n, --limit-nodes N  Limite l'inspection détaillée à N nœuds RUNNING

Exemples:
    $0                    Rapport complet texte
    $0 -j                 Résumé JSON seul
    $0 -j -o rapport.json Résumé JSON vers fichier
    $0 -q -n 5            Rapport texte mais nœuds détaillés (max 5)
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage; exit 0;;
        -j|--json) JSON_MODE=1; shift;;
        -o|--output) OUTPUT_FILE="${2:-}"; shift 2;;
        -q|--quiet) QUIET=1; shift;;
        -n|--limit-nodes) LIMIT_NODES="${2:-}"; shift 2;;
        *) echo "Option inconnue: $1"; usage; exit 1;;
    esac
done

log() {
    if [ $QUIET -eq 0 ]; then
        echo "$@"
    fi
}

header() {
    echo -e "\n========================================="
    echo "$1"
    echo "========================================="
}

check_prereqs() {
    local missing=()
    for cmd in hadoop yarn hdfs; do
        command -v $cmd >/dev/null 2>&1 || missing+=("$cmd")
    done
    if [ ${#missing[@]} -gt 0 ]; then
        echo "⚠️ Outils manquants: ${missing[*]}" >&2
    fi
}

echo "========================================="
echo "INSPECTION DU CLUSTER HADOOP (v${VERSION})"
echo "Date: ${DATE_RUN}"
echo "========================================="

check_prereqs

# ============================================================================
# 1. INFORMATIONS SYSTÈME
# ============================================================================
echo -e "\n1. VERSIONS ET CONFIGURATION"
echo "----------------------------------------"
echo "Version Hadoop:"; hadoop version 2>/dev/null | head -n 1 || true
echo -e "\nVersion Java:"; java -version 2>&1 | head -n 1 || true

if command -v spark-submit &> /dev/null; then
    echo -e "\nVersion Spark:"; spark-submit --version 2>&1 | grep "version" | head -n 1 || true
fi

RESOURCE_MANAGER_LINE=$(yarn node -list 2>&1 | grep "Connecting to ResourceManager" | head -n 1 || true)
echo -e "\nResource Manager:"; echo "$RESOURCE_MANAGER_LINE"

# ============================================================================
# 2. NŒUDS YARN - STATISTIQUES GLOBALES
# ============================================================================
echo -e "\n========================================="
echo "2. NŒUDS YARN - STATISTIQUES GLOBALES"
echo "========================================="

YARN_LIST_ALL=$(yarn node -list -all 2>&1 || true)
YARN_LIST_RUNNING=$(yarn node -list 2>&1 || true)

TOTAL_NODES=$(echo "$YARN_LIST_ALL" | grep -v "INFO" | tail -n +4 | wc -l | tr -d ' ')
RUNNING_NODES=$(echo "$YARN_LIST_RUNNING" | grep "RUNNING" | wc -l | tr -d ' ')
LOST_NODES=$(echo "$YARN_LIST_ALL" | grep -c "LOST" || true)
UNHEALTHY_NODES=$(echo "$YARN_LIST_ALL" | grep -c "UNHEALTHY" || true)

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
NODE_IDS_LIST=$(echo "$YARN_LIST_RUNNING" | grep "RUNNING" | awk '{print $1}')
if [ -n "$LIMIT_NODES" ]; then
    NODE_IDS_LIST=$(echo "$NODE_IDS_LIST" | head -n "$LIMIT_NODES")
fi

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
declare -A NODE_JSON
for NODE_ID in $NODE_IDS_LIST; do
    log "  Interrogation de ${NODE_ID}..."
    NODE_DETAILS=$(yarn node -status "$NODE_ID" 2>/dev/null || true)
    MEMORY=$(echo "$NODE_DETAILS" | grep "Memory-Capacity" | sed 's/.*: \([0-9]*\)MB/\1/')
    VCORES=$(echo "$NODE_DETAILS" | grep "CPU-Capacity" | sed 's/.*: \([0-9]*\) vcores/\1/')
    MEMORY_USED=$(echo "$NODE_DETAILS" | grep "Memory-Used" | sed 's/.*: \([0-9]*\)MB/\1/')
    VCORES_USED=$(echo "$NODE_DETAILS" | grep "CPU-Used" | sed 's/.*: \([0-9]*\) vcores/\1/')
    CONTAINERS=$(echo "$NODE_DETAILS" | grep "Containers" | sed 's/.*: \([0-9]*\)/\1/')

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
        NODE_JSON[$NODE_ID]="{\"memoryMB\":$MEMORY,\"memoryUsedMB\":$MEMORY_USED,\"vcores\":$VCORES,\"vcoresUsed\":$VCORES_USED,\"containers\":$CONTAINERS}"
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
    entry=${NODE_JSON[$NODE_ID]:-}
    if [ -n "$entry" ]; then
        VCORES=$(echo "$entry" | sed 's/.*\"vcores\":\([0-9]*\).*/\1/')
        MEMORY=$(echo "$entry" | sed 's/.*\"memoryMB\":\([0-9]*\).*/\1/')
        if [ "$VCORES" -ge 16 ]; then
            HIGH_PERF_COUNT=$((HIGH_PERF_COUNT + 1))
            HIGH_PERF_NODES+="  ${NODE_ID} (${VCORES} vcores, ${MEMORY}MB)\n"
        elif [ "$VCORES" -ge 8 ]; then
            MEDIUM_PERF_COUNT=$((MEDIUM_PERF_COUNT + 1))
            MEDIUM_PERF_NODES+="  ${NODE_ID} (${VCORES} vcores, ${MEMORY}MB)\n"
        else
            LOW_PERF_COUNT=$((LOW_PERF_COUNT + 1))
            LOW_PERF_NODES+="  ${NODE_ID} (${VCORES} vcores, ${MEMORY}MB)\n"
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

NODE_IDS=$(echo "$YARN_LIST_ALL" | grep -v "INFO" | tail -n +4 | awk '{print $1}')

if [ $QUIET -eq 0 ]; then
    if [ -z "$NODE_IDS" ]; then
        echo "⚠️ Aucun Node-Id YARN trouvé."
    else
        for NODE_ID in $NODE_IDS; do
            echo -e "\n--- Rapport détaillé pour : **${NODE_ID}** ---"
            yarn node -status "$NODE_ID" 2>/dev/null | grep -v "INFO client" || true
            echo -e "-----------------------------------------"
        done
    fi
else
    echo "Mode quiet: détails des nœuds ignorés."
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
RM_HOST=$(echo "$RESOURCE_MANAGER_LINE" | sed 's/.*at //;s/:.*//')
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

RECO=""
if [ $TOTAL_MEMORY -gt 0 ] && [ $TOTAL_MEMORY_USED -gt 0 ]; then
    if [ $MEM_USAGE_PERCENT -gt 80 ]; then
        RECO+="Mémoire élevée (>80%). Envisager ajout nœuds ou optimisation jobs.\n"
    fi
fi
if [ $TOTAL_VCORES -gt 0 ] && [ $TOTAL_VCORES_USED -gt 0 ]; then
    if [ $CPU_USAGE_PERCENT -gt 85 ]; then
        RECO+="CPU saturé (>85%). Revoir tailles d'exécuteurs ou capacity scheduler.\n"
    fi
fi
if [ -n "$RECO" ]; then
    echo -e "\nRecommandations:"; echo -e "${RECO}" | sed '/^$/d'
fi

echo -e "\n========================================="
echo "Inspection terminée: ${DATE_RUN}"
echo "========================================="

if [ $JSON_MODE -eq 1 ]; then
    SUMMARY_JSON="$(
        echo "{"
        echo "  \"timestamp\": \"${DATE_RUN}\"," 
        echo "  \"nodes\": {"
        echo "    \"total\": ${TOTAL_NODES},"
        echo "    \"running\": ${RUNNING_NODES},"
        echo "    \"lost\": ${LOST_NODES},"
        echo "    \"unhealthy\": ${UNHEALTHY_NODES}"
        echo "  },"
        echo "  \"resources\": {"
        echo "    \"memoryTotalMB\": ${TOTAL_MEMORY},"
        echo "    \"memoryUsedMB\": ${TOTAL_MEMORY_USED},"
        echo "    \"vcoresTotal\": ${TOTAL_VCORES},"
        echo "    \"vcoresUsed\": ${TOTAL_VCORES_USED},"
        echo "    \"containersActive\": ${TOTAL_CONTAINERS}"
        echo "  },"
        echo "  \"classification\": {"
        echo "    \"highPerf\": ${HIGH_PERF_COUNT},"
        echo "    \"mediumPerf\": ${MEDIUM_PERF_COUNT},"
        echo "    \"lowPerf\": ${LOW_PERF_COUNT}"
        echo "  },"
        echo "  \"utilisation\": {"
        if [ $TOTAL_MEMORY -gt 0 ]; then
            echo "    \"memoryUsagePct\": ${MEM_USAGE_PERCENT},"
        else
            echo "    \"memoryUsagePct\": null,"
        fi
        if [ $TOTAL_VCORES -gt 0 ]; then
            echo "    \"cpuUsagePct\": ${CPU_USAGE_PERCENT}"
        else
            echo "    \"cpuUsagePct\": null"
        fi
        echo "  },"
        echo "  \"resourceManagerHost\": \"${RM_HOST:-unknown}\","
        echo "  \"recommandations\": \"$(echo "$RECO" | tr '"' "'" | tr '\n' ' ')\"" 
        echo "}" 
    )"
    if [ -n "$OUTPUT_FILE" ]; then
        echo "$SUMMARY_JSON" > "$OUTPUT_FILE"
        echo "JSON écrit dans $OUTPUT_FILE"
    else
        echo "$SUMMARY_JSON"
    fi
fi
