#!/bin/bash

###############################################################################
# Script de validation rapide des parquets
#
# Usage:
#   ./scripts/validate_parquets.sh Experience-local-D-60-7-7
#
# Description:
#   Valide que les parquets générés contiennent correctement:
#   - Flags de missing par heure (_missing_h1 à _missing_h7)
#   - Compteurs de missing pour features agrégées (_missing_count)
#   - Valeurs sentinelles (-999, "MISSING")
#   - Aucune valeur NULL restante
###############################################################################

set -e

# Couleurs pour les logs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
EXPERIMENT_NAME=${1:-"Experience-local-D-60-7-7"}
BASE_PATH="/output"
TRAIN_PATH="$BASE_PATH/$EXPERIMENT_NAME/data/join_exploded_train_prepared.parquet"
TEST_PATH="$BASE_PATH/$EXPERIMENT_NAME/data/join_exploded_test_prepared.parquet"

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           VALIDATION DES PARQUETS - GESTION DES VALEURS MANQUANTES          ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

echo -e "${YELLOW}📦 Experiment: ${EXPERIMENT_NAME}${NC}"
echo -e "${YELLOW}📁 Base path:  ${BASE_PATH}${NC}"
echo ""

# Vérifier l'existence des fichiers
echo -e "${BLUE}🔍 Vérification de l'existence des fichiers...${NC}"

if [ ! -d "$TRAIN_PATH" ]; then
    echo -e "${RED}❌ ERREUR: Train parquet non trouvé: $TRAIN_PATH${NC}"
    exit 1
fi

if [ ! -d "$TEST_PATH" ]; then
    echo -e "${RED}❌ ERREUR: Test parquet non trouvé: $TEST_PATH${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Train parquet trouvé: $TRAIN_PATH${NC}"
echo -e "${GREEN}✅ Test parquet trouvé:  $TEST_PATH${NC}"
echo ""

# Lancer la validation avec Spark
echo -e "${BLUE}🚀 Lancement de la validation avec Spark...${NC}"
echo ""

# Option 1: Utiliser spark-shell avec le validateur
cat << 'EOF' > /tmp/validate_parquets_temp.scala
import com.flightdelay.features.quality.ParquetMissingValuesValidator

val experimentName = sys.env.getOrElse("EXPERIMENT_NAME", "Experience-local-D-60-7-7")
val basePath = "/output"
val trainPath = s"$basePath/$experimentName/data/join_exploded_train_prepared.parquet"
val testPath = s"$basePath/$experimentName/data/join_exploded_test_prepared.parquet"

ParquetMissingValuesValidator.generateReport(trainPath, testPath)

System.exit(0)
EOF

# Lancer avec spark-shell
EXPERIMENT_NAME=$EXPERIMENT_NAME spark-shell \
  --master local[*] \
  --driver-memory 4g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.ui.showConsoleProgress=false \
  -i /tmp/validate_parquets_temp.scala

# Nettoyer le fichier temporaire
rm -f /tmp/validate_parquets_temp.scala

echo ""
echo -e "${GREEN}✅ Validation terminée avec succès !${NC}"
echo ""
