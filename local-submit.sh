#!/bin/bash

# Configuration des workers (2w ou 4w)
WORKERS="${1:-4w}"
TASKS="${2:-data-pipeline,feature-extraction,train}"

if [[ "$WORKERS" != "1w" && "$WORKERS" != "2w" && "$WORKERS" != "4w" ]]; then
    echo "❌ Argument invalide. Utilisation: $0 [2w|4w] [tasks]"
    echo "   2w: 2 workers × 20G × 6 cores"
    echo "   4w: 4 workers × 10G × 3 cores (défaut)"
    echo ""
    echo "Exemples:"
    echo "  $0 2w"
    echo "  $0 4w data-pipeline,train"
    exit 1
fi

echo "--> Build Spark job..."
sbt clean package
echo "--> Spark job built."

echo "--> Submitting Spark job with config: $WORKERS"
docker exec -it spark-submit chmod +x /scripts/spark-submit.sh
docker exec -it spark-submit /scripts/spark-submit.sh "$WORKERS" "$TASKS"
echo "--> Spark job submitted."