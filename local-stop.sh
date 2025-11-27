#!/bin/bash

# Configuration des workers (2w ou 4w)
WORKERS="${1:-4w}"

if [[ "$WORKERS" != "2w" && "$WORKERS" != "4w" ]]; then
    echo "❌ Argument invalide. Utilisation: $0 [2w|4w]"
    echo "   2w: 2 workers × 20G × 6 cores"
    echo "   4w: 4 workers × 10G × 3 cores (défaut)"
    exit 1
fi

echo "🛑 Arrêt du cluster Spark ($WORKERS)..."

# Détection de la commande Docker Compose
if command -v docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD="docker-compose"
elif docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD="docker compose"
else
    echo "❌ Ni docker-compose ni docker compose n'est disponible"
    exit 1
fi

$DOCKER_COMPOSE_CMD -f ./docker/docker-compose-${WORKERS}.yml down
echo "✅ Cluster arrêté ($WORKERS)"
