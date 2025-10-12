#!/bin/bash

sbt package
echo "🛑 Démarrage du cluster Spark..."

# Détection de la commande Docker Compose
if command -v docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD="docker-compose"
elif docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE_CMD="docker compose"
else
    echo "❌ Ni docker-compose ni docker compose n'est disponible"
    exit 1
fi

$DOCKER_COMPOSE_CMD -f ./docker/docker-compose.yml up -d
echo "✅ Cluster démarré"