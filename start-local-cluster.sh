#!/bin/bash
echo "🛑 Démarrage du cluster Spark..."
docker-compose -f ./docker/docker-compose.yml up -d
echo "✅ Cluster démarré"