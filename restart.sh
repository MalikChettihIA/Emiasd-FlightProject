#!/bin/bash
sbt package
echo "🛑 Arrêt du cluster Spark..."
docker-compose -f ./docker/docker-compose.yml down
echo "🛑 Démarrage du cluster Spark..."
docker-compose -f ./docker/docker-compose.yml up -d
echo "✅ Cluster redémarré"