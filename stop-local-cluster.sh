#!/bin/bash
echo "🛑 Arrêt du cluster Spark..."
docker-compose -f ./docker/docker-compose.yml down
echo "✅ Cluster arrêté"

