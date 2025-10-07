#!/bin/bash

set -e

echo "🚀 Configuration du cluster Spark avec 4 workers"
echo "================================================="

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}ℹ️  $1${NC}"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
log_error() { echo -e "${RED}❌ $1${NC}"; }

# Détection de la commande Docker Compose
detect_docker_compose() {
    if command -v docker-compose >/dev/null 2>&1; then
        echo "docker-compose"
    elif docker compose version >/dev/null 2>&1; then
        echo "docker compose"
    else
        log_error "Ni docker-compose ni docker compose n'est disponible"
        exit 1
    fi
}

DOCKER_COMPOSE_CMD=$(detect_docker_compose)
log_info "Utilisation de: $DOCKER_COMPOSE_CMD"

# Vérification des fichiers requis
log_info "Vérification des fichiers requis..."

required_files=("docker-compose.yml" "Dockerfile.jupyter" "kernel.json")
missing_files=()

for file in "${required_files[@]}"; do
    if [[ ! -f "$file" ]]; then
        missing_files+=("$file")
    fi
done

if [[ ${#missing_files[@]} -gt 0 ]]; then
    log_error "Fichiers manquants: ${missing_files[*]}"
    log_info "Utilisez les artifacts fournis pour créer ces fichiers."
    exit 1
fi

# Validation du docker-compose.yml
log_info "Validation de la configuration Docker Compose..."
if ! $DOCKER_COMPOSE_CMD config >/dev/null 2>&1; then
    log_error "Erreur dans docker-compose.yml. Vérifiez la syntaxe."
    exit 1
fi

# Nettoyage optionnel
read -p "🧹 Voulez-vous faire un nettoyage complet ? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    log_info "Nettoyage en cours..."
    $DOCKER_COMPOSE_CMD down --volumes --remove-orphans 2>/dev/null || true
    docker system prune -f
fi

# Création des répertoires
log_info "Création des répertoires..."
mkdir -p notebooks data

# Construction et démarrage
log_info "Construction des images..."
$DOCKER_COMPOSE_CMD build --no-cache

log_info "Démarrage du cluster..."
$DOCKER_COMPOSE_CMD up -d

# Attente que les services soient prêts
log_info "Attente du démarrage des services..."
sleep 30

# Vérifications
log_info "Vérification des services..."

services=(
    "Spark Master:http://localhost:8080"
    "Jupyter Lab:http://localhost:8888"
    "Worker 1:http://localhost:8081"
    "Worker 2:http://localhost:8082"
    "Worker 3:http://localhost:8083"
    "Worker 4:http://localhost:8084"
)

for service in "${services[@]}"; do
    name=$(echo $service | cut -d: -f1)
    url=$(echo $service | cut -d: -f2-3)
    
    if curl -s -f "$url" > /dev/null 2>&1; then
        log_success "$name accessible"
    else
        log_warning "$name non accessible ($url)"
    fi
done

# Vérification des workers
workers_count=$(curl -s http://localhost:8080 | grep -o 'worker-[0-9]' | wc -l 2>/dev/null || echo "0")
log_info "Workers connectés: $workers_count/4"

# Test container Jupyter
if docker exec jupyter-spark java -version >/dev/null 2>&1; then
    log_success "Java OK dans Jupyter"
else
    log_warning "Problème avec Java dans Jupyter"
fi

# Kernels disponibles
log_info "Kernels Jupyter:"
docker exec jupyter-spark jupyter kernelspec list 2>/dev/null | grep -E "(python3|scala|toree)" || log_warning "Kernels non listés"

echo ""
log_success "Configuration terminée !"
echo ""
echo "🌐 URLs importantes:"
echo "   • Jupyter Lab:     http://localhost:8888"
echo "   • Spark Master UI: http://localhost:8080"
echo "   • Workers UI:      http://localhost:8081-8084"
echo ""
echo "🛠️  Commandes utiles:"
echo "   • $DOCKER_COMPOSE_CMD logs -f [service]  - Voir les logs"
echo "   • $DOCKER_COMPOSE_CMD restart [service] - Redémarrer un service"
echo "   • $DOCKER_COMPOSE_CMD down              - Arrêter le cluster"
echo ""