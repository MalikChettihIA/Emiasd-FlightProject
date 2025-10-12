#!/bin/bash
# Script de gestion des configurations Docker Compose
# Permet de basculer entre configurations sans modifier les fichiers

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# Fonction d'aide
show_help() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  start [dev|prod|limited]  Démarrer les services"
    echo "  stop                      Arrêter les services"
    echo "  restart [dev|prod|limited] Redémarrer les services"
    echo "  status                    Afficher l'état des services"
    echo "  logs [service]            Afficher les logs"
    echo "  clean                     Nettoyer les containers et volumes"
    echo ""
    echo "Configurations:"
    echo "  dev      Configuration de développement (défaut)"
    echo "  prod     Configuration production (nécessite docker-compose.prod.yml)"
    echo "  limited  Configuration ressources limitées (override automatique)"
    echo ""
    echo "Exemples:"
    echo "  $0 start limited    # Démarre avec ressources limitées"
    echo "  $0 logs spark-master # Affiche les logs du master"
    echo "  $0 stop             # Arrête tous les services"
}

# Fonction pour obtenir les fichiers compose à utiliser
get_compose_files() {
    local config=$1
    case $config in
        "prod")
            if [ -f "docker-compose.prod.yml" ]; then
                echo "-f docker-compose.yml -f docker-compose.prod.yml"
            else
                echo "Erreur: docker-compose.prod.yml n'existe pas"
                exit 1
            fi
            ;;
        "limited")
            echo "-f docker-compose.yml -f docker-compose.override.yml"
            ;;
        "dev"|*)
            echo "-f docker-compose.yml"
            ;;
    esac
}

# Commandes principales
case "${1:-help}" in
    "start")
        config="${2:-dev}"
        echo "Démarrage des services en configuration: $config"
        compose_files=$(get_compose_files "$config")
        eval "docker-compose $compose_files up -d"
        echo "Services démarrés. Attendez 30 secondes que les services soient prêts..."
        sleep 30
        echo "État des services:"
        eval "docker-compose $compose_files ps"
        ;;

    "stop")
        echo "Arrêt des services..."
        docker-compose down
        ;;

    "restart")
        config="${2:-dev}"
        echo "Redémarrage des services en configuration: $config"
        $0 stop
        sleep 5
        $0 start "$config"
        ;;

    "status")
        echo "État des services:"
        docker-compose ps
        ;;

    "logs")
        service="${2:-}"
        if [ -n "$service" ]; then
            echo "Logs du service: $service"
            docker-compose logs -f "$service"
        else
            echo "Logs de tous les services:"
            docker-compose logs --tail=100
        fi
        ;;

    "clean")
        echo "Nettoyage des containers, volumes et images..."
        docker-compose down -v --rmi local
        docker system prune -f
        ;;

    "help"|*)
        show_help
        ;;
esac