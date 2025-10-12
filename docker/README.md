# Gestion des Configurations Docker Compose

Ce projet utilise une approche modulaire pour gérer différentes configurations Docker Compose sans modifier le fichier principal `docker-compose.yml`.

## Structure des fichiers

```
docker/
├── docker-compose.yml          # Configuration de base (inchangée)
├── docker-compose.override.yml # Configuration pour ressources limitées
├── docker-compose.prod.yml     # Configuration production
└── manage.sh                   # Script de gestion des configurations
```

## Configurations disponibles

### 1. Configuration de développement (par défaut)
- Utilise `docker-compose.yml` uniquement
- Ressources standard (4G RAM par worker, 2 cores)
- Idéale pour développement local

### 2. Configuration ressources limitées
- Utilise `docker-compose.yml` + `docker-compose.override.yml`
- Ressources réduites (2G RAM par worker, 1 core)
- Optimisée pour machines avec ressources limitées

### 3. Configuration production
- Utilise `docker-compose.yml` + `docker-compose.prod.yml`
- Ressources augmentées (16G+ RAM, 4+ cores, worker supplémentaire)
- Optimisée pour traitement de gros volumes de données

## Utilisation

### Avec le script de gestion (recommandé)

```bash
# Configuration ressources limitées (recommandé pour votre poste)
./docker/manage.sh start limited

# Configuration de développement
./docker/manage.sh start dev

# Configuration production (si ressources suffisantes)
./docker/manage.sh start prod

# Vérifier l'état
./docker/manage.sh status

# Voir les logs
./docker/manage.sh logs spark-master

# Arrêter les services
./docker/manage.sh stop
```

### Utilisation directe de Docker Compose

```bash
# Ressources limitées
docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d

# Production
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# Développement (défaut)
docker-compose up -d
```

## Avantages de cette approche

1. **Fichier original préservé** : `docker-compose.yml` reste inchangé
2. **Configurations séparées** : Chaque environnement a sa propre configuration
3. **Facilité de basculement** : Changement rapide entre configurations
4. **Maintenance simplifiée** : Mises à jour du fichier de base sans impact sur les overrides
5. **Git-friendly** : Possibilité de commiter différentes configurations

## Personnalisation

### Modifier la configuration ressources limitées

Éditez `docker-compose.override.yml` pour ajuster :
- Mémoire des workers (`SPARK_WORKER_MEMORY`)
- Nombre de cores (`SPARK_WORKER_CORES`)
- Limites de ressources Docker

### Créer une nouvelle configuration

1. Créez un nouveau fichier `docker-compose.<nom>.yml`
2. Utilisez avec : `./docker/manage.sh start <nom>`

## Services disponibles

Après démarrage, les services sont accessibles sur :

- **Spark Master UI** : http://localhost:8080
- **Spark Worker 1** : http://localhost:8081
- **Spark Worker 2** : http://localhost:8082
- **MLflow UI** : http://localhost:5555
- **Jupyter Lab** : http://localhost:8888

## Dépannage

### Mémoire insuffisante
Si vous obtenez des erreurs OOM :
1. Utilisez la configuration `limited` : `./docker/manage.sh start limited`
2. Réduisez encore les ressources dans `docker-compose.override.yml`

### Ports déjà utilisés
Modifiez les ports dans les fichiers de configuration si nécessaire.

### Nettoyage
Pour nettoyer complètement :
```bash
./docker/manage.sh clean
```