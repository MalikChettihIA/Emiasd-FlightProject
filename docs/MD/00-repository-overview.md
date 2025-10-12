# Explication du Dépôt Emiasd-FlightProject et Comment l'Utiliser

Ce dépôt Git (Emiasd-FlightProject) est un projet complet et structuré pour prédire les retards de vol en utilisant Apache Spark et Scala, basé sur l'article scientifique "Using Scalable Data Mining for Predicting Flight Delays" (ACM TIST, 2016). Il implémente un pipeline de machine learning scalable pour analyser des données de vol historiques combinées à des observations météorologiques. Voici une explication détaillée de ce qu'est le dépôt, sa structure, et comment l'utiliser étape par étape.

## Qu'est-ce que ce dépôt ?
- **Objectif** : Prédire si un vol sera retardé (>15, 30, 45 ou 60 minutes) en se basant sur des données de vol (horaires, aéroports) et météo (température, visibilité, etc.). Le système utilise un modèle Random Forest entraîné avec Spark MLlib, avec réduction de dimensionnalité PCA, validation croisée et suivi d'expériences via MLflow.
- **Technologies** : Scala 2.12, Apache Spark 3.5.3, MLflow pour le tracking, Docker pour la conteneurisation.
- **Performances cibles** : Précision ~85-87%, rappel ~86-89%, entraînement rapide (<5 min sur cluster).
- **Échelle** : Conçu pour traiter des millions de vols, d'abord localement puis sur cluster.
- **Inspiration** : Reproduit la méthodologie de l'article, avec focus sur la préparation des données (jointures complexes) et la scalabilité.

Le projet est éducatif/recherche, avec une documentation complète et des scripts pour faciliter l'usage.

## Structure du dépôt
Le dépôt suit une architecture modulaire Scala/Spark standard :

- **`build.sbt`** / **`plugins.sbt`** : Configuration SBT pour les dépendances (Spark, MLflow, etc.).
- **`src/main/scala/com/flightdelay/`** : Code source Scala organisé en packages :
  - `app/` : Point d'entrée principal (Main).
  - `config/` : Classes pour charger la config YAML.
  - `data/` : Chargement et préprocessing des données (loaders pour CSV, preprocessing pour jointures/équilibrage).
  - `features/` : Ingénierie des features (pipelines, PCA pour réduction dimensionnelle).
  - `ml/` : Modèles (Random Forest), entraînement, évaluation, tracking MLflow.
  - `utils/` : Utilitaires (logging, etc.).
- **`src/main/resources/`** : Fichiers de config YAML (ex. : `local-config.yml` pour définir les expériences).
- **`docker/`** : Infrastructure conteneurisée :
  - `docker-compose.yml` : Définition des services (Spark master/workers, MLflow, Jupyter).
  - Scripts : `setup.sh`, `start.sh`, `submit.sh`, etc., pour gérer le cluster.
- **`docs/`** : Documentation Markdown détaillée (guides pour installation, architecture, pipeline, etc.).
- **`work/`** : Répertoire de travail :
  - `data/` : Données d'entrée (CSV de vols/météo à placer ici).
  - `output/` : Résultats (modèles, métriques).
  - `scripts/` : Scripts Python pour visualisations (ROC, PCA, etc.).
  - `apps/` : JARs compilés.
- **`run-on-local.sh`** / **`start-local-cluster.sh`** / etc. : Scripts pour exécution locale ou cluster.
- **`README.md`** : Guide principal avec aperçu, installation et exemples.

Le projet est prêt à l'emploi avec Docker pour éviter les conflits de dépendances.

## Comment l'utiliser ?
Le dépôt est conçu pour être simple à utiliser via Docker. Voici les étapes pour commencer (assurez-vous d'avoir Docker et Docker Compose installés, avec 16GB+ RAM et 20GB+ espace disque).

### 1. Prérequis et préparation
- Clonez le dépôt (déjà fait, vous êtes dedans).
- Téléchargez les datasets CSV depuis le lien Dropbox (mentionné dans indications.pdf) et placez-les dans `work/data/` (ex. : flights.csv, weather.csv).
- Vérifiez la config : Éditez `src/main/resources/local-config.yml` pour définir vos expériences (ex. : seuil de retard, nombre d'arbres Random Forest).

### 2. Configuration Docker
- Allez dans `docker/` :
  ```
  cd docker
  ./setup.sh  # Lance le setup interactif (choisissez "oui" pour nettoyer si nécessaire)
  ```
  Cela démarre un cluster Spark (1 master + 4 workers), MLflow (pour tracking), et Jupyter (optionnel).

### 3. Lancer une expérience
- Depuis `docker/` :
  ```
  ./submit.sh  # Soumet le job Spark pour entraîner le modèle
  ```
  - Le système charge les données, fait le preprocessing (jointures, PCA), entraîne Random Forest avec CV, et logge tout dans MLflow.
  - Temps typique : ~5 min pour un petit dataset.
- Interfaces :
  - Spark UI : http://localhost:8080 (monitorez les jobs).
  - MLflow UI : http://localhost:5555 (visualisez les expériences, métriques, modèles).

### 4. Analyser les résultats
- Résultats dans `work/output/` : Métriques (accuracy, recall), modèles sauvegardés, analyses PCA.
- Visualisations : Lancez les scripts Python (ex. : `python work/scripts/visualize_metrics.py /output/exp_name` pour graphiques ROC/confusion).
- Exemple de sortie : Précision 87%, rappel 89%, avec courbes ROC.

### 5. Développement et personnalisation
- Pour modifier le code : Éditez dans `src/main/scala/`, compilez avec `sbt compile`, puis `sbt package` pour générer le JAR.
- Ajouter un modèle : Suivez le guide `docs/MD/10-adding-models.md` (ex. : implémentez GBT au lieu de Random Forest).
- Tests locaux : Utilisez `run-on-local.sh` pour tester sans Docker.
- Cluster réel : Utilisez `start-local-cluster.sh` ou déployez sur cloud (ex. : AWS EMR).

### 6. Arrêt et nettoyage
- `cd docker && ./stop.sh` pour arrêter les services.
- `./cleanup.sh` pour supprimer les conteneurs/volumes.

Si vous rencontrez des erreurs (ex. : manque de RAM), consultez les guides dans `docs/MD/` (ex. : 02-installation.md pour troubleshooting). Le projet est reproductible et bien documenté pour faciliter l'apprentissage. Si vous voulez lancer une expérience spécifique ou déboguer, dites-moi !