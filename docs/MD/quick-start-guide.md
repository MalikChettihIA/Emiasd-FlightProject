# 🚀 Guide de Démarrage Rapide - Flight Delay Prediction

Ce guide vous permet de lancer votre premier modèle de prédiction de retards de vol en moins de 15 minutes.

## 📋 Prérequis
- **Docker & Docker Compose** : Installés et fonctionnels
- **Ressources** : 16GB RAM minimum, 20GB espace disque
- **Données** : Téléchargez les datasets depuis [ce lien Dropbox](https://www.dropbox.com/sh/iasq7frk6f58ptq/AAAzSmk6cusSNfqYNYsnLGIXa)

## ⚡ Démarrage en 5 étapes

### 1. Préparation des données
```bash
# Téléchargez et placez les fichiers CSV dans work/data/
# - flights.csv (données de vol)
# - weather.csv (données météo)
# - airports.csv (mapping aéroports)
```

### 2. Configuration Docker
```bash
cd docker
./setup.sh
```
- Répondez "y" au nettoyage si demandé
- Patientez pendant la construction des images (~5-10 min)

### 3. Vérification du cluster
Une fois setup terminé, vérifiez :
- **Spark Master** : http://localhost:8080
- **Jupyter Lab** : http://localhost:8888
- **MLflow** : http://localhost:5555

### 4. Lancement d'une expérience
```bash
cd docker
./submit.sh
```
Le système va automatiquement :
- Charger et prétraiter les données
- Appliquer PCA pour réduction dimensionnelle
- Entraîner un Random Forest avec validation croisée
- Sauvegarder les résultats dans MLflow

### 5. Analyse des résultats
- **MLflow UI** : Comparez les métriques (accuracy ~85%, recall ~87%)
- **Visualisations** : Lancez les scripts Python dans `work/scripts/`
- **Logs** : `docker compose logs -f spark-master`

## 🎯 Résultats attendus
```
Cross-Validation Results (5 folds):
  Accuracy:   87.32% ± 1.23%
  Precision:  85.67% ± 2.10%
  Recall:     88.45% ± 1.87%
  F1-Score:   87.02% ± 1.56%
  AUC-ROC:    0.9234 ± 0.0156
```

## 🛠️ Commandes utiles
```bash
# Arrêter le cluster
docker compose down

# Redémarrer un service
docker compose restart spark-master

# Voir les logs
docker compose logs -f mlflow-server

# Nettoyer tout
docker compose down --volumes --remove-orphans
docker system prune -f
```

## 🔍 Dépannage rapide
- **Erreur Dockerfile** : Vérifiez que `docker/Dockerfile.jupyter` existe
- **Mémoire insuffisante** : Augmentez la RAM allouée à Docker
- **Port occupé** : Changez les ports dans `docker-compose.yml`
- **Données manquantes** : Vérifiez les chemins dans `work/data/`

## 📚 Prochaines étapes
- **Explorer le code** : Consultez `src/main/scala/com/flightdelay/`
- **Modifier la config** : Éditez `src/main/resources/local-config.yml`
- **Ajouter un modèle** : Suivez `docs/MD/10-adding-models.md`
- **Documentation complète** : Lisez `docs/MD/01-quick-start.md`

**Temps total estimé** : 15-20 minutes pour votre premier run !

✈️ **Bon vol dans le monde du ML !**