# Instructions de Redémarrage - MLFlow Integration

## ⚠️ IMPORTANT - À Lire Avant de Redémarrer

### Ce qui va se passer

1. **Arrêt des conteneurs actuels** (`docker-compose down`)
   - Tous les conteneurs seront arrêtés et supprimés
   - ❌ L'installation temporaire de MLFlow Python sera perdue (c'est normal!)
   - ✅ Les volumes (data, output, mlflow) seront **préservés**

2. **Construction de la nouvelle image** (`docker-compose build`)
   - Création de l'image `custom-spark-mlflow:3.5.3`
   - Installation permanente de MLFlow Python dans l'image
   - Durée: ~5-10 minutes (première fois)

3. **Démarrage des nouveaux conteneurs** (`docker-compose up -d`)
   - Tous les services démarrent avec la nouvelle image
   - MLFlow Python sera **installé automatiquement**
   - Volume `/mlflow` sera monté sur `spark-submit`

### Ce qui sera préservé ✅

- ✅ **Toutes les données** (`work/data/`)
- ✅ **Tous les outputs** (`work/output/`)
- ✅ **Base de données MLFlow** (`work/mlflow/mlflow.db`)
- ✅ **Anciens artifacts MLFlow** (`work/mlflow/artifacts/`)
- ✅ **Code source** (aucun changement)

### Ce qui sera perdu ❌

- ❌ Installations temporaires dans les conteneurs
  - MLFlow Python (sera réinstallé automatiquement via Dockerfile)
  - Symlink `/mlflow → /output/mlflow` (ne sera plus nécessaire)

## Instructions de Redémarrage

### Option 1: Script Automatique (Recommandé)

```bash
cd /Users/malikchettih/Projects/Emiasd-Projects/Emiasd-FlightProject/docker
./rebuild-and-local-restart.sh
```

Ce script fait tout automatiquement et affiche la progression.

### Option 2: Manuel (Étape par Étape)

```bash
cd /Users/malikchettih/Projects/Emiasd-Projects/Emiasd-FlightProject/docker

# 1. Arrêter les conteneurs
docker-compose down

# 2. Construire l'image custom (peut prendre 5-10 min)
docker-compose build

# 3. Démarrer tous les services
docker-compose up -d

# 4. Attendre que les services soient prêts
sleep 20

# 5. Vérifier que MLFlow est installé
docker exec spark-submit python3 -c "import mlflow; print(f'MLFlow {mlflow.__version__}')"
```

## Après le Redémarrage

### 1. Vérifier l'Installation

```bash
# Vérifier MLFlow sur spark-submit
docker exec spark-submit python3 -c "import mlflow; print(f'MLFlow {mlflow.__version__}')"

# Vérifier MLFlow sur workers
docker exec spark-worker-1 python3 -c "import mlflow; print(f'MLFlow {mlflow.__version__}')"

# Vérifier le volume mlflow
docker exec spark-submit ls -la /mlflow/
docker exec mlflow-server ls -la /mlflow/
```

**Attendu**:
```
MLFlow 3.4.0
```

### 2. Tester le Pipeline

```bash
cd /Users/malikchettih/Projects/Emiasd-Projects/Emiasd-FlightProject
./local-submit.sh
```

**Attendu**:
- ✅ Aucune erreur "Failed to log artifact"
- ✅ Logs montrant le succès de l'entraînement
- ✅ Artifacts visibles dans MLFlow UI

### 3. Vérifier l'Interface MLFlow

Ouvrez: http://localhost:5555

**Attendu**:
- ✅ Expériences précédentes toujours visibles
- ✅ Nouvelle expérience avec artifacts
- ✅ Metrics, parameters, et models tous loggés

## Rollback en Cas de Problème

Si quelque chose ne fonctionne pas, vous pouvez revenir à l'ancienne configuration:

```bash
cd docker

# Revenir à l'image Bitnami standard
git checkout docker-compose.yml

# Redémarrer
docker-compose down
docker-compose up -d

# Réinstaller MLFlow manuellement (comme avant)
docker exec -u root spark-submit bash -c 'apt-get update && apt-get install -y python3-pip && pip3 install mlflow==3.4.0'
```

## Temps Estimés

- **Construction de l'image**: 5-10 minutes (première fois)
- **Arrêt des conteneurs**: 10-30 secondes
- **Démarrage des conteneurs**: 30-60 secondes
- **Total**: ~6-12 minutes

## Prochaines Fois

Après la première construction, les redémarrages seront **beaucoup plus rapides** car l'image sera déjà construite:

```bash
docker-compose down
docker-compose up -d
# ~1 minute total
```

## Support

Si vous rencontrez des problèmes:

1. **Vérifier les logs**:
   ```bash
   docker-compose logs spark-submit
   docker-compose logs mlflow-server
   ```

2. **Vérifier les conteneurs**:
   ```bash
   docker ps -a
   ```

3. **Nettoyer et recommencer**:
   ```bash
   docker-compose down -v  # ⚠️ Supprime aussi les volumes!
   docker-compose build --no-cache
   docker-compose up -d
   ```

## Questions Fréquentes

**Q: Mes données seront-elles perdues?**
R: Non, toutes les données dans `work/` sont préservées.

**Q: Mes expériences MLFlow seront-elles perdues?**
R: Non, la base de données MLFlow (`mlflow.db`) est préservée.

**Q: Combien de temps ça prend?**
R: ~10 minutes la première fois, ~1 minute les fois suivantes.

**Q: Puis-je annuler si ça ne marche pas?**
R: Oui, voir la section "Rollback" ci-dessus.

**Q: Est-ce que je dois refaire ça à chaque fois?**
R: Non, une seule fois! Après, les conteneurs garderont MLFlow automatiquement.
