# Flight Delay Prediction - LAMSADE Cluster Deployment

Ce workflow GitHub Actions automatise le déploiement et l'exécution de l'application Spark sur le cluster LAMSADE avec support pour JAR local.

## 🚀 Utilisation

### Préparation du JAR local

**1. Build le JAR sur votre machine :**
```bash
./prepare-local-jar.sh
```
Ce script compile l'application et prépare le JAR pour déploiement.

**2. Options de déploiement :**

#### Option A - Upload JAR local (recommandé)
1. Allez sur : https://github.com/MalikChettihIA/Emiasd-FlightProject/actions
2. Sélectionnez "Flight Delay Prediction - LAMSADE Cluster Deployment"
3. Cliquez "Run workflow"
4. Configurez :
   - **JAR source** : `upload`
   - **JAR path** : `target/scala-2.12/emiasd-flight-data-analysis_2.12-1.0.jar`
   - **Target environment** : `prod`
   - **Run on LAMSADE cluster** : ✅ coché

#### Option B - Build automatique
- Choisissez **JAR source** : `build`
- Le workflow compilera automatiquement le JAR

### Configuration des Secrets GitHub

**Secrets requis :**
- `LAMSADE_SSH_KEY` : Votre clé SSH privée
- `LAMSADE_USERNAME` : Votre nom d'utilisateur

## 📋 Jobs du Workflow

### Job 1: Build JAR (conditionnel)
- Compile uniquement si "JAR source: build"
- Génère `emiasd-flight-data-analysis_2.12-1.0.jar`

### Job 1b: Upload JAR local (conditionnel)
- Utilise le JAR spécifié dans "JAR path"
- Vérifie que le fichier existe

### Job 2: Deploy to LAMSADE
- Upload JAR, données, dépendances MLflow
- Crée les répertoires HDFS
- Upload vers HDFS

### Job 3: Run on LAMSADE
- Soumet le job Spark avec YARN
- Télécharge les résultats

### Job 4: Local Pipeline
- Pipeline Docker pour développement

## 🛠️ Scripts utilitaires

### `prepare-local-jar.sh`
Prépare le JAR local pour déploiement :
```bash
./prepare-local-jar.sh
```

### `upload-jar-to-github.sh`
Upload automatique vers GitHub Actions :
```bash
./upload-jar-to-github.sh target/scala-2.12/emiasd-flight-data-analysis_2.12-1.0.jar
```

### `deploy-to-lamsade.sh`
Test de connexion au cluster :
```bash
./deploy-to-lamsade.sh ~/.ssh/lamsade_key username
```
- Soumet le job Spark sur le cluster avec YARN
- Configuration Spark optimisée pour le cluster :
  ```bash
  --master yarn
  --deploy-mode cluster
  --executor-cores 4
  --executor-memory 4G
  --num-executors 3
  ```
- Télécharge les résultats depuis HDFS
- **Artefact** : `lamsade-results`

### Job 4: Local Pipeline (par défaut)
- Exécute le pipeline complet en local avec Docker
- Utilise le cluster Spark local
- **Artefact** : `local-results`

## 🔧 Configuration Spark pour LAMSADE

Le workflow utilise ces paramètres Spark optimisés pour le cluster LAMSADE :

```yaml
--master yarn                    # Utilise YARN comme gestionnaire de ressources
--deploy-mode cluster           # Mode cluster pour meilleure scalabilité
--executor-cores 4              # 4 cœurs par executor
--executor-memory 4G            # 4GB RAM par executor
--num-executors 3               # 3 executors
--driver-memory 2G              # 2GB pour le driver
--driver-cores 2                # 2 cœurs pour le driver
```

## 📊 Artefacts Générés

Après exécution, vous pouvez télécharger :

- **flight-delay-app-jar** : JAR de l'application
- **lamsade-results** : Résultats du cluster LAMSADE
- **local-results** : Résultats du pipeline local

## 🔍 Monitoring

### Interface Spark UI
Pour monitorer vos jobs sur le cluster LAMSADE :
```
http://vmhadoopmaster.cluster.lamsade.dauphine.fr:8088
```

### Accès SSH direct
Vous pouvez toujours accéder directement au cluster :
```bash
ssh -p 5022 -i votre_cle_ssh votre_username@ssh.lamsade.dauphine.fr
```

## 🛠️ Dépannage

### Problèmes SSH
- Vérifiez que votre clé SSH est correctement configurée dans les secrets
- Assurez-vous que la clé n'a pas de passphrase
- Vérifiez les permissions de la clé (600)

### Problèmes HDFS
- Vérifiez que votre répertoire utilisateur existe : `/students/p6emiasd2025/votre_username/`
- Vérifiez les quotas HDFS si nécessaire

### Problèmes Spark
- Consultez les logs YARN via l'interface web
- Vérifiez la disponibilité des ressources cluster
- Ajustez les paramètres Spark si nécessaire

## 📝 Notes Importantes

- Le workflow utilise `--deploy-mode cluster` pour une meilleure isolation
- Les données sont automatiquement uploadées vers HDFS
- MLflow est configuré pour fonctionner avec le cluster
- Le timeout d'attente des jobs est de 30 minutes maximum

## 📝 Workflow Modes

| Mode | JAR Source | Cluster | Usage |
|------|------------|---------|-------|
| **Local Dev** | build | ❌ | Développement local |
| **Local JAR** | upload | ❌ | Test JAR local |
| **LAMSADE Auto** | build | ✅ | CI/CD complet |
| **LAMSADE JAR** | upload | ✅ | Déploiement JAR local |

---

**🎯 Résultat** : Build local + déploiement one-click sur LAMSADE ! 🚀