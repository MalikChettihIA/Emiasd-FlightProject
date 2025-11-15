# Ansible Deployment pour Flight Project

## Installation d'Ansible (avec venv)

### Création de l'environnement virtuel

```bash
cd ansible
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Activer le venv (à faire à chaque session)

```bash
cd ansible
source venv/bin/activate
```

### Désactiver le venv

```bash
deactivate
```

## Structure

```
ansible/
├── inventory.ini          # Inventaire des serveurs
├── deploy.yml            # Playbook principal (compile + deploy + run)
├── download-logs.yml     # Télécharger les logs
└── stop-app.yml          # Arrêter l'application
```

## Configuration

Éditez `ansible/inventory.ini` pour adapter :
- `ansible_user` : votre nom d'utilisateur
- `ansible_ssh_private_key_file` : chemin vers votre clé SSH
- `ansible_port` : port SSH

Éditez les `vars` dans chaque playbook pour adapter les chemins.

## Utilisation

⚠️ **N'oubliez pas d'activer le venv avant d'exécuter les commandes** : `source ansible/venv/bin/activate`

### 1. Déploiement complet (compile + deploy + upload + run)

```bash
cd ansible
source venv/bin/activate
ansible-playbook -i inventory.ini deploy.yml
```

### 2. Télécharger les logs

```bash
cd ansible
source venv/bin/activate
ansible-playbook -i inventory.ini download-logs.yml
```

### 3. Arrêter l'application

```bash
cd ansible
source venv/bin/activate
ansible-playbook -i inventory.ini stop-app.yml
```

### 4. Suivre les logs en temps réel

```bash
cd ansible
source venv/bin/activate
ansible-playbook -i inventory.ini tail-logs.yml
```

**Note** : Appuyez sur `Ctrl+C` pour arrêter le suivi des logs.

## Exécution tâche par tâche (avec tags)

### Lister les tags disponibles

```bash
ansible-playbook -i inventory.ini deploy.yml --list-tags
```

### Exécuter uniquement certaines tâches

```bash
# 1. Seulement la compilation
ansible-playbook -i inventory.ini deploy.yml --tags compile

# 2. Seulement le déploiement du JAR
ansible-playbook -i inventory.ini deploy.yml --tags deploy-jar

# 3. Seulement le déploiement de la config
ansible-playbook -i inventory.ini deploy.yml --tags deploy-config

# 4. Seulement le déploiement des données locales
ansible-playbook -i inventory.ini deploy.yml --tags deploy-data

# 5. Seulement l'upload HDFS
ansible-playbook -i inventory.ini deploy.yml --tags hdfs

# 6. Seulement l'exécution
ansible-playbook -i inventory.ini deploy.yml --tags run

# Combiner plusieurs tags
ansible-playbook -i inventory.ini deploy.yml --tags "compile,deploy-jar,deploy-config"
```

### Exclure certaines tâches

```bash
# Tout sauf l'exécution
ansible-playbook -i inventory.ini deploy.yml --skip-tags run

# Tout sauf la compilation
ansible-playbook -i inventory.ini deploy.yml --skip-tags compile
```

## Options avancées

### Afficher les logs de compilation en détail

```bash
# Mode verbeux simple (affiche les logs après chaque tâche)
ansible-playbook -i inventory.ini deploy.yml --tags compile -v

# Mode verbeux détaillé (affiche plus d'informations)
ansible-playbook -i inventory.ini deploy.yml --tags compile -vv

# Mode debug maximum (pour le débogage)
ansible-playbook -i inventory.ini deploy.yml --tags compile -vvv
```

### Exécuter avec des tâches Spark spécifiques

```bash
ansible-playbook -i inventory.ini deploy.yml -e "tasks_to_run=data-pipeline,train"
```

### Dry-run (test sans exécution)

```bash
ansible-playbook -i inventory.ini deploy.yml --check
```

### Changer le format d'affichage

Éditez `ansible.cfg` et changez `stdout_callback` :
- `yaml` : format lisible et coloré (par défaut)
- `debug` : format détaillé avec timestamps
- `json` : format JSON
- `default` : format Ansible standard

## Avantages par rapport au script bash

✅ Idempotence : peut être réexécuté sans problème  
✅ Gestion d'erreurs plus robuste  
✅ Logs structurés  
✅ Parallélisation possible sur plusieurs serveurs  
✅ Plus facile à maintenir et étendre  

## ✅ Vérifier que l'application fonctionne

Après avoir lancé l'application avec `--tags run`, vous pouvez :

**1. Télécharger les logs immédiatement :**
```bash
ansible-playbook -i inventory.ini download-logs.yml
```

**2. Consulter les logs téléchargés :**
```bash
# Voir le dernier log
ls -lht logs-from-cluster/ | head -5

# Lire le contenu du dernier log non-vide
find logs-from-cluster/ -name "*.log" -size +0 -type f -printf '%T@ %p\n' | sort -rn | head -1 | cut -d' ' -f2 | xargs cat
```

**3. Vérifier le statut de l'application :**
```bash
ansible-playbook -i inventory.ini check-status.yml
```

Cette commande affiche :
- ✓ Les processus en cours (runflight_v1.sh, spark-submit)
- ✓ Les applications YARN actives
- ✓ Les fichiers logs récents
- ✓ Les dernières lignes du log

**4. Suivre les logs en temps réel :**
```bash
ansible-playbook -i inventory.ini tail-logs.yml
```

**Note** : Pour une application longue (2h+), le playbook lance l'application avec `nohup` en arrière-plan. Elle continue même si votre connexion SSH est interrompue.

**5. Arrêter l'application :**
```bash
ansible-playbook -i inventory.ini stop-app.yml
```

## Prérequis

- Ansible installé sur la machine locale
- Accès SSH configuré au cluster
- SBT installé localement pour la compilation
- Python 3 sur le serveur distant


## Exécution directe via SSH (sans Ansible)

Pour lancer l'application directement via SSH sans passer par Ansible :

```bash
ssh -T -i ~/.ssh/id_ed25519_hbalamou.key -p 5022 hbalamou@ssh.lamsade.dauphine.fr bash << 'EOF'
LOG_DIR=$HOME/workspace/logs
mkdir -p $LOG_DIR
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE=$LOG_DIR/flight-app-${TIMESTAMP}.log
LATEST_LOG=$LOG_DIR/latest.log
nohup /opt/shared/spark-current/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --class com.flightdelay.app.FlightDelayPredictionApp \
    --files $HOME/workspace/config/prodlamsade-config.yml \
    --driver-memory 32G \
    --driver-cores 8 \
    --executor-memory 8G \
    --num-executors 6 \
    --conf spark.driver.maxResultSize=8g \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.default.parallelism=400 \
    --jars $HOME/workspace/apps/mlflow-client-3.4.0.jar,$HOME/workspace/apps/mlflow-spark_2.13-3.4.0.jar \
    $HOME/workspace/apps/Emiasd-Flight-Data-Analysis.jar \
    prodlamsade data-pipeline,feature-extraction,train > $LOG_FILE 2>&1 &
ln -sf $LOG_FILE $LATEST_LOG
echo "Job started in background. Log file: $LOG_FILE"
EOF
```

Cette commande :
- Se connecte au cluster via SSH
- Lance l'application Spark en arrière-plan avec `nohup`
- Crée un fichier de log avec timestamp
- Maintient un lien symbolique vers le dernier log
- Affiche le nom du fichier de log créé



### Mode interactif avec logs en temps réel

Pour lancer l'application et voir les logs en temps réel (attention : la session reste active) :

```bash
ssh -T -i ~/.ssh/id_ed25519_hbalamou.key -p 5022 hbalamou@ssh.lamsade.dauphine.fr bash << 'EOF'
LOG_DIR=$HOME/workspace/logs
mkdir -p $LOG_DIR
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE=$LOG_DIR/flight-app-${TIMESTAMP}.log
echo "Log file: $LOG_FILE"
/opt/shared/spark-current/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --class com.flightdelay.app.FlightDelayPredictionApp \
    --files $HOME/workspace/config/prodlamsade-config.yml \
    --driver-memory 32G \
    --driver-cores 8 \
    --executor-memory 8G \
    --num-executors 6 \
    --conf spark.driver.maxResultSize=8g \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.default.parallelism=400 \
    --jars $HOME/workspace/apps/mlflow-client-3.4.0.jar,$HOME/workspace/apps/mlflow-spark_2.13-3.4.0.jar \
    $HOME/workspace/apps/Emiasd-Flight-Data-Analysis.jar \
    prodlamsade data-pipeline,feature-extraction,train 2>&1 | tee $LOG_FILE
EOF
```




**Note** : Cette commande maintient la session SSH active et affiche les logs en temps réel. Utilisez `Ctrl+C` pour interrompre.


### Mode détaché avec screen

Pour lancer l'application dans une session screen (permet de se déconnecter sans interrompre le job) :

```bash
ssh -i ~/.ssh/id_ed25519_hbalamou.key -p 5022 hbalamou@ssh.lamsade.dauphine.fr "screen -dmS spark_job bash -c '
LOG_DIR=\$HOME/workspace/logs
mkdir -p \$LOG_DIR
TIMESTAMP=\$(date +%Y%m%d_%H%M%S)
LOG_FILE=\$LOG_DIR/flight-app-\${TIMESTAMP}.log
LATEST_LOG=\$LOG_DIR/latest.log
/opt/shared/spark-current/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --class com.flightdelay.app.FlightDelayPredictionApp \
    --files \$HOME/workspace/config/prodlamsade-config.yml \
    --driver-memory 32G \
    --driver-cores 8 \
    --executor-memory 8G \
    --num-executors 6 \
    --conf spark.driver.maxResultSize=8g \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.default.parallelism=400 \
    --jars \$HOME/workspace/apps/mlflow-client-3.4.0.jar,\$HOME/workspace/apps/mlflow-spark_2.13-3.4.0.jar \
    \$HOME/workspace/apps/Emiasd-Flight-Data-Analysis.jar \
    prodlamsade data-pipeline,feature-extraction,train 2>&1 | tee \$LOG_FILE
ln -sf \$LOG_FILE \$LATEST_LOG
' && echo 'Screen session spark_job started'"
```

**Avantages du mode screen** :
- Le job continue même si la connexion SSH est interrompue
- Possibilité de se reconnecter à la session avec `screen -r spark_job`
- Plus robuste que `nohup` pour les longues exécutions

**Pour vérifier et gérer la session** :
```bash
# Lister les sessions screen actives
ssh -i ~/.ssh/id_ed25519_hbalamou.key -p 5022 hbalamou@ssh.lamsade.dauphine.fr "screen -ls"

# Se reconnecter à la session
ssh -i ~/.ssh/id_ed25519_hbalamou.key -p 5022 hbalamou@ssh.lamsade.dauphine.fr -t "screen -r spark_job"

# Tuer la session si besoin
ssh -i ~/.ssh/id_ed25519_hbalamou.key -p 5022 hbalamou@ssh.lamsade.dauphine.fr "screen -S spark_job -X quit"
```