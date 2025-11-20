# ✨ Rapport d’implémentation : cluster Spark local (`docker/`)

**Stack logicielle (versions)**  
- Spark : 3.5.3 (Bitnami & Jupyter)  
- Scala : 2.12 (Delta Lake / kernels Toree & spylon)  
- Java : OpenJDK 11 (bitnamilegacy + all-spark-notebook ARM64)  
- MLflow : 3.4.0 (serveur & clients pip)  
- Python : 3.10 (image Jupyter) / 3.9 (Bitnami Spark)  
- Delta Lake : 3.2.1  
- JupyterLab : distribution `all-spark-notebook` 2024-08-02 (quay.io)  
- Prometheus : 2.48.0 | Grafana : 10.2.2 | cAdvisor : 0.47.2  
- Docker Engine requise : 24.x (Docker Desktop sur macOS)

## Résumé
Ce rapport présente la conception, la configuration et l’évaluation d’un cluster Spark local intégré au dépôt `Emiasd-FlightProject`. L’infrastructure, décrite dans `docker/docker-compose.yml`, vise à reproduire une topologie de production (1 master, 4 workers, outils MLOps) tout en restant exécutable sur une station de développement. Nous détaillons les choix d’images, les mécanismes de configuration (volumes, variables d’environnement, agents JMX), les scripts de soumission, ainsi que les limites opérationnelles observées. Enfin, nous proposons des perspectives d’amélioration inspirées des pratiques de clusters distribués à grande échelle.

## 1. Introduction
Les pipelines Scala/Spark qui orchestrent l’ingestion météo et l’entraînement des modèles Random Forest exigent un environnement distribué pour rester représentatifs. L’objectif était donc de fournir, dans le dossier `docker/`, un cluster reproductible, monitoré, et connecté au serveur MLflow sans nécessiter un déploiement cloud. La solution devait aussi permettre l’exploration interactive (Jupyter) et l’inspection post-mortem des jobs (Spark History). Nous avons retenu Docker Compose comme orchestrateur afin de simplifier l’intégration avec les scripts existants (`start.sh`, `submit.sh`, `stop.sh`).

## 2. Architecture générale
- **Services Spark** : `spark-master` (image `bitnamilegacy/spark:3.5.3`) expose les ports 7077 (RPC) et 8080 (UI). Quatre workers (`spark-worker-{1..4}`) sont bâtis à partir d’une image custom `custom-spark-mlflow:3.5.3` issue de `docker/Dockerfile.spark-mlflow`. Chaque worker déclare `SPARK_WORKER_CORES=3`, `SPARK_WORKER_MEMORY=10G`, ce qui reproduit le profil décrit dans nos expériences (section README « Key Features »). L’orchestration a été optimisée pour un MacBook Pro M4 (14 cœurs physiques, 48 Gi RAM) : Docker Desktop reçoit 12 cœurs/36 Gi, ce qui laisse une marge au système hôte tout en offrant 12 cœurs logiques et 40 Gi utilisables par Spark (4×3 cœurs + overhead JVM).

- **Canal de soumission** : `spark-submit` utilise la même image custom mais reste en veille (`command: tail -f /dev/null`). Les scripts `submit.sh` et `/scripts/spark-submit.sh` injectés dans `../work/scripts` déclenchent les jobs via `docker exec spark-submit`.
- **Plan de contrôle data science** : `jupyter` (Dockerfile `docker/Dockerfile.jupyter`) fournit JupyterLab configuré avec `SPARK_MASTER_URL`, `SPARK_EXECUTOR_MEMORY=7g` et une fenêtre `4040-4050` pour les Spark UI éphémères. MLflow (`ghcr.io/mlflow/mlflow:v3.4.0`) assure le suivi des expériences, tandis que `spark-history` relit `../work/spark-events`.
- **Observabilité** : un agent JMX est attaché à chaque master/worker (`-javaagent:/opt/jmx-exporter/...`). Prometheus (`prom/prometheus:v2.48.0`), Grafana (`grafana/grafana:10.2.2`) et cAdvisor complètent la chaîne.

Toutes les composantes résident sur un réseau commun `spark-network` (driver bridge), favorisant les résolutions DNS entre conteneurs.

## 3. Implémentation des images et du kernel Scala
### 3.1 Image Spark/MLflow (`docker/Dockerfile.spark-mlflow`)
Le Dockerfile étend `bitnamilegacy/spark:3.5.3` pour garantir la compatibilité avec Spark 3.5.3 et Java 11. Les sections clés sont reproduites ci-dessous :
```dockerfile
FROM bitnamilegacy/spark:3.5.3
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip python3-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir \
    mlflow==3.4.0 matplotlib==3.8.2 seaborn==0.13.0 \
    pandas==2.1.4 numpy==1.26.2 scikit-learn==1.3.2
ENV MPLCONFIGDIR=/tmp/matplotlib
USER 1001
```
1. Passage en `USER root` pour installer `python3-pip`/`python3-dev`.  
2. Installation via `pip` de `mlflow==3.4.0`, `matplotlib`, `seaborn`, `pandas`, `numpy`, `scikit-learn`. Ces versions alignent la CLI MLflow utilisée dans nos jobs et permettent la génération de graphiques produits nativement par les pipelines Scala.  
3. Variable `MPLCONFIGDIR=/tmp/matplotlib` afin de contourner les permissions restreintes sur `/home/1001/.config`.  
4. Retour au `USER 1001` (Bitnami) : les volumes montés depuis `../work` doivent donc être accessibles en écriture par cet UID/GID (cf. section 5).

Cette image est référencée par les services `spark-worker-*` et `spark-submit`. Elle garantit que la même stack Python/MLflow est disponible sur tous les nœuds où des scripts auxiliaires (ex. visualisations) peuvent s’exécuter, ainsi qu’un environnement identique pour les jobs submit et les workers.

### 3.2 Image Jupyter (`docker/Dockerfile.jupyter`)
L’image Jupyter vise une expérience interactive complète côté développeur, tout en restant compatible ARM64 (MacBook Pro M4). Extrait :
```dockerfile
FROM quay.io/jupyter/all-spark-notebook:spark-3.5.3
RUN apt-get install -y openjdk-11-jdk-headless scala curl wget netcat-openbsd
ENV SPARK_HOME=/usr/local/spark \
    SPARK_VERSION=3.5.3 \
    DELTA_VERSION=3.2.1 \
    JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-arm64
RUN wget https://archive.apache.org/.../spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf ... && mv ... $SPARK_HOME
RUN wget -O $SPARK_HOME/jars/delta-spark_2.12-${DELTA_VERSION}.jar ... && \
    wget -O $SPARK_HOME/jars/delta-storage-3.2.1.jar ...
RUN pip install pyspark==${SPARK_VERSION} delta-spark==${DELTA_VERSION} pyngrok \
    toree==0.5.0 spylon-kernel plotly seaborn pandas numpy matplotlib
RUN jupyter toree install --user --spark_home=$SPARK_HOME --kernel_name="apache_toree_scala"
```
Points saillants :
1. Base `all-spark-notebook` (ARM64-ready) sur laquelle on réinstalle Spark 3.5.3 pour rester aligné avec le cluster.  
2. Déploiement des jars Delta Lake (`delta-spark`, `delta-storage`), permettant d’explorer les mêmes tables que dans les jobs Scala.  
3. Installation simultanée de `toree` et `spylon-kernel` pour disposer d’un kernel Scala natif et d’un kernel PySpark Scala-like.  
4. Copie d’un `kernel.json` personnalisé (voir §3.3) qui force l’utilisation du master `spark://spark-master:7077` et applique les mêmes options d’event log que les jobs batch.  
5. La commande finale `CMD ["start-notebook.sh", ...]` désactive l’authentification par token pour simplifier les sessions locales (à restreindre si nécessaire).

### 3.3 Kernel Apache Toree personnalisé (`docker/kernel.json`)
Pour disposer d’un kernel Scala pleinement connecté au cluster, nous avons modifié le `kernel.json` généré par Toree. Contenu :
```json
{
  "argv": [
    "/home/jovyan/.local/share/jupyter/kernels/apache_toree_scala_scala/bin/run.sh",
    "--profile",
    "{connection_file}"
  ],
  "env": {
    "DEFAULT_INTERPRETER": "Scala",
    "__TOREE_SPARK_OPTS__": "--master spark://spark-master:7077 --conf spark.executor.heartbeatInterval=60s --conf spark.sql.shuffle.partitions=128 --conf spark.default.parallelism=128 --conf spark.sql.files.maxPartitionBytes=64m --conf spark.sql.debug.maxToStringFields=1000 --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --jars /home/jovyan/work/apps/* --driver-class-path /home/jovyan/work/apps/* --conf spark.jars=/home/jovyan/work/apps/*",
    "__TOREE_OPTS__": "",
    "SPARK_HOME": "/usr/local/spark"
  },
  "display_name": "apache_toree_scala - Scala",
  "language": "scala"
}
```
- `DEFAULT_INTERPRETER=Scala` garantit que le kernel démarre en mode Scala (et non PySpark).  
- `__TOREE_SPARK_OPTS__` pointe vers `spark://spark-master:7077` et applique les mêmes réglages d’optimisation que les jobs batch (heartbeat, partitions shuffle, `spark.sql.adaptive.*`). Il injecte également automatiquement les jars générés (`/home/jovyan/work/apps/*`) pour permettre d’appeler les classes du pipeline Scala depuis les notebooks.  
- En copiant ce fichier dans `/home/jovyan/.local/share/jupyter/kernels/apache_toree_scala_scala/kernel.json`, nous écrasons la configuration par défaut de Toree et alignons l’expérience notebook avec le cluster Docker.

Les notebooks Scala peuvent ainsi charger `FlightDelayPredictionApp` ou des utilitaires de `com.flightdelay.*` sans recompilation ni configuration additionnelle.

## 4. Description détaillée de `docker/docker-compose.yml`
Le fichier Compose constitue la « spécification » du cluster. Les aspects clés sont :

### 4.1 Définition des services Spark
- Extrait pour le master (`docker/docker-compose.yml`, lignes dédiées) :
```yaml
  spark-master:
    image: bitnamilegacy/spark:3.5.3
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_DAEMON_MEMORY=2g
      - SPARK_MASTER_OPTS=-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9999:/opt/jmx-exporter/jmx-exporter-config.yml
    ports:
      - "8080:8080"
      - "7077:7077"
      - "9999:9999"
```

- **spark-master** : 
  - Variables `SPARK_RPC_AUTHENTICATION_ENABLED=no`, `SPARK_SSL_ENABLED=no` simplifient le déploiement local mais désactivent toute sécurité réseau.  
  - `SPARK_DAEMON_MEMORY=2g` garantit que les processus de supervision restent stables.  
  - Le healthcheck `</dev/tcp/localhost/8080` assure que la UI est disponible avant de démarrer les workers.
- **spark-worker-{1..4}** :
  - Extrait standard :
```yaml
  spark-worker-1:
    build:
      context: .
      dockerfile: Dockerfile.spark-mlflow
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=10G
      - SPARK_WORKER_CORES=3
      - SPARK_WORKER_WEBUI_PORT=8081
      - SPARK_LOCAL_DIRS=/tmp/spark-local
      - SPARK_WORKER_OPTS=-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9999:/opt/jmx-exporter/jmx-exporter-config.yml
    volumes:
      - ../work/tmp/spark-tmp-1:/tmp/spark-local
```

  - Chaque worker construit l’image à partir du Dockerfile custom, puis configure `SPARK_WORKER_WEBUI_PORT=8081..8084` afin de disposer d’interfaces distinctes.  
  - Les volumes montent `../work/data`, `../work/apps`, `../work/output` et un dossier temporaire distinct (`../work/tmp/spark-tmp-{n}`) qui sert à `SPARK_LOCAL_DIRS`. Cette séparation limite les conflits d’espace disque.  
  - Les agents JMX sont exposés sur `9101..9104`.

### 4.2 Soumission des jobs
Le service `spark-submit` monte également `../work/scripts` et `../work/libs`. Lorsqu’on exécute `./submit.sh`, la commande suivante est lancée :
```bash
docker exec -it spark-submit chmod +x /scripts/spark-submit.sh
docker exec -it spark-submit /scripts/spark-submit.sh
```
`/scripts/spark-submit.sh` appelle ensuite `/opt/bitnami/spark/bin/spark-submit` avec les arguments définis dans nos configs YAML (chemin du jar `work/apps/Emiasd-Flight-Data-Analysis.jar`, chemin du `local-config.yml`, etc.). La présence du conteneur `spark-submit` évite d’installer Spark sur la machine hôte et garantit que les dépendances (MLflow, Python libs) sont identiques aux workers.

### 4.3 Services de support
- **jupyter** — configuration exposée :
```yaml
  jupyter:
    build:
      context: ..
      dockerfile: docker/Dockerfile.jupyter
    ports:
      - "8888:8888"
      - "4040-4050:4040-4050"
    environment:
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_EXECUTOR_MEMORY=7g
      - SPARK_DRIVER_MEMORY=8g
      - SPARK_OPTS=--driver-java-options=-Xms1024M ... --conf spark.eventLog.enabled=true
```

- **jupyter** : expose 8888 et 4040-4050, configure `PYSPARK_DRIVER_PYTHON=jupyter` et `SPARK_OPTS` (activation de l’event log).  
- **mlflow** : stockage SQLite dans `../work/mlflow/mlflow.db` et artefacts dans `../work/mlflow/artifacts`. L’argument `--host 0.0.0.0 --port 5000` est remappé sur `5555:5000` (port local 5555).  
- **spark-history** : exécute `start-history-server.sh` et lit `../work/spark-events`. L’option `SPARK_HISTORY_OPTS` augmente la limite JSON pour supporter les jobs volumineux. Ce service expose l’UI sur `http://localhost:18080` et permet d’inspecter tous les jobs terminés, y compris ceux soumis via `spark-submit` ou Jupyter. Les event logs sont écrits par chaque driver grâce aux options passées dans `kernel.json` et `SparkSession.builder`.  
- **prometheus** : charge la configuration `docker/monitoring/prometheus/prometheus.yml` qui déclare les targets JMX (`spark-master:9999`, `spark-worker-X:9999`).  
- **grafana** : est provisionné automatiquement via `monitoring/grafana/provisioning` et précharge les dashboards JSON du même dossier.  
- **cadvisor** : collecte les métriques Docker au niveau hôte, expose 8889 pour visualiser la consommation par conteneur.

### 4.4 Réseau et volumes
Deux volumes nommés sont déclarés (`prometheus-data`, `grafana-data`) afin de persister les métriques et dashboards. Tout le reste repose sur des bind mounts depuis `../work`, ce qui maintient la cohérence des chemins et permet à Git d’ignorer les artefacts lourds.

### 4.5 Observabilité intégrée (Prometheus + Grafana)
Le cluster embarque un pipeline de monitoring complet :
- **Prometheus (9090)** scrappe les endpoints JMX exposés par le master et les workers (`spark-master:9999`, `spark-worker-1:9999`, etc.) via la configuration `monitoring/prometheus/prometheus.yml`. Les métriques collectées couvrent :
  - Utilisation CPU/RAM des processus Spark (daemon et executors) ;
  - Nombre d’executors actifs, tâches en cours, jobs terminés/échoués (via `spark_worker_*` séries) ;
  - Temps de garbage collection JVM, taux d’allocations mémoire ;
  - Export `cadvisor` pour suivre CPU/mémoire/disque de chaque conteneur Docker.
- **Grafana (3000)** charge automatiquement les dashboards JSON présents dans `monitoring/grafana/dashboards-json`. Trois tableaux de bord principaux sont fournis :
  1. *Spark Cluster Overview* — offre une vue consolidée sur le master (état RPC, latence des heartbeat) et sur chaque worker (cores utilisés, mémoire, tâches actives). Les panels s’appuient directement sur les métriques JMX.  
  2. *Executors & Jobs* — détaille les temps de scheduling, la durée moyenne des stages, le nombre d’executors disponibles vs. utilisés, et les volumes shuffle. Utile pour diagnostiquer les goulots (ex. `spark.sql.shuffle.partitions`).  
  3. *Docker Host Metrics* — basé sur cAdvisor, affiche CPU, mémoire, IO par conteneur (`spark-master`, `spark-worker-2`, `mlflow`, etc.), permettant d’identifier rapidement les services saturés ou sous-utilisés.

Ces tableaux de bord permettent de :  
- Vérifier que les ressources allouées (3 cœurs / 10 Go par worker) sont réellement consommées par les jobs ;  
- Détecter les erreurs (jobs échoués, executors perdus) sans ouvrir les UIs Spark individuellement ;  
- Mesurer l’impact d’une nouvelle configuration (ex. variation de `spark.sql.shuffle.partitions`) en observant les graphes de durée et de shuffle ;  
- Surveiller les conteneurs annexes (MLflow, Jupyter) afin de s’assurer qu’ils ne pénalisent pas le cluster.

Grafana expose les dashboards sur http://localhost:3000 (admin/admin par défaut). Prometheus reste accessible sur http://localhost:9090 pour des requêtes ad-hoc (PromQL).

### 4.6 Conteneurs documentés un à un
Pour référence rapide, chaque service déclaré dans `docker/docker-compose.yml` est détaillé ci-dessous.

#### spark-master
```yaml
  spark-master:
    image: bitnamilegacy/spark:3.5.3
    hostname: spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_DAEMON_MEMORY=2g
      - SPARK_MASTER_OPTS=-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9999:/opt/jmx-exporter/jmx-exporter-config.yml
    ports:
      - "8080:8080"
      - "7077:7077"
      - "9999:9999"
```
Ce conteneur fournit le point de contrôle central du cluster : il expose l’endpoint RPC `spark://spark-master:7077` auquel tous les workers et drivers se connectent, ainsi qu’une interface Web sur 8080 pour inspecter les jobs actifs. Les flags `SPARK_RPC_AUTHENTICATION_ENABLED=no` et `SPARK_SSL_ENABLED=no` simplifient les échanges en réseau local, tandis que `SPARK_MASTER_OPTS` injecte le Java agent Prometheus afin d’exporter toutes les métriques JVM (threads, mémoire, scheduler). Les volumes montés (`../work/...`) donnent accès aux mêmes datasets que les workers et permettent de publier les informations de cluster sur le disque partagé.

#### spark-worker-1 … spark-worker-4
```yaml
  spark-worker-2:
    build:
      context: .
      dockerfile: Dockerfile.spark-mlflow
    hostname: spark-worker-2
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=10G
      - SPARK_WORKER_CORES=3
      - SPARK_WORKER_WEBUI_PORT=8082
      - SPARK_DAEMON_MEMORY=2g
      - SPARK_LOCAL_DIRS=/tmp/spark-local
      - SPARK_WORKER_OPTS=-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9999:/opt/jmx-exporter/jmx-exporter-config.yml
    volumes:
      - ../work/data:/data
      - ../work/apps:/apps
      - ../work/output:/output
      - ../work/tmp/spark-tmp-2:/tmp/spark-local
```
Quatre instances identiques assurent la parallélisation des traitements. Chaque worker réserve 3 cœurs vCPU et 10 Go de RAM pour les executors qu’il héberge, publie une UI propre (8081‑8084) et écrit les fichiers temporaires dans un répertoire dédié (`../work/tmp/spark-tmp-n`). Les options `SPARK_WORKER_OPTS` branchent également le Java agent pour exporter des métriques sur les ports 9101‑9104. Les montages `../work/data`, `../work/apps`, `../work/output` garantissent que toutes les partitions de données, jars et sorties Spark restent partagées entre workers et drivers, évitant les copies additionnelles.

#### spark-submit
```yaml
  spark-submit:
    build:
      context: .
      dockerfile: Dockerfile.spark-mlflow
    command: tail -f /dev/null
    volumes:
      - ../work/data:/data
      - ../work/apps:/apps
      - ../work/output:/output
      - ../work/scripts:/scripts
      - ../work/libs:/libs
      - ../work/mlflow:/mlflow
      - ../work/spark-events:/spark-events
    deploy:
      resources:
        limits:
          memory: 32G
          cpus: '10'
```
Ce service n’exécute aucun daemon Spark en continu : il reste en veille (`tail -f /dev/null`) et sert uniquement de shell d’exécution pour les jobs via `docker exec spark-submit /scripts/spark-submit.sh`. L’image étant identique à celle des workers, les dépendances Python/MLflow et les variables Spark restent cohérentes entre driver et executors. Les limites `memory: 32G` et `cpus: '10'` autorisent de gros broadcasts et des phases de shuffle intensives lors de la recherche d’hyperparamètres, sans menacer la stabilité du host macOS. Les répertoires montés (`/scripts`, `/libs`, `/mlflow`) exposent tous les artefacts nécessaires au job.

#### jupyter
```yaml
  jupyter:
    build:
      context: ..
      dockerfile: docker/Dockerfile.jupyter
    ports:
      - "8888:8888"
      - "4040-4050:4040-4050"
    environment:
      - JUPYTER_ENABLE_LAB=yes
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_DRIVER_MEMORY=8g
      - SPARK_EXECUTOR_MEMORY=7g
      - SPARK_OPTS=--driver-java-options=-Xms1024M ... --conf spark.eventLog.enabled=true
    volumes:
      - ../work:/home/jovyan/work
```
Cette image all-spark-notebook personnalisée installe Spark 3.5.3, Delta Lake, Toree et Spylon pour offrir une expérience interactive complète. Les variables `SPARK_MASTER_URL`, `SPARK_DRIVER_MEMORY` et `SPARK_EXECUTOR_MEMORY` sont injectées afin que les notebooks Scala/Python se connectent automatiquement au master Docker et respectent des quotas (8 Go driver, 7 Go executor). La plage 4040‑4050 est mappée pour permettre la visualisation simultanée de plusieurs Spark UI éphémères (une par session). Le montage `../work` expose les mêmes jars (`/home/jovyan/work/apps`) que ceux utilisés par le pipeline scala, autorisant l’import direct d’`FlightDelayPredictionApp` ou des utilitaires `com.flightdelay.*`.

#### mlflow
```yaml
  mlflow:
    image: ghcr.io/mlflow/mlflow:v3.4.0
    ports:
      - "5555:5000"
    volumes:
      - ../work/mlflow:/mlflow
      - ../work/mlflow/artifacts:/mlflow/artifacts
    command: >
      mlflow server
      --backend-store-uri sqlite:///mlflow/mlflow.db
      --default-artifact-root /mlflow/artifacts
```
Ce conteneur encapsule le serveur MLflow officiel (3.4.0) et persiste toutes les données scientifiques dans `../work/mlflow`. La base SQLite (`mlflow.db`) assure l’état (runs, params, metrics), tandis que le répertoire `/mlflow/artifacts` conserve les modèles Spark, les plots et les dumps parquet produits par le pipeline. Son exposition sur `5555:5000` permet d’y accéder depuis l’hôte (`http://localhost:5555`) et depuis les autres conteneurs via `mlflow:5000`. Tous les jobs `spark-submit` et notebooks s’y connectent grâce aux variables `MLFLOW_TRACKING_URI` définies dans les configurations YAML.

#### spark-history
```yaml
  spark-history:
    image: bitnamilegacy/spark:3.5.3
    environment:
      - SPARK_MODE=master
      - "SPARK_HISTORY_OPTS=-Dspark.history.server.jsonStreamReadConstraints.maxStringLength=100000000 -Dspark.history.fs.logDirectory=file:///spark-events"
    command: bash -c "/opt/bitnami/spark/sbin/start-history-server.sh && tail -f /dev/null"
    ports:
      - "18080:18080"
    volumes:
      - ../work/spark-events:/spark-events
```
La Spark History Server lit en continu les fichiers event log générés par chaque driver (stockés sous `../work/spark-events`) et les expose via une UI accessible sur `http://localhost:18080`. L’option `SPARK_HISTORY_OPTS` augmente la limite de lecture JSON pour supporter des jobs lourds et fixe `spark.history.fs.logDirectory` vers le bind mount partagé. Grâce à ce service, on peut rejouer toutes les exécutions (soumises via Jupyter ou `spark-submit`), télécharger les métriques et analyser les stages, même après l’arrêt des drivers.

#### prometheus
```yaml
  prometheus:
    image: prom/prometheus:v2.48.0
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus:/etc/prometheus
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.retention.time=15d'
```
Prometheus centralise les métriques exportées par le master, les workers et cAdvisor. Le fichier `monitoring/prometheus/prometheus.yml` déclare chaque endpoint (`spark-master:9999`, `spark-worker-1:9999`, `cadvisor:8080`) et définit des jobs de scraping spécifiques. Le rangement des données dans le volume `prometheus-data` garantit une rétention de 15 jours (paramètre `--storage.tsdb.retention.time`), ce qui permet de corréler les comportements Spark sur plusieurs itérations de tuning. Les requêtes PromQL sont accessibles via http://localhost:9090.

#### grafana
```yaml
  grafana:
    image: grafana/grafana:10.2.2
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=admin
      - GF_INSTALL_PLUGINS=grafana-piechart-panel
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning
      - ./monitoring/grafana/dashboards-json:/var/lib/grafana/dashboards
      - grafana-data:/var/lib/grafana/data
```
Ce conteneur charge automatiquement les dashboards JSON fournis dans `monitoring/grafana/dashboards-json` et configure les data sources/promotions via les fichiers de provisioning. Il s’authentifie par défaut avec `admin/admin` (modifiable via variables), expose son interface sur http://localhost:3000 et stocke la configuration utilisateur dans `grafana-data`. Grâce au plugin `grafana-piechart-panel` préinstallé, nous visualisons rapidement la répartition des ressources (CPU/mémoire) entre services et nous suivons l’évolution des jobs Spark dans le temps.

#### cadvisor
```yaml
  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.47.2
    ports:
      - "8889:8080"
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /var/lib/docker:/var/lib/docker:ro
    privileged: true
```
cAdvisor explore `/var/lib/docker` de l’hôte pour mesurer CPU, mémoire, IO et consommation disque de chaque conteneur. Les volumes montés en lecture seule garantissent la visibilité sur l’ensemble du daemon Docker tout en limitant les risques de corruption. Le mode `privileged` est requis pour accéder à `/dev/kmsg` et certaines statistiques noyau. L’interface Web (http://localhost:8889) offre une vue instantanée, tandis que Prometheus exploite l’endpoint `cadvisor:8080/metrics` pour historiser ces valeurs et les exploiter dans Grafana.

## 5. Contraintes et défis techniques
1. **Empreinte mémoire** : avec quatre workers (10 Go chacun, 3 cœurs affectés) et un `spark-submit` pouvant consommer jusqu’à 32 Go (section `deploy.resources.limits`), la machine hôte doit disposer d’au moins 64 Go pour éviter le swapping. Cette configuration a été calibrée spécifiquement pour un MacBook Pro M4 (14 cœurs, 48 Gi de RAM). Sur cette machine, nous allouons à Docker Desktop environ 36 Gi/12 cœurs pour laisser 12 Gi/2 cœurs aux processus macOS. En dessous de ces seuils, les OOM apparaissent lorsque Jupyter et Spark soumettent des jobs simultanément.
2. **Temps de démarrage** : le `depends_on` conditionne les workers à l’état « healthy » du master. Si le healthcheck échoue (port 8080 indisponible), `docker compose up` reste bloqué jusqu’au timeout. L’ajout du flag `--attach-dependencies` peut diagnostiquer plus rapidement les erreurs de mise en réseau.
3. **Permissions** : l’image Bitnami exécute Spark sous l’UID 1001. Lorsque `../work` est créé par un utilisateur différent, certains dossiers (notamment `../work/tmp` ou `../work/mlflow`) doivent être chown/chmod manuellement (`sudo chown -R 1001:1001 work`). Faute de quoi, les workers échouent à créer les répertoires `spark-local`.
4. **Saturation storage** : `../work/spark-events` et `../work/mlflow/artifacts` peuvent grossir rapidement. Sans rotation ou nettoyage, ils atteignent plusieurs dizaines de Go après quelques jours d’expérimentation. Pour limiter la taille de `spark-events`, il est possible de désactiver temporairement le logging en retirant `--conf spark.eventLog.enabled=true` des scripts de soumission ou du `kernel.json`, ou d’abaisser `spark.history.fs.cleaner.maxAge` dans `spark-history` afin d’expurger automatiquement les logs anciens.
5. **Sécurité** : aucune authentification n’est activée (RPC, SSL, MLflow). Les services sont exposés sur des ports `localhost`, mais rien n’empêche un utilisateur local de s’y connecter. Cette configuration n’est donc pas utilisable telle quelle dans un environnement partagé.
6. **Observabilité partielle** : nous exposons uniquement les métriques JMX génériques. Les métriques spécifiques (shuffle, executors dynamiques, métriques MLflow) ne sont pas encore collectées ni correlées dans Grafana. De plus, l’absence d’Alertmanager limite la détection proactive des anomalies.

## 6. Recommandations et améliorations futures
1. **Profils Compose** : introduire des profils (`docker compose --profile base up`, `--profile monitoring`) pour ajuster dynamiquement le nombre de services et réduire l’empreinte lors des tests unitaires.
2. **Sécurisation** : activer `SPARK_RPC_AUTHENTICATION_ENABLED=yes`, `SPARK_SSL_ENABLED=yes`, configurer des certificats montés via secrets Docker, et protéger MLflow avec un proxy ou un token d’authentification.
3. **Gestion du stockage** : externaliser `spark-events` et les artefacts MLflow vers un backend objet (MinIO/S3) ou un disque dédié, et implémenter un job de nettoyage automatisé (Cron dans l’hôte ou conteneur sidecar) pour purger les anciens logs.
4. **Observabilité avancée** : intégrer les endpoints HTTP du Spark History Server et de MLflow dans Prometheus, ajouter Alertmanager, et enrichir les dashboards Grafana avec des panels sur les files d’attente de jobs et la saturation des shuffle services.
5. **Soumission à la demande** : remplacer le conteneur `spark-submit` toujours actif par un service éphémère déclenché via `docker compose run spark-submit ...`. Cela réduirait la consommation mémoire et permettrait de définir des limites spécifiques par job.
6. **Migration Kubernetes** : envisager une déclinaison Helm ou Kustomize pour exécuter la même topologie sur Kubernetes (Minikube ou cluster distant). Cela rapprocherait encore davantage l’environnement local des environnements de production potentiels.

## 7. Conclusion
L’infrastructure Docker fournie offre un environnement riche, cohérent et proche de la production pour développer la solution de prédiction de retards. Son adoption simplifie l’itération sur les pipelines, la collecte de métriques et le suivi MLflow. Les limitations identifiées (consommation mémoire, absence de sécurité, observabilité partielle) sont typiques d’un cluster local et peuvent être levées progressivement grâce aux pistes décrites. Ce rapport documente l’état actuel et sert de base pour les prochaines évolutions vers des déploiements plus robustes ou distribués.
