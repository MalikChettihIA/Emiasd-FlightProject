# Emiasd-FlightProject

## Env
- Scala is 2.12.18
- Java is 17.0.13
- Spark is 3.5.5

## Env Installation

Open a command windows and change directory to the root directory of then projetc.
Make sure that scripts are executable. 
```
su chmod +x ./docker/setup.sh
su chmod +x ./start-local-cluster.sh
su chmod +x ./stop-local-cluster.sh
su chmod +x ./run-on-docker.sh
```

Build local Spark cluster using Docker
```
cd docker
./setup.sh
```

Start and stop Cluster with:
```
./start-local-cluster.sh
./stop-local-cluster.sh
```
## Build and Run

### On Local Docker Cluster
Rebuild and submit the project on the local cluster:
```
./run-on-docker.sh
```

###  On Lamsade Cluster
Avant tout copier votre fichier **id_ed25519_mchettih.key** à la racine du répertoire du projet.

Dans un premier temps, envoyer les fichiers data et jar sur le serveur.
Executer les commandes ci-dessous depuis le poste de développement.
```
sbt package
scp -P 5022 -i id_ed25519_mchettih.key ./work/data/FLIGHT-3Y.zip mchettih@ssh.lamsade.dauphine.fr:~/workspace
scp -P 5022 -i id_ed25519_mchettih.key ./work/apps/Emiasd-Flight-Data-Analysis.jar mchettih@ssh.lamsade.dauphine.fr:~/workspace
```

Se connecter au serveur en ssh pour dézipper et copier les donner dans le répertoire data 
```
ssh -p 5022 -i id_ed25519_mchettih.key mchettih@ssh.lamsade.dauphine.fr
```
Puis dans la session ssh sur le cluster lamsade
```
cp FLIGHT-3Y.zip ./data/
cd ./data
unzip FLIGHT-3Y.zip
rm FLIGHT-3Y.zip

hdfs dfs -mkdir -p /students/p6emiasd2025/mchettih/data/FLIGHT-3Y
hdfs dfs -mkdir -p /students/p6emiasd2025/mchettih/data/FLIGHT-3Y/Flights
hdfs dfs -mkdir -p /students/p6emiasd2025/mchettih/data/FLIGHT-3Y/Weather

hdfs dfs -put data/FLIGHT-3Y/wban_airport_timezone.csv /students/p6emiasd2025/mchettih/data/FLIGHT-3Y
hdfs dfs -put data/FLIGHT-3Y/Flights/* /students/p6emiasd2025/mchettih/data/FLIGHT-3Y/Flights
hdfs dfs -put data/FLIGHT-3Y/Weather/* /students/p6emiasd2025/mchettih/data/FLIGHT-3Y/Weather

```


Pour executer l'application faire depuis la racine 
```
cd /opt/cephfs/users/students/p6emiasd2025/mchettih/workspace

spark-submit \
  --deploy-mode client \
  --class com.flightdelay.app.FlightDelayPredictionApp \
  --executor-cores 4 \
  --executor-memory 2G \
  --num-executors 1 \
  Emiasd-Flight-Data-Analysis.jar \
  lamsade
  
```


## Jupyter Server
Jupyter Lab is accessible on http://localhost:8888 once the local cluster is tarted.

## Data 
Data is stored on ./work/data/FLIGHT-3Y. Accessible from Jupyter notebooks and spark jobs in the local spark cluster.

