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

Rebuild and submit the project on the local cluster:
```
./run-on-docker.sh
```

## Jupyter Server
Jupyter Lab is accessible on http://localhost:8888 once the local cluster is tarted.

## Data 
Data is stored on ./work/data/FLIGHT-3Y. Accessible from Jupyter notebooks and spark jobs in the local spark cluster.

