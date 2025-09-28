# Prédiction des Retards de Vols basée sur les Conditions Météorologiques

## Introduction

Ce projet implémente un système de prédiction des retards de vols en utilisant Apache Spark et Scala, s'inspirant de l'article 
académique "Using Scalable Data Mining for Predicting Flight Delays" (ACM TIST, 2016) 
https://www.dropbox.com/s/4rqnjueuqi5e0uo/TIST-Flight-Delay-final.pdf. 

L'objectif est de développer un modèle de Machine Learning capable de prédire avec précision les retards de vols causés 
par les conditions météorologiques, en analysant à la fois les données historiques de vols et les observations météorologiques 
aux aéroports d'origine et de destination.

Le système traite de manière scalable des datasets complexes en effectuant des jointures sophistiquées entre les données de 
vols et les conditions météorologiques, en considérant plusieurs observations météorologiques jusqu'à 12 heures avant le 
départ programmé. L'approche utilise Spark ML pour implémenter des algorithmes de classification (Random Forest et Decision Trees) 
optimisés pour traiter des volumes importants de données en parallèle.

L'objectif de performance visé est d'atteindre une précision de 85.8% et un recall de 86.9% pour la prédiction des retards 
de plus de 60 minutes, reproduisant ainsi les résultats de l'étude de référence. Cette solution pourrait être intégrée 
dans des systèmes de recommandation pour les passagers, les compagnies aériennes et les plateformes de réservation de vols, 
permettant une meilleure gestion du trafic aérien et une optimisation des plannings.

## Datasets

Le projet utilise trois datasets principaux (https://www.dropbox.com/sh/iasq7frk6f58ptq/AAAzSmk6cusSNfqYNYsnLGIXa):

- **Flights_samples.csv** : Données de vols avec informations sur les retards (1352 vols)
- **Weather_samples.csv** : Observations météorologiques horaires détaillées (80 observations, 44 variables)
- **wban_airport_timezone.csv** : Mapping entre aéroports et stations météorologiques (305 aéroports)

## Technologies

- **Scala 2.12.18** : Langage de programmation principal
- **Apache Spark** : Framework de traitement Big Data
- **Spark ML** : Bibliothèque de Machine Learning pour la modélisation
- **MapReduce** : Paradigme pour le traitement parallèle des données

## Table des Matières

- [Installation](docs/MD/1.installation_guide.md) - Guide complet d'installation et de configuration
- [Architecture du Projet](doc/MD/2.Architecture_projet.md)
- [Utilisation](#utilisation)
- [Résultats](#résultats)
- [Contribution](#contribution)

## Démarrage Rapide

Pour commencer rapidement avec le projet :

1. **📋 Prérequis** : Vérifiez que vous avez Scala 2.12.18, Java 17.0.13 et Spark 3.5.5
2. **⚙️ Installation** : Suivez le [guide d'installation détaillé](INSTALLATION.md)
3. **🚀 Exécution** : Lancez le cluster local et exécutez l'analyse

```bash
# Démarrage rapide pour l'environnement local
./start-local-cluster.sh
./run-on-docker.sh
```

> 📖 **Guide Complet** : Pour une installation détaillée sur cluster local ou Lamsade, consultez [INSTALLATION.md](INSTALLATION.md)