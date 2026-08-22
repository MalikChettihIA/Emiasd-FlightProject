#!/bin/bash

# Script pour télécharger XGBoost 1.7.6 JARs
# XGBoost 1.7.6 a un meilleur support pour Docker et le mode single-node

echo "📦 Téléchargement des JARs XGBoost 1.7.6..."

cd /Users/malikchettih/Projects/Emiasd-Projects/Emiasd-FlightProject/work/apps

# XGBoost4j core
if [ ! -f "xgboost4j_2.12-1.7.6.jar" ]; then
    echo "Téléchargement de xgboost4j_2.12-1.7.6.jar..."
    curl -L -o xgboost4j_2.12-1.7.6.jar \
        "https://repo1.maven.org/maven2/ml/dmlc/xgboost4j_2.12/1.7.6/xgboost4j_2.12-1.7.6.jar"
    echo "✅ xgboost4j_2.12-1.7.6.jar téléchargé"
else
    echo "✅ xgboost4j_2.12-1.7.6.jar déjà présent"
fi

# XGBoost4j-Spark
if [ ! -f "xgboost4j-spark_2.12-1.7.6.jar" ]; then
    echo "Téléchargement de xgboost4j-spark_2.12-1.7.6.jar..."
    curl -L -o xgboost4j-spark_2.12-1.7.6.jar \
        "https://repo1.maven.org/maven2/ml/dmlc/xgboost4j-spark_2.12/1.7.6/xgboost4j-spark_2.12-1.7.6.jar"
    echo "✅ xgboost4j-spark_2.12-1.7.6.jar téléchargé"
else
    echo "✅ xgboost4j-spark_2.12-1.7.6.jar déjà présent"
fi

echo ""
echo "📋 JARs XGBoost dans work/apps/:"
ls -lh xgboost*.jar

echo ""
echo "✅ Téléchargement terminé!"
echo ""
echo "Prochaines étapes:"
echo "1. Compiler le projet: sbt package"
echo "2. Redémarrer Docker: cd docker && docker-compose -f docker-compose-2w.yml restart"
echo "3. Tester XGBoost: docker exec -it spark-submit bash -c 'cd /scripts && ./spark-submit.sh 2w train local-md-xgb-d2_60_7_7'"
