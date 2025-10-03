# BasicAutoPipeline - Documentation Complète

## Vue d'ensemble

`BasicAutoPipeline` est une classe Scala qui automatise la création d'un pipeline de prétraitement Spark ML pour préparer des données avant l'entraînement d'un modèle de Machine Learning. Elle enchaîne trois transformations essentielles en un seul appel.

### Pipeline de transformations

```
Données brutes
      ↓
[1] StringIndexer → Encode les catégories en indices
      ↓
[2] VectorAssembler → Assemble toutes les features en un vecteur
      ↓
[3] VectorIndexer → Détecte et indexe les features catégorielles
      ↓
DataFrame prêt pour ML (features, label)
```

---

## Signature de la classe

```scala
package com.flightdelay.features.pipelines

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class BasicAutoPipeline(
  textCols: Array[String],      // Colonnes catégorielles (texte)
  numericCols: Array[String],   // Colonnes numériques
  target: String,               // Colonne cible
  maxCat: Int,                  // Nombre max de catégories pour VectorIndexer
  handleInvalid: String         // "error", "skip", ou "keep"
)
```

---

## Paramètres

| Paramètre | Type | Description | Exemple |
|-----------|------|-------------|---------|
| `textCols` | `Array[String]` | Colonnes catégorielles (texte) à indexer | `Array("carrier", "origin", "dest")` |
| `numericCols` | `Array[String]` | Colonnes numériques à inclure dans les features | `Array("dep_delay", "air_time")` |
| `target` | `String` | Nom de la colonne cible (sera renommée en "label") | `"flight_status"` |
| `maxCat` | `Int` | Nombre max de catégories pour VectorIndexer | `50` |
| `handleInvalid` | `String` | Gestion des valeurs invalides | `"keep"`, `"skip"`, ou `"error"` |

### Options pour `handleInvalid`

- **`"error"`** : Lève une exception si une valeur invalide est rencontrée
- **`"skip"`** : Ignore les lignes contenant des valeurs invalides
- **`"keep"`** : Conserve les valeurs invalides et leur assigne un index spécial

---

## Sorties

Le DataFrame transformé contient **uniquement 2 colonnes** :

| Colonne | Type | Description |
|---------|------|-------------|
| `features` | Vector | Vecteur Dense/Sparse contenant toutes les features encodées et assemblées |
| `label` | Double | Cible encodée en Double (0.0, 1.0, 2.0, etc.) |

---

## Exemple complet : Prédiction des retards de vols

### 1. Données d'entrée

```scala
import org.apache.spark.sql.SparkSession
import com.flightdelay.features.pipelines.BasicFlightFeaturePipeline

val spark = SparkSession.builder()
  .appName("BasicAutoPipeline Example")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Données de vols avec retards
val flightData = Seq(
  ("AA", "ORD", "LAX", 120, 15, 240, "OnTime"),
  ("DL", "JFK", "SFO", 150, 20, 300, "Delayed"),
  ("UA", "ORD", "DEN", 90, 10, 150, "OnTime"),
  ("AA", "LAX", "JFK", 300, 25, 320, "Delayed"),
  ("DL", "SFO", "ORD", 200, 18, 240, "OnTime"),
  ("UA", "DEN", "LAX", 110, 12, 130, "Delayed"),
  ("AA", "ORD", "SFO", 180, 22, 200, "Delayed"),
  ("DL", "JFK", "ORD", 130, 16, 160, "OnTime")
).toDF(
  "carrier", // Catégoriel: compagnie aérienne
  "origin", // Catégoriel: aéroport de départ
  "dest", // Catégoriel: aéroport d'arrivée
  "dep_delay", // Numérique: retard au départ (minutes)
  "taxi_out", // Numérique: temps de roulage (minutes)
  "air_time", // Numérique: temps de vol (minutes)
  "flight_status" // Cible: statut du vol
)

flightData.show(false)
```

**Sortie :**
```
+-------+------+----+---------+--------+--------+-------------+
|carrier|origin|dest|dep_delay|taxi_out|air_time|flight_status|
+-------+------+----+---------+--------+--------+-------------+
|AA     |ORD   |LAX |120      |15      |240     |OnTime       |
|DL     |JFK   |SFO |150      |20      |300     |Delayed      |
|UA     |ORD   |DEN |90       |10      |150     |OnTime       |
|AA     |LAX   |JFK |300      |25      |320     |Delayed      |
|DL     |SFO   |ORD |200      |18      |240     |OnTime       |
|UA     |DEN   |LAX |110      |12      |130     |Delayed      |
|AA     |ORD   |SFO |180      |22      |200     |Delayed      |
|DL     |JFK   |ORD |130      |16      |160     |OnTime       |
+-------+------+----+---------+--------+--------+-------------+
```

### 2. Application du pipeline

```scala
// Définir les colonnes
val textCols = Array("carrier", "origin", "dest")
val numericCols = Array("dep_delay", "taxi_out", "air_time")
val target = "flight_status"

// Créer et appliquer le pipeline
val autoPipeline = new BasicAutoPipeline(
  textCols = textCols,
  numericCols = numericCols,
  target = target,
  maxCat = 50,
  handleInvalid = "keep"
)

val transformedData = autoPipeline.fit(flightData)

transformedData.show(false)
```

**Sortie :**
```
+----------------------------------------+-----+
|features                                |label|
+----------------------------------------+-----+
|[0.0,0.0,0.0,120.0,15.0,240.0]         |0.0  |
|[1.0,1.0,1.0,150.0,20.0,300.0]         |1.0  |
|[2.0,0.0,2.0,90.0,10.0,150.0]          |0.0  |
|[0.0,0.0,1.0,300.0,25.0,320.0]         |1.0  |
|[1.0,1.0,0.0,200.0,18.0,240.0]         |0.0  |
|[2.0,2.0,0.0,110.0,12.0,130.0]         |1.0  |
|[0.0,0.0,1.0,180.0,22.0,200.0]         |1.0  |
|[1.0,1.0,0.0,130.0,16.0,160.0]         |0.0  |
+----------------------------------------+-----+
```

### 3. Explication des transformations

#### Étape 1 : StringIndexer

Encode les colonnes catégorielles en indices numériques :

```
carrier:        AA → 0.0, DL → 1.0, UA → 2.0
origin:         ORD → 0.0, JFK → 1.0, SFO → 1.0, LAX → 0.0, DEN → 2.0
dest:           LAX → 0.0, SFO → 1.0, DEN → 2.0, JFK → 1.0, ORD → 0.0
flight_status:  OnTime → 0.0, Delayed → 1.0
```

**Note :** Les indices sont assignés par fréquence (la valeur la plus fréquente = 0.0)

#### Étape 2 : VectorAssembler

Assemble toutes les features en un seul vecteur :

```
Ordre des features dans le vecteur :
[indexed_carrier, indexed_origin, indexed_dest, dep_delay, taxi_out, air_time]

Exemple de première ligne :
[0.0, 0.0, 0.0, 120.0, 15.0, 240.0]
```

#### Étape 3 : VectorIndexer

Détecte automatiquement les features catégorielles (≤ maxCat valeurs distinctes) :
- Les 3 premières features sont identifiées comme catégorielles
- Les 3 dernières restent continues

---

## Utilisation avec un modèle ML

### Exemple 1 : Random Forest

```scala
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

// Split train/test
val Array(trainData, testData) = transformedData.randomSplit(Array(0.8, 0.2), seed = 42)

// Entraîner le modèle
val rf = new RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setNumTrees(100)
  .setMaxDepth(10)

val model = rf.fit(trainData)

// Prédictions
val predictions = model.transform(testData)

predictions.select("features", "label", "prediction", "probability").show()

// Évaluation
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)
println(f"Accuracy: ${accuracy * 100}%.2f%%")
```

### Exemple 2 : Logistic Regression

```scala
import org.apache.spark.ml.classification.LogisticRegression

val lr = new LogisticRegression()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

val lrModel = lr.fit(trainData)
val lrPredictions = lrModel.transform(testData)
```

---

## Code source complet

```scala
package com.flightdelay.features.pipelines

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * BasicAutoPipeline
 *
 * Construit automatiquement un pipeline de prétraitement combinant :
 *  - StringIndexer : encodage des colonnes catégorielles et de la cible
 *  - VectorAssembler : assemblage des features en un vecteur
 *  - VectorIndexer : détection des features discrètes
 *
 * @param textCols      Colonnes catégorielles à indexer
 * @param numericCols   Colonnes numériques
 * @param target        Colonne cible
 * @param maxCat        Nombre max de catégories pour VectorIndexer
 * @param handleInvalid Gestion des valeurs invalides ("error", "skip", "keep")
 */
class BasicAutoPipeline(
  textCols: Array[String],
  numericCols: Array[String],
  target: String,
  maxCat: Int,
  handleInvalid: String
) {

  // Noms de colonnes internes
  private val _label = "label"
  private val _prefix = "indexed_"
  private val _featuresVec = "featuresVec"
  private val _featuresVecIndex = "features"

  // StringIndexer
  private val inAttsNames = textCols ++ Array(target)
  private val outAttsNames = inAttsNames.map(_prefix + _)

  private val stringIndexer = new StringIndexer()
    .setInputCols(inAttsNames)
    .setOutputCols(outAttsNames)
    .setHandleInvalid(handleInvalid)

  private val features = outAttsNames.filterNot(_.contains(target)) ++ numericCols

  // VectorAssembler
  private val vectorAssembler = new VectorAssembler()
    .setInputCols(features)
    .setOutputCol(_featuresVec)
    .setHandleInvalid(handleInvalid)

  // VectorIndexer
  private val vectorIndexer = new VectorIndexer()
    .setInputCol(_featuresVec)
    .setOutputCol(_featuresVecIndex)
    .setMaxCategories(maxCat)
    .setHandleInvalid(handleInvalid)

  /**
   * Applique le pipeline sur les données
   * @param data DataFrame d'entrée
   * @return DataFrame transformé avec colonnes (features, label)
   */
  def fit(data: DataFrame): DataFrame = {
    val pipeline = new Pipeline()
      .setStages(Array(stringIndexer, vectorAssembler, vectorIndexer))

    pipeline.fit(data).transform(data)
      .select(col(_featuresVecIndex), col(_prefix + target))
      .withColumnRenamed(_prefix + target, _label)
  }
}
```

---

## Avantages

1. **Automatisation complète** : Plus besoin de créer manuellement chaque StringIndexer
2. **Code concis** : Une seule ligne pour préparer toutes les features
3. **Prêt pour ML** : Sortie directement compatible avec tous les algorithmes Spark ML
4. **Gestion robuste des erreurs** : Paramètre `handleInvalid` pour gérer les valeurs manquantes
5. **Détection automatique** : VectorIndexer identifie automatiquement les features catégorielles

---

## Limitations

1. **Pas de récupération du PipelineModel** : La méthode `fit()` retourne le DataFrame, pas le modèle
2. **Pas de persistence** : Impossible de sauvegarder le pipeline pour réutilisation
3. **Pas d'accès aux métadonnées** : Les mappings (ex: "AA" → 0.0) ne sont pas exposés
4. **Transformation unique** : Le pipeline est recréé à chaque appel de `fit()`

---

## Amélioration suggérée pour la production

Pour pouvoir sauvegarder et réutiliser le pipeline :

```scala
class BasicAutoPipelineV2(
  textCols: Array[String],
  numericCols: Array[String],
  target: String,
  maxCat: Int,
  handleInvalid: String
) {
  
  // ... (même code pour les transformers)
  
  /**
   * Entraîne le pipeline et retourne le modèle
   */
  def fitPipeline(data: DataFrame): PipelineModel = {
    val pipeline = new Pipeline()
      .setStages(Array(stringIndexer, vectorAssembler, vectorIndexer))
    
    pipeline.fit(data)
  }
  
  /**
   * Transforme les données avec un modèle pré-entraîné
   */
  def transform(data: DataFrame, model: PipelineModel): DataFrame = {
    model.transform(data)
      .select(col(_featuresVecIndex), col(_prefix + target))
      .withColumnRenamed(_prefix + target, _label)
  }
  
  /**
   * Sauvegarde le modèle
   */
  def save(model: PipelineModel, path: String): Unit = {
    model.write.overwrite().save(path)
  }
  
  /**
   * Charge un modèle sauvegardé
   */
  def load(path: String): PipelineModel = {
    PipelineModel.load(path)
  }
}

// Utilisation en production
val pipeline = new BasicAutoPipelineV2(textCols, numericCols, target, 50, "keep")

// Entraînement
val model = pipeline.fitPipeline(trainData)

// Sauvegarde
pipeline.save(model, "/path/to/model")

// Chargement et utilisation
val loadedModel = pipeline.load("/path/to/model")
val transformed = pipeline.transform(newData, loadedModel)
```

---

## Cas d'usage typiques

### 1. Prototypage rapide

```scala
val pipeline = new BasicAutoPipeline(textCols, numericCols, target, 50, "keep")
val data = pipeline.fit(rawData)
// Tester rapidement plusieurs modèles ML
```

### 2. Exploration de données

```scala
val transformed = pipeline.fit(explorationData)
transformed.describe().show()
transformed.groupBy("label").count().show()
```

### 3. Comparaison de modèles

```scala
val prepared = pipeline.fit(data)
val Array(train, test) = prepared.randomSplit(Array(0.8, 0.2))

// Tester rapidement plusieurs algorithmes
val rfAccuracy = trainAndEvaluate(new RandomForestClassifier(), train, test)
val lrAccuracy = trainAndEvaluate(new LogisticRegression(), train, test)
val gbAccuracy = trainAndEvaluate(new GBTClassifier(), train, test)
```

---

## Conclusion

`BasicAutoPipeline` est une classe très utile pour :
- Le **prototypage rapide** de modèles ML
- L'**exploration** de données
- Les **expérimentations** avec différents algorithmes

Pour la **production**, il est recommandé de créer un pipeline complet avec :
- Sauvegarde et chargement du modèle
- Gestion des métadonnées (mappings des catégories)
- Logs et monitoring
- Validation des données

---

## Ressources

- [Spark ML Pipeline Documentation](https://spark.apache.org/docs/latest/ml-pipeline.html)
- [StringIndexer](https://spark.apache.org/docs/latest/ml-features.html#stringindexer)
- [VectorAssembler](https://spark.apache.org/docs/latest/ml-features.html#vectorassembler)
- [VectorIndexer](https://spark.apache.org/docs/latest/ml-features.html#vectorindexer)

---

**Version :** 1.0  
**Date :** 2025-10-02  
**Auteur :** Documentation générée pour le projet Flight Delay Prediction
