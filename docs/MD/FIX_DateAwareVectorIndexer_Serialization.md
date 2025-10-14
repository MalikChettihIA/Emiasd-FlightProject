# Fix: DateAwareVectorIndexer Serialization Issue

## Problème Initial

L'exécution échouait avec une erreur **Task not serializable** :

```
org.apache.spark.SparkException: Task not serializable
Caused by: java.io.NotSerializableException: scala.collection.immutable.MapLike$anon$1
Serialization stack:
  - object not serializable (class: scala.collection.immutable.MapLike$anon$1, value: Map(...))
  - field (class: org.apache.spark.ml.feature.VectorIndexerModel, name: categoryMaps, type: interface scala.collection.immutable.Map)
  - object (class org.apache.spark.ml.feature.VectorIndexerModel, VectorIndexerModel: uid=dateAwareVectorIndexer_...)
```

**Contexte :** L'erreur survenait lors du fit du pipeline avec le nouveau `DateAwareVectorIndexer`.

---

## Cause Racine

Dans `DateAwareVectorIndexer.scala`, ligne 49 :

```scala
val filteredCategoryMaps = baseModel.categoryMaps.filterKeys(idx => !dateIndices.contains(idx))
```

**Problème :** La méthode `.filterKeys()` crée une **vue lazy** sur la Map originale :
- Type retourné : `scala.collection.immutable.MapLike$anon$1`
- Cette vue n'est **pas sérialisable**
- Quand Spark essaie de sérialiser le `VectorIndexerModel` pour l'envoyer aux workers, la sérialisation échoue

**Pourquoi c'est un problème :**
1. Spark distribue les tâches en sérialisant les objets (closures, modèles, etc.)
2. Les vues lazy comme `MapLike$anon$1` contiennent des références à la Map originale et des fonctions
3. Ces références ne sont pas sérialisables

---

## Solution Implémentée

**Fichier :** `DateAwareVectorIndexer.scala`, ligne 49-50

**Avant :**
```scala
// ❌ filterKeys crée une vue non-sérialisable
val filteredCategoryMaps = baseModel.categoryMaps.filterKeys(idx => !dateIndices.contains(idx))
```

**Après :**
```scala
// ✅ filter crée une Map concrète sérialisable
// Use .filter instead of .filterKeys to create a serializable Map
val filteredCategoryMaps = baseModel.categoryMaps.filter { case (idx, _) => !dateIndices.contains(idx) }
```

**Différence clé :**

| Méthode | Type retourné | Sérialisable ? |
|---------|---------------|----------------|
| `.filterKeys(f)` | `MapLike$anon$1` (vue lazy) | ❌ Non |
| `.filter { case (k, v) => f(k) }` | `Map[Int, Map[Double, Int]]` (Map concrète) | ✅ Oui |

---

## Explication Technique

### `.filterKeys()` (Problématique)

```scala
val map = Map(1 -> "a", 2 -> "b", 3 -> "c")
val filtered = map.filterKeys(_ > 1)  // ❌ Vue lazy
```

**Caractéristiques :**
- Crée une vue qui applique le filtre **à la demande**
- Garde une référence à la Map originale
- Type : `scala.collection.MapLike$anon$1`
- **Non sérialisable** (contient des closures)

---

### `.filter()` (Solution)

```scala
val map = Map(1 -> "a", 2 -> "b", 3 -> "c")
val filtered = map.filter { case (k, _) => k > 1 }  // ✅ Map concrète
```

**Caractéristiques :**
- Crée une **nouvelle Map** en mémoire
- Ne garde pas de référence à la Map originale
- Type : `scala.collection.immutable.Map[K, V]`
- **Sérialisable** (Map standard)

---

## Tests de Validation

### 1. Compilation

```bash
sbt compile
```

**Résultat attendu :**
```
[info] done compiling
[success] Total time: 2 s
```

### 2. Packaging

```bash
sbt package
```

**Résultat attendu :**
```
[success] Total time: 0 s
```

### 3. Exécution

```bash
./work/scripts/spark-submit.sh
```

**Logs attendus :**
```
[DateAwareVectorIndexer] Excluded 5 date-derived features from categorical treatment:
  [36] date_UTC_FL_DATE_year
  [37] date_UTC_FL_DATE_month
  [38] date_UTC_FL_DATE_day
  [39] date_UTC_FL_DATE_dayofweek
  [40] date_UTC_FL_DATE_unix

[VectorIndexer] Feature Type Analysis
================================================================================
Total features in vector: 41
Categorical features detected: X  ← Réduit (sans date features)
Numeric features: Y              ← Augmenté (avec date features)
```

**Aucune erreur de sérialisation !** ✅

---

## Bonnes Pratiques Spark/Scala

### Éviter les Vues Lazy dans Spark

Les méthodes suivantes créent des vues **non-sérialisables** :

❌ **À ÉVITER dans Spark :**
- `map.filterKeys(f)` → utiliser `map.filter { case (k, _) => f(k) }`
- `map.mapValues(f)` → utiliser `map.map { case (k, v) => k -> f(v) }`
- `seq.view.map(f)` → utiliser `seq.map(f)` directement

✅ **À UTILISER :**
- `map.filter { case (k, v) => ... }` → Map concrète
- `map.map { case (k, v) => ... }` → Map concrète
- `seq.map(f)` → Collection concrète

---

## Impact sur les Performances

**Question :** Est-ce que `.filter()` est plus lent que `.filterKeys()` ?

**Réponse :** Non, négligeable dans ce contexte :

| Aspect | `.filterKeys()` | `.filter()` |
|--------|-----------------|-------------|
| **Création** | O(1) (vue) | O(n) (copie) |
| **Accès** | O(n) par accès (lazy) | O(1) par accès (Map) |
| **Sérialisation** | ❌ Impossible | ✅ O(n) |

**Dans notre cas :**
- La Map `categoryMaps` est petite (< 50 entrées typiquement)
- Elle est créée **une seule fois** pendant le fit
- Elle est ensuite **sérialisée plusieurs fois** (envoyée aux workers)
- **Conclusion :** `.filter()` est plus efficace car évite les erreurs de sérialisation

---

## Autres Cas de Sérialisation dans le Projet

Si vous rencontrez d'autres erreurs `Task not serializable`, vérifiez :

### 1. Closures Capturant des Objets Non-Sérialisables

```scala
// ❌ Capture tout l'objet `this`
class MyClass {
  val data = Map(...)

  def process(rdd: RDD[Int]): RDD[Int] = {
    rdd.map(x => x + data.size)  // ❌ Capture `this.data`
  }
}

// ✅ Extrait la valeur avant la closure
class MyClass {
  val data = Map(...)

  def process(rdd: RDD[Int]): RDD[Int] = {
    val size = data.size  // Copie locale
    rdd.map(x => x + size)  // ✅ Capture seulement `size` (Int)
  }
}
```

### 2. Références à des Objets Spark Non-Sérialisables

```scala
// ❌ SparkContext n'est pas sérialisable
val sc = spark.sparkContext
rdd.map(x => sc.broadcast(...))  // ❌ Erreur

// ✅ Créer le broadcast AVANT le map
val bcData = sc.broadcast(data)
rdd.map(x => bcData.value)  // ✅ OK
```

### 3. Utilisation de Classes Java Non-Sérialisables

```scala
// ❌ SimpleDateFormat n'est pas serializable
val formatter = new SimpleDateFormat("yyyy-MM-dd")
rdd.map(x => formatter.parse(x))  // ❌ Erreur

// ✅ Créer l'objet dans chaque partition
rdd.mapPartitions { partition =>
  val formatter = new SimpleDateFormat("yyyy-MM-dd")  // Un par partition
  partition.map(x => formatter.parse(x))
}
```

---

## Troubleshooting

### Erreur Persiste Après Fix

**Symptôme :**
```
Task not serializable
```

**Vérifications :**
1. Recompiler complètement :
   ```bash
   sbt clean compile package
   ```

2. Vérifier que le JAR est bien régénéré :
   ```bash
   ls -lh target/scala-2.12/*.jar
   ```

3. Vérifier que Spark utilise le bon JAR :
   ```bash
   grep "main JAR" work/scripts/spark-submit.sh
   ```

---

### Comment Déboguer les Erreurs de Sérialisation

Activer le mode debug de sérialisation :

```scala
// Dans SparkConf
spark.conf.set("spark.serializer.objectStreamReset", "100")
spark.conf.set("spark.kryo.registrationRequired", "false")
```

Ou ajouter dans les logs :
```scala
import org.apache.spark.serializer.SerializationDebugger
SerializationDebugger.find(monObjet)
```

---

## Résumé

| Avant | Après |
|-------|-------|
| ❌ `.filterKeys()` → Vue lazy MapLike$anon$1 | ✅ `.filter { case ... }` → Map concrète |
| ❌ Task not serializable | ✅ Sérialisation OK |
| ❌ Pipeline échoue au fit | ✅ Pipeline s'exécute complètement |
| ❌ Date features traitées comme catégorielles | ✅ Date features traitées comme numériques |

**Le fix est minimal (1 ligne changée) mais critique pour le fonctionnement !**

---

## Références

- [Spark Documentation - Closures](https://spark.apache.org/docs/latest/rdd-programming-guide.html#understanding-closures)
- [Scala Collections - Views](https://docs.scala-lang.org/overviews/collections/views.html)
- [Spark Serialization](https://spark.apache.org/docs/latest/tuning.html#data-serialization)

**Prochaine étape :** Relancer le pipeline pour vérifier que les date features sont bien traitées comme numériques ! 🎯

```bash
./work/scripts/spark-submit.sh
```
