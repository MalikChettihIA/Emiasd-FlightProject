# Solutions pour le problème maxBins

## Problème
```
DecisionTree requires maxBins (= 128) to be at least as large as the number of values
in each categorical feature, but categorical feature 1 has 234 values.
```

## Identification de la feature problématique

Après le prochain run, votre code affichera automatiquement:
```
[ConfigurationBasedPipeline] Categorical Features Cardinality:
  [ 0] OP_CARRIER_AIRLINE_ID                      :     23 distinct values ✓
  [ 1] ORIGIN_AIRPORT_ID                          :    234 distinct values ⚠ EXCEEDS maxBins=128
  [ 2] DEST_AIRPORT_ID                            :    234 distinct values ⚠ EXCEEDS maxBins=128
  [ 3] feature_departure_hour_rounded             :     24 distinct values ✓
  ...
```

## Solutions

### Solution 1: Augmenter maxBins (✅ Recommandé pour votre cas)

Modifiez `local-config.yml`:

```yaml
hyperparameters:
  numTrees: [20]
  maxDepth: [7]
  maxBins: [256]  # ← Augmenté à 256 (ou 300 pour être sûr)
  minInstancesPerNode: [5]
```

**Avantages:**
- Simple et rapide
- Préserve toute l'information
- Pas de perte de précision

**Inconvénients:**
- Augmente légèrement le temps d'entraînement
- Augmente la mémoire utilisée

**Recommandation:**
- Pour 234 valeurs distinctes: `maxBins: [256]` ou `maxBins: [300]`
- Testez avec `maxBins: [512]` si vous avez assez de mémoire

---

### Solution 2: Retirer les features problématiques

Dans `local-config.yml`, commentez les features avec trop de valeurs:

```yaml
flightSelectedFeatures:
  # ORIGIN_AIRPORT_ID:
  #   transformation: "StringIndexer"
  # DEST_AIRPORT_ID:
  #   transformation: "StringIndexer"
```

**Quand utiliser:**
- Si ces features ne sont pas cruciales pour votre modèle
- Si vous voulez un modèle plus simple/rapide

**Inconvénients:**
- Perte d'information potentiellement importante
- Les aéroports d'origine/destination peuvent être très prédictifs

---

### Solution 3: Feature Hashing (Alternative avancée)

Remplacez `StringIndexer` par `FeatureHasher` pour les features haute cardinalité.

**Modifiez `ConfigurationBasedFeatureExtractorPipeline.scala`:**

```scala
// Au lieu de StringIndexer, utilisez FeatureHasher pour les features > 128 valeurs
val hasherFeatures = Seq("ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID")

if (hasherFeatures.nonEmpty) {
  val hasher = new FeatureHasher()
    .setInputCols(hasherFeatures)
    .setOutputCol("hashed_airports")
    .setNumFeatures(128)  // Nombre de bins pour le hashing
  stages = stages :+ hasher
}
```

**Avantages:**
- Gère n'importe quelle cardinalité
- Taille fixe du vecteur de features

**Inconvénients:**
- Collisions possibles (plusieurs valeurs → même hash)
- Perte d'interprétabilité

---

### Solution 4: Regroupement des aéroports (Feature Engineering)

Créez des groupes d'aéroports par région/taille:

```scala
// Exemple: Regrouper par région géographique
val airportRegionMapping = Map(
  "JFK" -> "EAST_MAJOR",
  "LAX" -> "WEST_MAJOR",
  "ORD" -> "MIDWEST_MAJOR",
  "ATL" -> "SOUTH_MAJOR",
  // ... autres mappings
  "DEFAULT" -> "OTHER"
)

val grouped = df.withColumn(
  "ORIGIN_REGION",
  coalesce(
    lit(airportRegionMapping.getOrElse(col("ORIGIN_AIRPORT_ID"), "OTHER")),
    lit("OTHER")
  )
)
```

**Avantages:**
- Réduit drastiquement la cardinalité (234 → 10-20 régions)
- Capture les patterns géographiques
- Améliore potentiellement la généralisation

**Inconvénients:**
- Nécessite du travail de feature engineering
- Perte de granularité

---

## Recommandations par ordre de priorité

### Pour votre cas spécifique:

1. **Court terme (✅ Déjà fait?)**:
   ```yaml
   maxBins: [256]  # ou [300] pour être large
   ```

2. **Si mémoire insuffisante**:
   - Essayez `maxBins: [256]` d'abord
   - Si ça ne marche pas, passez à la Solution 4 (regroupement par région)

3. **Pour optimiser les performances**:
   - Analysez l'importance des features après entraînement
   - Si `ORIGIN_AIRPORT_ID` et `DEST_AIRPORT_ID` sont peu importantes, retirez-les
   - Sinon, gardez-les avec maxBins=256

---

## Test de performance par maxBins

Voici les valeurs typiques:

| maxBins | Mémoire  | Temps entraînement | Précision |
|---------|----------|-------------------|-----------|
| 128     | Baseline | 1x                | Baseline  |
| 256     | +20%     | +15%              | +0-2%     |
| 512     | +40%     | +30%              | +0-3%     |

---

## Vérification après modification

Après avoir changé `maxBins`, vous verrez dans les logs:

```
[ConfigurationBasedPipeline] Categorical Features Cardinality:
  [ 0] OP_CARRIER_AIRLINE_ID              :     23 distinct values ✓
  [ 1] ORIGIN_AIRPORT_ID                  :    234 distinct values ✓ (maxBins=256)
  [ 2] DEST_AIRPORT_ID                    :    234 distinct values ✓ (maxBins=256)
```

Plus d'erreur! 🎉
