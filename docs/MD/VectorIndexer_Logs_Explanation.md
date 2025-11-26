# Logs VectorIndexer - Explication Détaillée

## Nouveaux Logs Ajoutés

Après la compilation et l'exécution, vous verrez maintenant des logs détaillés montrant comment **VectorIndexer** classifie chaque feature.

---

## Format des Logs

### 1. Vue d'Ensemble

```
================================================================================
[VectorIndexer] Feature Type Analysis
================================================================================
Total features in vector: 138
Categorical features detected: 18
Numeric features: 120
Max categories threshold: 50
================================================================================
```

**Signification:**
- **Total features:** 138 features dans le vecteur assemblé
- **Categorical:** 18 features traitées comme catégorielles (≤ 50 valeurs distinctes)
- **Numeric:** 120 features traitées comme numériques (> 50 valeurs ou continues)
- **Threshold:** Seuil configuré dans `local-config.yml`

---

### 2. Features Catégorielles (Détail)

```
[Categorical Features] (treated as discrete values):
--------------------------------------------------------------------------------
  [  0] indexed_DEST_WBAN                               →  47 categories
  [  1] indexed_ORIGIN_WBAN                             →  47 categories
  [  2] indexed_DEST_AIRPORT_ID                         → 305 categories  ← ERREUR!
  [  3] indexed_OP_CARRIER_AIRLINE_ID                   →  14 categories
  [  4] indexed_OP_CARRIER_FL_NUM                       →  50 categories
  [  5] indexed_ORIGIN_AIRPORT_ID                       → 305 categories  ← ERREUR!
  [ 96] origin_weather_feature_is_vfr_conditions-11     →   2 categories
  [ 97] origin_weather_feature_is_ifr_conditions-11     →   2 categories
  ...
```

**Colonnes expliquées:**
- **[Index]** : Position dans le vecteur de features
- **Feature Name** : Nom de la feature
- **→ N categories** : Nombre de valeurs distinctes détectées

**⚠️ ATTENTION:** Si vous voyez `→ 305 categories` ou plus, c'est que `maxCat` est trop élevé !

---

### 3. Features Numériques (Résumé)

```
[Numeric Features] (treated as continuous values):
--------------------------------------------------------------------------------
  Showing first 5 and last 5 of 120 numeric features:
  [  6] origin_weather_feature_weather_severity_index-11
  [  7] origin_weather_feature_operations_risk_level-11
  [  8] origin_weather_feature_weather_severity_index-10
  [  9] origin_weather_feature_operations_risk_level-10
  [ 10] origin_weather_feature_weather_severity_index-9
  ... (110 more)
  [133] date_UTC_FL_DATE_dayofweek
  [134] date_UTC_FL_DATE_unix
  [135] date_FL_DATE_year
  [136] date_FL_DATE_month
  [137] date_FL_DATE_day
```

**Signification:**
- Ces features ont **> 50 valeurs distinctes** ou sont continues
- Random Forest les traitera avec des seuils numériques (ex: `feature <= 0.5`)

---

## Comment Interpréter les Résultats

### ✅ Bon Comportement

```
[  3] indexed_OP_CARRIER_AIRLINE_ID                   →  14 categories  ✅
[ 96] origin_weather_feature_is_vfr_conditions-11     →   2 categories  ✅
[ 97] date_FL_DATE_day                                →  31 categories  ✅
```

**Pourquoi c'est bon:**
- Peu de catégories (< 50)
- Sémantiquement, ce sont des valeurs discrètes (compagnies, booléens, jours du mois)
- Random Forest peut les traiter efficacement

---

### ⚠️ Comportement Suspect

```
[  2] indexed_DEST_AIRPORT_ID                         → 305 categories  ⚠️
[  5] indexed_ORIGIN_AIRPORT_ID                       → 305 categories  ⚠️
```

**Problème:**
- Trop de catégories (305 > 50 mais détecté quand même?)
- **Cause possible:** Le seuil `maxCat=50` n'est pas respecté par VectorIndexer ?
- **Impact:** Augmente la mémoire et le temps d'entraînement

**Solution:**
1. Vérifier pourquoi VectorIndexer détecte 305 catégories alors que `maxCat=50`
2. Possibilité: StringIndexer a déjà encodé ces colonnes, donc VectorIndexer voit 305 valeurs numériques distinctes

---

### ❌ Comportement Problématique

```
[  4] OP_CARRIER_FL_NUM                               → 3000 categories  ❌
```

**Problème:**
- Numéro de vol avec des milliers de valeurs distinctes
- Si traité comme catégoriel → explosion mémoire

**Solution:**
- Cette feature devrait être **exclue** ou **transformée** (ex: modulo 100)
- Ne devrait JAMAIS être catégorielle avec autant de valeurs

---

## Exemples Réels de Votre Dataset

### Features qui DEVRAIENT être Catégorielles

| Feature | Valeurs Attendues | Raison |
|---------|------------------|--------|
| `indexed_OP_CARRIER_AIRLINE_ID` | ~15 | ID de compagnie aérienne |
| `indexed_DEST_WBAN` | ~50 | Stations météo destinations |
| `indexed_ORIGIN_WBAN` | ~50 | Stations météo origines |
| `date_FL_DATE_month` | 12 | Mois de l'année |
| `date_FL_DATE_day` | 31 | Jour du mois |
| `origin_weather_feature_is_vfr_conditions-*` | 2 | Booléen (0/1) |

### Features qui DEVRAIENT être Numériques

| Feature | Raison |
|---------|--------|
| `origin_weather_feature_weather_severity_index-*` | Score continu 0-100 |
| `origin_weather_feature_operations_risk_level-*` | Score continu |
| `date_FL_DATE_unix` | Timestamp Unix (millions de valeurs) |
| `date_FL_DATE_year` | Année (continu) |

---

## Actions selon les Logs

### Si vous voyez trop de features catégorielles (> 30):

**Symptôme:**
```
Categorical features detected: 85
```

**Action:**
```yaml
# local-config.yml
maxCategoricalCardinality: 20  # Réduire de 50 → 20
```

---

### Si vous voyez trop de catégories par feature (> 100):

**Symptôme:**
```
[  2] indexed_DEST_AIRPORT_ID    → 305 categories
```

**Action:**
1. Vérifier que StringIndexer ne crée pas trop de valeurs
2. Envisager de grouper les aéroports par région/hub
3. Ou exclure cette feature si peu importante

---

### Si aucune feature n'est catégorielle:

**Symptôme:**
```
⚠ No categorical features detected - all features treated as numeric
```

**Action:**
```yaml
# local-config.yml
maxCategoricalCardinality: 100  # Augmenter de 50 → 100
```

---

## Impact sur Random Forest

### Features Catégorielles (ex: AIRLINE_ID = 14 catégories)

**Avant VectorIndexer:**
```
AIRLINE_ID brut: [19393, 19805, 20436, ...]
```

**Après StringIndexer:**
```
indexed_AIRLINE_ID: [0, 1, 2, 3, ..., 13]  (14 valeurs)
```

**Après VectorIndexer:**
```
VectorIndexer détecte: 14 valeurs distinctes ≤ 50 → CATÉGORIELLE
```

**Random Forest peut faire:**
```scala
if (AIRLINE_ID in {0, 3, 7, 11}) → Gauche  // Groupes de compagnies
else → Droite
```

### Features Numériques (ex: weather_severity_index)

**Random Forest fait:**
```scala
if (weather_severity_index <= 32.5) → Gauche  // Seuil numérique
else → Droite
```

---

## Commandes Utiles

### Recompiler après modification:
```bash
sbt package
```

### Exécuter et capturer les logs:
```bash
./local-submit.sh 2>&1 | tee execution.log
```

### Rechercher les logs VectorIndexer:
```bash
grep -A 50 "VectorIndexer.*Feature Type Analysis" execution.log
```

### Compter les features catégorielles:
```bash
grep "→.*categories" execution.log | wc -l
```

---

## Checklist de Vérification

Après avoir vu les nouveaux logs, vérifiez:

- [ ] Nombre de features catégorielles raisonnable (< 30)
- [ ] Aucune feature avec > 100 catégories
- [ ] Les IDs (AIRLINE_ID, WBAN) sont catégoriels
- [ ] Les scores continus (severity_index) sont numériques
- [ ] Les dates dérivées (day, month) sont catégorielles
- [ ] Les timestamps Unix sont numériques
- [ ] Pas de warnings mémoire dans Spark

---

## Optimisation selon les Logs

### Si Task Binary Size > 5 MB:

**Symptôme dans les logs:**
```
WARN DAGScheduler: Broadcasting large task binary with size 14.7 MiB
```

**Cause probable:** Trop de features catégorielles

**Action:**
1. Réduire `maxCategoricalCardinality` à 30
2. Ou exclure les features avec > 50 catégories

### Si Entraînement très lent (> 2 min par fold):

**Cause probable:** Features catégorielles avec trop de valeurs

**Action:**
1. Vérifier les features avec > 50 catégories dans les logs
2. Les passer en numérique ou les exclure

---

## Conclusion

Ces nouveaux logs vous permettent de:

✅ **Comprendre** comment VectorIndexer traite vos features
✅ **Détecter** les configurations sous-optimales
✅ **Optimiser** `maxCategoricalCardinality` pour votre dataset
✅ **Valider** que les features sont traitées correctement

**Règle d'or:** Moins de 30 features catégorielles, chacune avec < 50 catégories.
