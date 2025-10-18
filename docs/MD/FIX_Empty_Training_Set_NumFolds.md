# Fix: Empty Training Set avec numFolds: 1

## Erreur Rencontrée

```
java.lang.IllegalArgumentException: DecisionTree requires size of input RDD > 0, but was given by empty one.
```

**Contexte :**
```
[STEP 2] Cross-Validation on Development Set
--------------------------------------------------------------------------------
[CrossValidator] Starting K-Fold Cross-Validation
  - Number of folds: 1
  - Grid Search: ENABLED

[RandomForest] Training with hyperparameters:
  - Number of trees: 100
  - Max depth: 10
  ...

ERROR: DecisionTree requires size of input RDD > 0, but was given by empty one.
```

---

## Cause Racine

La configuration avait `numFolds: 1` :

```yaml
# local-config.yml (INCORRECT)
train:
  trainRatio: 0.8

  crossValidation:
    numFolds: 1  # ← ERREUR!
```

### Pourquoi `numFolds: 1` Cause un Training Set Vide ?

Le K-Fold Cross-Validation divise les données en `k` parties égales :

**Avec `numFolds: 1` :**
```
Development Set (601,224 samples)
├─ Fold 0: 100% des données
│   ├─ Training: 0%       ← VIDE ! (0 samples)
│   └─ Validation: 100%   ← Toutes les données (601,224 samples)
```

**Résultat :** Le Random Forest reçoit un training set vide → Erreur !

---

### Explication Mathématique

Pour K-Fold Cross-Validation :
- **Training size** = `(k-1)/k × total`
- **Validation size** = `1/k × total`

**Avec k=1 :**
- Training = `(1-1)/1 × 601,224` = `0 × 601,224` = **0 samples** ❌
- Validation = `1/1 × 601,224` = **601,224 samples** ✓

**Avec k=5 (correct) :**
- Training = `(5-1)/5 × 601,224` = `0.8 × 601,224` = **480,979 samples** ✓
- Validation = `1/5 × 601,224` = `0.2 × 601,224` = **120,245 samples** ✓

---

## Solution Appliquée

### Fichier Modifié : `local-config.yml`

**Avant :**
```yaml
crossValidation:
  numFolds: 1  # ← INCORRECT
```

**Après :**
```yaml
crossValidation:
  numFolds: 5  # ← CORRECT
```

### Valeurs Valides pour `numFolds`

| numFolds | Training % | Validation % | Recommandé ? |
|----------|------------|--------------|--------------|
| 1 | 0% | 100% | ❌ **INVALIDE** (training vide) |
| 2 | 50% | 50% | ⚠️ Peu de données d'entraînement |
| 3 | 67% | 33% | ✅ OK (petit dataset) |
| 5 | 80% | 20% | ✅ **STANDARD** (recommandé) |
| 10 | 90% | 10% | ✅ OK (grand dataset) |

**Règle :** `numFolds >= 2` obligatoire !

---

## Résultat Attendu Après Fix

Avec `numFolds: 5`, le pipeline créera 5 folds :

```
Development Set (601,224 samples)
├─ Fold 0:
│   ├─ Training: 480,979 samples (80%)  ← 4 folds
│   └─ Validation: 120,245 samples (20%) ← 1 fold
├─ Fold 1:
│   ├─ Training: 480,979 samples (80%)
│   └─ Validation: 120,245 samples (20%)
├─ Fold 2:
│   ├─ Training: 480,979 samples (80%)
│   └─ Validation: 120,245 samples (20%)
├─ Fold 3:
│   ├─ Training: 480,979 samples (80%)
│   └─ Validation: 120,245 samples (20%)
└─ Fold 4:
    ├─ Training: 480,979 samples (80%)
    └─ Validation: 120,245 samples (20%)

Average metrics across 5 folds → Final model
```

**Logs attendus :**
```
[CrossValidator] Starting K-Fold Cross-Validation
  - Number of folds: 5
  - Grid Search: ENABLED

[Fold 1/5] Training...
  - Training samples: 480,979
  - Validation samples: 120,245

[RandomForest] Training completed
  - Training accuracy: 85.23%
  - Validation F1: 62.45%

[Fold 2/5] Training...
...
```

---

## Pourquoi Avait-on `numFolds: 1` ?

### Hypothèses

1. **Test rapide** : Pour désactiver temporairement la cross-validation
2. **Copie d'erreur** : Valeur par défaut incorrecte
3. **Confusion** : Pensée que 1 fold = pas de split

### Comment Désactiver la Cross-Validation Correctement ?

Si vous voulez un **simple train/test split** sans cross-validation :

**Option 1 : Utiliser 2 folds (split 50/50)**
```yaml
crossValidation:
  numFolds: 2  # Training 50%, Validation 50%
```

**Option 2 : Augmenter le trainRatio et utiliser 5 folds**
```yaml
train:
  trainRatio: 0.9  # 90% dev, 10% hold-out test

  crossValidation:
    numFolds: 5  # Sur les 90% dev : 72% training, 18% validation par fold
```

**Option 3 : Code custom (non supporté actuellement)**
```scala
// Désactiver complètement le K-Fold
// Entraîner directement sur trainRatio sans CV
// Nécessite modification du code MLPipeline.scala
```

---

## Vérification du Fix

### 1. Vérifier la Configuration

```bash
grep -A 3 "crossValidation:" src/main/resources/local-config.yml
```

**Résultat attendu :**
```yaml
crossValidation:
  numFolds: 5
```

### 2. Recompiler

```bash
sbt package
```

### 3. Relancer le Pipeline

```bash
./submit.sh
```

### 4. Vérifier les Logs

**Chercher :**
```bash
grep "Number of folds" logs.txt
```

**Résultat attendu :**
```
[CrossValidator] Starting K-Fold Cross-Validation
  - Number of folds: 5
```

**Chercher :**
```bash
grep "Training samples:" logs.txt
```

**Résultat attendu :**
```
[Fold 1/5] Training...
  - Training samples: 480,979
  - Validation samples: 120,245
```

**Si vous voyez encore "Training samples: 0"** → Le JAR n'a pas été régénéré avec la nouvelle config

---

## Autres Erreurs Similaires

### "Expected numFolds > 1"

**Erreur :**
```
java.lang.IllegalArgumentException: requirement failed: numFolds must be > 1
```

**Cause :** Spark valide `numFolds >= 2` à la création du CrossValidator

**Solution :** Même fix que ci-dessus (`numFolds: 5`)

---

### "Training dataset is empty"

**Erreur :**
```
java.lang.IllegalStateException: Training dataset is empty
```

**Causes possibles :**
1. `numFolds: 1` (ce fix)
2. Données filtrées complètement par le preprocessing
3. Tous les records ont des valeurs nulles dans les features

**Diagnostic :**
```scala
// Vérifier le nombre de records avant training
println(s"Training samples: ${trainingData.count()}")
```

---

## Résumé

| Avant | Après |
|-------|-------|
| ❌ `numFolds: 1` | ✅ `numFolds: 5` |
| ❌ Training set vide (0 samples) | ✅ Training set valide (480,979 samples) |
| ❌ Erreur "empty RDD" | ✅ Entraînement réussi |
| ❌ Pas de cross-validation possible | ✅ Cross-validation sur 5 folds |

**Configuration recommandée :**
```yaml
train:
  trainRatio: 0.8  # 80% dev, 20% hold-out test

  crossValidation:
    numFolds: 5  # K-Fold CV sur les 80% dev
```

**Répartition finale :**
- **Development (80%)** : 601,224 samples
  - Training (64%) : 480,979 samples (4/5 des dev)
  - Validation (16%) : 120,245 samples (1/5 des dev)
- **Hold-out Test (20%)** : 150,058 samples

---

## Prochaine Étape

Relancer le pipeline :

```bash
./submit.sh
```

L'entraînement devrait maintenant fonctionner correctement avec 5 folds de cross-validation ! 🎯
