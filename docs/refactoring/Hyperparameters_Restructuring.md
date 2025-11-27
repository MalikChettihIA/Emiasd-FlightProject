# Restructuration: Hyperparameters → Model

## Objectif

Déplacer la section `hyperparameters` de `train` vers `model` pour une organisation plus logique de la configuration.

## Motivation

Les hyperparamètres sont des propriétés intrinsèques du **modèle** (nombre d'arbres, profondeur max, etc.) et non du processus d'**entraînement** (ratio train/test, K-folds, etc.). Cette restructuration améliore la cohérence logique de la configuration.

## Avant / Après

### Avant (Structure illogique)

```yaml
experiments:
  - name: "My-Experiment"
    model:
      modelType: "randomforest"

    train:
      trainRatio: 0.9
      crossValidation:
        numFolds: 2
      gridSearch:
        enabled: true

      hyperparameters:  # ❌ Illogique: ce sont des propriétés du modèle!
        numTrees: [20]
        maxDepth: [7]
        maxBins: [256]
```

### Après (Structure logique)

```yaml
experiments:
  - name: "My-Experiment"
    model:
      modelType: "randomforest"
      hyperparameters:  # ✅ Logique: propriétés du modèle sous model!
        numTrees: [20]
        maxDepth: [7]
        maxBins: [256]

    train:
      trainRatio: 0.9
      crossValidation:
        numFolds: 2
      gridSearch:
        enabled: true
```

## Modifications Effectuées

### 1. Configuration Case Classes

#### ExperimentModelConfig.scala ✅
**Avant**:
```scala
case class ExperimentModelConfig(
  modelType: String
)
```

**Après**:
```scala
case class ExperimentModelConfig(
  modelType: String,
  hyperparameters: HyperparametersConfig
)
```

#### TrainConfig.scala ✅
**Avant**:
```scala
case class TrainConfig(
  trainRatio: Double,
  crossValidation: CrossValidationConfig,
  gridSearch: GridSearchConfig,
  hyperparameters: HyperparametersConfig  // ❌ À enlever
)
```

**Après**:
```scala
case class TrainConfig(
  trainRatio: Double,
  crossValidation: CrossValidationConfig,
  gridSearch: GridSearchConfig
)
```

### 2. Code Scala - Accès aux Hyperparamètres

Tous les fichiers qui accédaient à `experiment.train.hyperparameters` ont été modifiés vers `experiment.model.hyperparameters`:

| Fichier | Ligne | Changement |
|---------|-------|------------|
| **CrossValidator.scala** | 243 | `val hp = experiment.model.hyperparameters` |
| **GradientBoostedTreesModel.scala** | 36 | `val hp = experiment.model.hyperparameters` |
| **LogisticRegressionModel.scala** | 31 | `val hp = experiment.model.hyperparameters` |
| **RandomForestModel.scala** | 32 | `val hp = experiment.model.hyperparameters` |
| **MLPipeline.scala** | 365 | `val hp = experiment.model.hyperparameters` |

### 3. Fichiers YAML

#### local-config.yml ✅
- **Experience-1**: hyperparameters déplacés sous `model`
- **Experience-2**: hyperparameters déplacés sous `model`

#### Example_Flight_Only_Config.yml ✅
- Mis à jour pour refléter la nouvelle structure

## Impact

### Aucun Impact sur le Comportement

✅ **Cette restructuration est purement organisationnelle**:
- Aucun changement de logique métier
- Aucun changement d'algorithme
- Les mêmes hyperparamètres sont utilisés exactement de la même façon
- Seule la **localisation** dans la configuration change

### Avantages

1. **Clarté**: Plus logique de trouver les hyperparamètres sous `model`
2. **Cohérence**: Propriétés du modèle groupées ensemble
3. **Maintenabilité**: Structure plus intuitive pour futurs développeurs
4. **Documentation**: Plus facile d'expliquer la structure

## Compatibilité

⚠️ **Breaking Change**: Les anciens fichiers de configuration ne fonctionneront plus!

**Migration Required**: Si vous avez des configurations existantes, vous devez:

1. Copier la section `hyperparameters` depuis `train`
2. La coller sous `model`
3. Supprimer `hyperparameters` de `train`

**Script de migration** (optionnel):
```bash
# Pour automatiser la migration de vos fichiers YAML
# (à exécuter manuellement ou via script)
```

## Test de Validation

Pour valider que la restructuration fonctionne:

```bash
# 1. Recompiler le projet
sbt clean compile

# 2. Lancer un training avec la nouvelle config
./work/scripts/spark-local-submit.sh

# 3. Vérifier que les logs affichent correctement les hyperparamètres
# Chercher "[RandomForest] Training with hyperparameters:"
```

## Fichiers Modifiés

### Configuration Classes
- ✅ `src/main/scala/com/flightdelay/config/ExperimentModelConfig.scala`
- ✅ `src/main/scala/com/flightdelay/config/TrainConfig.scala`

### ML Code
- ✅ `src/main/scala/com/flightdelay/ml/training/CrossValidator.scala`
- ✅ `src/main/scala/com/flightdelay/ml/models/GradientBoostedTreesModel.scala`
- ✅ `src/main/scala/com/flightdelay/ml/models/LogisticRegressionModel.scala`
- ✅ `src/main/scala/com/flightdelay/ml/models/RandomForestModel.scala`
- ✅ `src/main/scala/com/flightdelay/ml/MLPipeline.scala`

### Configuration Files
- ✅ `src/main/resources/local-config.yml`
- ✅ `docs/features/Example_Flight_Only_Config.yml`

## Résumé

✅ **Restructuration terminée avec succès**

La configuration est maintenant plus logique et intuitive:
- Les **hyperparamètres** sont sous **`model`** (où ils devraient être)
- La section **`train`** ne contient que les paramètres d'entraînement (trainRatio, CV, gridSearch)

Cette organisation facilite la compréhension et la maintenance du code.
