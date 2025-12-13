# Machine Learning - Histoire Complète du Projet de Prédiction des Retards de Vols

## Table des Matières

1. [Introduction et Vision d'Ensemble](#1-introduction-et-vision-densemble)
2. [Architecture du Pipeline ML](#2-architecture-du-pipeline-ml)
3. [Les Algorithmes Implémentés](#3-les-algorithmes-implémentés)
4. [Configuration et Hyperparamètres](#4-configuration-et-hyperparamètres)
5. [Stratégie de Validation](#5-stratégie-de-validation)
6. [Grid Search et Optimisation](#6-grid-search-et-optimisation)
7. [Évaluation des Modèles](#7-évaluation-des-modèles)
8. [Gestion de la Mémoire et Optimisations](#8-gestion-de-la-mémoire-et-optimisations)
9. [MLFlow et Tracking des Expériences](#9-mlflow-et-tracking-des-expériences)
10. [Visualisations et Rapports](#10-visualisations-et-rapports)

---

## 1. Introduction et Vision d'Ensemble

### 1.1 Le Problème

Ce projet vise à prédire les retards de vols en utilisant des données historiques de vols et des conditions météorologiques. Il s'agit d'un problème de **classification binaire** :
- **Classe 0** : Vol à l'heure (on-time)
- **Classe 1** : Vol en retard (delayed)

### 1.2 L'Approche Adoptée

Le projet implémente une approche industrielle complète avec :
- **4 algorithmes de Machine Learning** différents (Random Forest, GBT, XGBoost, Logistic Regression)
- **Grid Search** automatisé pour l'optimisation des hyperparamètres
- **K-Fold Cross-Validation** pour une évaluation robuste
- **Hold-out test set** pour l'évaluation finale sans biais
- **Protection contre le data leakage** via une séparation stricte train/test
- **Tracking MLFlow** pour la traçabilité des expériences

### 1.3 Fichier Central : MLPipeline.scala

Le cœur du système ML se trouve dans `src/main/scala/com/flightdelay/ml/MLPipeline.scala` (998 lignes). Ce fichier orchestre l'ensemble du processus d'entraînement et d'évaluation.

**Localisation** : `src/main/scala/com/flightdelay/ml/MLPipeline.scala`

---

## 2. Architecture du Pipeline ML

### 2.1 Vision d'Ensemble

L'architecture adoptée suit le pattern **"Hold-out test set + K-fold CV"**, considéré comme une best practice en ML :

```
┌─────────────────────────────────────────────────────────────┐
│                    DONNÉES COMPLÈTES                        │
│                   (balanced dataset)                        │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ Split 80/20
                   ▼
        ┌──────────┴──────────┐
        │                     │
        ▼                     ▼
┌───────────────┐    ┌────────────────┐
│  DEV SET      │    │  TEST SET      │
│  (80%)        │    │  (20%)         │
│               │    │  (hold-out)    │
└───────┬───────┘    └────────────────┘
        │
        │ K-Fold CV + Grid Search
        ▼
┌───────────────────────────────┐
│  Best Hyperparameters Found   │
└───────────┬───────────────────┘
            │
            │ Train final model on full dev set
            ▼
     ┌──────────────┐
     │ FINAL MODEL  │
     └──────┬───────┘
            │
            │ Evaluate on hold-out test
            ▼
     ┌──────────────┐
     │ TEST METRICS │
     └──────────────┘
```

### 2.2 Les 8 Étapes du Pipeline

Le pipeline ML se décompose en 8 étapes clairement définies dans `MLPipeline.scala` :

#### STEP 1 : Vérification des datasets pré-séparés
```scala
// Ligne 145-159 de MLPipeline.scala
info("[ML PIPELINE][STEP 1] Received pre-split balanced datasets from FeaturePipeline")
info("Note: Train/test split was done in FeaturePipeline BEFORE join/explosion")
info("      This ensures balanced datasets and avoids manipulating huge datasets")
```

**Pourquoi cette approche ?**
- Les datasets sont séparés **AVANT** les jointures complexes vols/météo
- Cela évite de manipuler des datasets gigantesques (explosion des données avec les jointures temporelles)
- Le balancement (50/50 delayed/on-time) est préservé dans les deux ensembles

#### STEP 2 : Feature Extraction sur le DEV SET uniquement
```scala
// Lignes 163-173 de MLPipeline.scala
info("[ML PIPELINE][STEP 2] Feature Extraction (fit on dev set only)")
info(" CRITICAL: Feature transformers are fit ONLY on training data")
info("            to prevent data leakage from test set")

val (devData, featureModels) = FeatureExtractor.extract(devDataRaw, experiment)
```

**Choix d'implémentation crucial** :
- Les transformateurs (StringIndexer, Scaler, PCA) sont **fit uniquement sur le dev set**
- Cela évite le **data leakage** : le test set ne doit jamais influencer l'entraînement
- Les modèles de transformation sont sauvegardés pour être réutilisés sur le test set

**Exemple concret** :
```scala
// Dans FeatureExtractor.scala
// 1. Fit des transformateurs sur DEV SET
val stringIndexer = new StringIndexer()
  .setInputCol("OP_CARRIER_AIRLINE_ID")
  .setOutputCol("indexed_carrier")
  .fit(devData)  // FIT sur dev uniquement !

// 2. Les statistiques apprises (ex: mapping "AA" -> 0.0, "UA" -> 1.0)
//    sont stockées dans featureModels
```

#### STEP 3 : Feature Extraction sur le TEST SET (transform only)
```scala
// Lignes 176-186 de MLPipeline.scala
info("[ML PIPELINE][STEP 3] Feature Extraction (test set)")
info(" Using pre-fitted models from dev set - NO DATA LEAKAGE")
info("  - StringIndexer: uses categories learned from dev set only")
info("  - Scaler: uses statistics (mean/std) from dev set only")
info("  - PCA: uses components fitted on dev set only")

val testData = FeatureExtractor.transform(testDataRaw, featureModels, experiment)
```

**Garantie anti-leakage** :
- Le test set est transformé avec les modèles déjà fittés
- Aucun re-fitting n'est effectué
- Si une catégorie inconnue apparaît dans le test set, elle est gérée via `handleInvalid: "keep"`

#### STEP 4 : K-Fold Cross-Validation + Grid Search
```scala
// Lignes 191-276 de MLPipeline.scala
if (fast) {
  // Mode rapide : skip CV, utilise hyperparamètres par défaut
  info("[ML PIPELINE][STEP 4] FAST MODE - Skipping Cross-Validation")
} else {
  // Mode complet : K-fold CV avec grid search optionnel
  info("[ML PIPELINE][STEP 4] Cross-Validation on Development Set")
  val cvRes = CrossValidator.validate(devData, experiment)
}
```

**Deux modes d'exécution** :
1. **Mode FAST** (`fast=true`) : Skip la CV, train directement avec les hyperparamètres par défaut
   - Utile pour le debugging rapide
   - Gain de temps considérable (pas de grid search)

2. **Mode COMPLET** (`fast=false`) : K-Fold CV complète avec grid search
   - Teste toutes les combinaisons d'hyperparamètres
   - Sélectionne la meilleure configuration
   - Retourne les métriques moyennes ± écart-type

#### STEP 5 : Entraînement du Modèle Final
```scala
// Lignes 281-290 de MLPipeline.scala
info("[ML PIPELINE][STEP 5] Training Final Model on Full Development Set")

val finalModel = Trainer.trainFinal(
  devData,
  experiment,
  cvResult.bestHyperparameters  // Utilise les meilleurs hyperparamètres trouvés
)
```

**Logique importante** :
- Le modèle final est entraîné sur **l'intégralité du dev set** (80% des données)
- Il utilise les **meilleurs hyperparamètres** trouvés durant la CV
- C'est ce modèle qui sera évalué sur le test set et sauvegardé pour la production

#### STEP 6 : Évaluation sur Hold-out Test Set
```scala
// Lignes 295-426 de MLPipeline.scala
info("[STEP 6] Final Evaluation on Hold-out Test Set")

// OPTIMIZATION: Save + reload model to avoid broadcast OOM
val modelPath = s"$experimentOutputPath/models/${experiment.model.modelType}_final"
finalModel.asInstanceOf[PipelineModel].write.overwrite().save(modelPath)

val reloadedModel = PipelineModel.load(modelPath)
val testPredictions = reloadedModel.transform(testData)
val holdOutMetrics = ModelEvaluator.evaluate(predictions=testPredictions)
```

**Optimisation critique** (voir section 8.1 pour les détails) :
- Le modèle est sauvegardé puis rechargé depuis le disque
- Cela évite les erreurs de **broadcast OOM** avec les grands modèles (Random Forest 300 arbres)
- C'est une solution pragmatique à un problème récurrent de Spark ML

#### STEP 7 : Sauvegarde des Métriques
```scala
// Lignes 431-476 de MLPipeline.scala
info("[STEP 7] Saving Metrics")

saveMetrics(experiment, cvResult, holdOutMetrics, testPredictions, experimentOutputPath, fast)
saveTrainingSummary(experiment, cvResult, holdOutMetrics, totalTime, experimentOutputPath, fast)
```

**Fichiers générés** :
```
{experiment.name}/metrics/
├── cv_fold_metrics.csv          # Métriques de chaque fold
├── cv_summary.csv                # Moyennes et écart-types
├── holdout_test_metrics.csv      # Métriques finales sur test set
├── best_hyperparameters.csv      # Meilleurs hyperparamètres trouvés
├── holdout_roc_data.csv          # Données pour courbe ROC
└── training_summary.txt          # Résumé human-readable
```

#### STEP 8 : Génération des Visualisations
```scala
// Lignes 957-996 de MLPipeline.scala
private def generatePlots(metricsPath: String): Unit = {
  val scriptPath = s"$scriptsBasePath/visualize_ml_pipeline.py"
  val command = s"python3 $scriptPath $metricsPath"
  command.!  // Execute Python script
}
```

**Visualisations générées** (par script Python) :
- CV fold metrics trends
- CV vs Hold-out comparison
- Stability box plots
- Confusion matrix heatmap
- ROC curve
- Feature importances

---

## 3. Les Algorithmes Implémentés

Le projet implémente **4 algorithmes de classification** via un pattern Factory.

### 3.1 Random Forest (RandomForestModel.scala)

**Localisation** : `src/main/scala/com/flightdelay/ml/models/RandomForestModel.scala` (353 lignes)

#### 3.1.1 Principe

Random Forest est un ensemble de **multiples arbres de décision** entraînés en parallèle :
- Chaque arbre est entraîné sur un sous-échantillon aléatoire des données (bootstrap)
- Chaque split considère un sous-ensemble aléatoire des features
- La prédiction finale est le **vote majoritaire** des arbres

**Avantages pour la prédiction de retards de vols** :
```scala
// Lignes 14-24 de RandomForestModel.scala
/**
 * Advantages for flight delay prediction:
 * - Handles non-linear relationships well
 * - Robust to outliers
 * - Provides feature importance
 * - Works well with high-dimensional data
 */
```

#### 3.1.2 Hyperparamètres Implémentés

```scala
// Lignes 36-76 de RandomForestModel.scala
def train(data: DataFrame): Transformer = {
  val hp = experiment.model.hyperparameters

  // Extraction des hyperparamètres (première valeur de chaque array)
  val numTrees = hp.numTrees.getOrElse(Seq(100)).head
  val maxDepth = hp.maxDepth.getOrElse(Seq(5)).head
  val maxBins = hp.maxBins.getOrElse(Seq(32)).head
  val minInstancesPerNode = hp.minInstancesPerNode.getOrElse(Seq(1)).head
  val subsamplingRate = hp.subsamplingRate.getOrElse(Seq(1.0)).head
  val featureSubsetStrategy = hp.featureSubsetStrategy.getOrElse(Seq("auto")).head
  val impurity = hp.impurity.getOrElse("gini")
```

**Détail de chaque hyperparamètre** :

##### numTrees (Nombre d'arbres)
```scala
.setNumTrees(numTrees)  // Ligne 65
```
- **Rôle** : Nombre d'arbres dans la forêt
- **Valeurs typiques** : 100, 200, 300
- **Impact** :
  - ↑ numTrees → ↑ précision, ↑ temps d'entraînement
  - Plateau de performance atteint généralement entre 100-300 arbres
  - Au-delà, gain marginal vs coût computationnel
- **Choix projet** : Testé [15, 100, 200, 300] via grid search
  - 15 arbres pour tests rapides
  - 100-300 pour production

##### maxDepth (Profondeur maximale)
```scala
.setMaxDepth(maxDepth)  // Ligne 66
```
- **Rôle** : Profondeur maximale de chaque arbre
- **Valeurs typiques** : 5, 10, 15, 20
- **Impact** :
  - ↑ maxDepth → Modèle plus complexe, risque de surapprentissage
  - ↓ maxDepth → Modèle plus simple, peut sous-apprendre
- **Choix projet** : Testé [5, 7, 10, 15]
  - 5-7 pour modèles régularisés
  - 10-15 pour capturer interactions complexes vols/météo

##### maxBins (Nombre de bins pour discrétisation)
```scala
.setMaxBins(maxBins)  // Ligne 67
```
- **Rôle** : Nombre de bins pour discrétiser les features continues
- **Valeurs typiques** : 32, 64, 128, 256
- **Impact** :
  - ↑ maxBins → Meilleure granularité, mais ↑ mémoire
  - Doit être ≥ nombre de catégories pour features catégorielles
- **Choix projet** : 280 (≥ maxCategoricalCardinality)
  - Permet de gérer les 280 aéroports distincts
  - Évite l'erreur "maxBins must be >= number of categories"

##### minInstancesPerNode (Instances minimales par nœud)
```scala
.setMinInstancesPerNode(minInstancesPerNode)  // Ligne 68
```
- **Rôle** : Nombre minimum d'échantillons requis pour créer un nœud enfant
- **Valeurs typiques** : 1, 5, 10, 20
- **Impact** :
  - ↑ minInstancesPerNode → Régularisation plus forte, prévient surapprentissage
  - ↓ minInstancesPerNode → Arbres plus profonds, risque de surapprentissage
- **Choix projet** : Testé [1, 5, 10]
  - 1 pour maximum de flexibilité
  - 5-10 pour régularisation

##### subsamplingRate (Taux de sous-échantillonnage)
```scala
.setSubsamplingRate(subsamplingRate)  // Ligne 71
```
- **Rôle** : Fraction des données utilisée pour entraîner chaque arbre
- **Valeurs typiques** : 0.6, 0.8, 1.0
- **Impact** :
  - 1.0 = Bootstrap complet (standard Random Forest)
  - <1.0 = Sous-échantillonnage supplémentaire (régularisation)
- **Choix projet** : 1.0 (bootstrap standard)

##### featureSubsetStrategy (Stratégie de sélection des features)
```scala
.setFeatureSubsetStrategy(featureSubsetStrategy)  // Ligne 69
```
- **Rôle** : Nombre de features considérées à chaque split
- **Valeurs possibles** :
  - `"auto"` : sqrt(n_features) pour classification
  - `"all"` : Toutes les features (désactive la randomisation)
  - `"sqrt"` : sqrt(n_features)
  - `"log2"` : log2(n_features)
  - `"onethird"` : n_features / 3
- **Impact** :
  - Moins de features → Plus de diversité entre arbres
  - Plus de features → Chaque arbre plus performant individuellement
- **Choix projet** : Testé ["auto", "all", "sqrt"]
  - "auto" recommandé pour classification
  - "all" pour tester performance sans randomisation

##### impurity (Mesure d'impureté)
```scala
.setImpurity(impurity)  // Ligne 70
```
- **Rôle** : Critère de split des nœuds
- **Valeurs possibles** :
  - `"gini"` : Gini impurity (par défaut)
  - `"entropy"` : Information gain
- **Formules** :
  - Gini : `1 - Σ(p_i²)` où p_i = proportion de classe i
  - Entropy : `-Σ(p_i * log(p_i))`
- **Choix projet** : "gini" (standard, plus rapide)

#### 3.1.3 Optimisations Critiques

```scala
// Lignes 72-75 de RandomForestModel.scala
// OPTIMISATIONS CRITIQUES
.setCacheNodeIds(true)             // Active le cache (améliore perf)
.setCheckpointInterval(5)         // CRITIQUE : checkpoint tous les 5 arbres
.setMaxMemoryInMB(2048)            // 2 GB (au lieu de 512 MB)
```

**Explications** :

##### setCacheNodeIds(true)
- **Rôle** : Met en cache les IDs des nœuds durant l'entraînement
- **Impact** : Accélère l'entraînement en évitant de recalculer les assignations nœud-échantillon
- **Coût** : Utilisation mémoire supplémentaire
- **Recommandation** : Toujours activer pour Random Forest

##### setCheckpointInterval(5)
- **Rôle** : Sauvegarde l'état tous les 5 arbres (Spark checkpoint)
- **Pourquoi critique** :
  - Random Forest construit un DAG Spark qui grandit avec chaque arbre
  - Sans checkpoint, le DAG devient trop large → stack overflow ou OOM
  - Le checkpoint "coupe" le DAG en le matérialisant sur disque
- **Valeur 5** : Compromis entre performance et sécurité
  - Trop fréquent (1-2) → Ralentit l'entraînement
  - Trop rare (20+) → Risque de DAG trop large

##### setMaxMemoryInMB(2048)
- **Rôle** : Mémoire maximale allouée pour les statistiques d'entraînement
- **Défaut Spark** : 512 MB (insuffisant pour grandes données)
- **Valeur 2048** : 2 GB, permet de gérer des datasets plus larges

#### 3.1.4 Feature Importance

```scala
// Lignes 90-105 de RandomForestModel.scala
val rfModel = model.stages(0).asInstanceOf[RandomForestClassificationModel]
displayFeatureImportance(rfModel)

// Save feature importance if path provided
featureImportancePath.foreach { path =>
  saveFeatureImportance(rfModel, path)
  val reportPath = path.replace(".csv", "_report.txt")
  saveFeatureImportanceReport(rfModel, reportPath)
}
```

**Calcul de l'importance** :
- Random Forest calcule l'importance via **Gini importance** (ou entropy)
- Pour chaque feature, somme la réduction d'impureté apportée par tous les splits utilisant cette feature
- Normalisé entre 0 et 1 (somme totale = 1)

**Affichage visuel** :
```scala
// Lignes 174-179 de RandomForestModel.scala
val indicator = if (importancePercent >= 10) "█"
               else if (importancePercent >= 5) "▓"
               else if (importancePercent >= 1) "▒"
               else "░"

info(f"${rank + 1}%-6d [${featureIdx}%3d]  ${shortName}%-60s ${indicator} ${importancePercent}%6.2f%%")
```

**Exemple de sortie** :
```
Top 20 Feature Importances:
==================================================================================
Rank   Index   Feature Name                                          Importance
==================================================================================
1      [123]  org_w_feature_flight_category_ordinal                 █  12.34%
2      [ 45]  CRS_DEP_TIME                                          ▓   8.76%
3      [ 67]  dst_w_HourlyPrecip_sum                                ▒   3.21%
...
```

### 3.2 Gradient Boosted Trees (GradientBoostedTreesModel.scala)

**Localisation** : `src/main/scala/com/flightdelay/ml/models/GradientBoostedTreesModel.scala` (228 lignes)

#### 3.2.1 Principe

GBT construit des arbres **séquentiellement**, où chaque nouvel arbre corrige les erreurs des précédents :

```
Modèle_0 = arbre initial (simple)
Modèle_1 = Modèle_0 + α * arbre_1 (corrige erreurs de Modèle_0)
Modèle_2 = Modèle_1 + α * arbre_2 (corrige erreurs de Modèle_1)
...
Modèle_final = Σ(α_i * arbre_i)
```

**Différences vs Random Forest** :
```scala
// Lignes 14-28 de GradientBoostedTreesModel.scala
/**
 * Advantages for flight delay prediction:
 * - Better accuracy than Random Forest on many datasets
 * - Handles non-linear relationships and interactions
 * - Less prone to overfitting with proper tuning
 * - Feature importance available
 *
 * Disadvantages:
 * - Slower training than Random Forest (sequential)
 * - More sensitive to hyperparameters
 * - Can overfit if not properly regularized
 */
```

#### 3.2.2 Hyperparamètres Implémentés

```scala
// Lignes 40-72 de GradientBoostedTreesModel.scala
val maxIter = hp.maxIter.getOrElse(Seq(100)).head          // Nombre d'arbres
val maxDepth = hp.maxDepth.getOrElse(Seq(5)).head          // Profondeur max
val maxBins = hp.maxBins.getOrElse(Seq(32)).head           // Bins
val minInstancesPerNode = hp.minInstancesPerNode.getOrElse(Seq(1)).head
val subsamplingRate = hp.subsamplingRate.getOrElse(Seq(1.0)).head
val stepSize = hp.stepSize.getOrElse(Seq(0.1)).head        // Learning rate

val gbt = new GBTClassifier()
  .setMaxIter(maxIter)
  .setMaxDepth(maxDepth)
  .setMaxBins(maxBins)
  .setMinInstancesPerNode(minInstancesPerNode)
  .setSubsamplingRate(subsamplingRate)
  .setStepSize(stepSize)
  .setSeed(experiment.name.hashCode.toLong)
```

**Hyperparamètre spécifique : stepSize (Learning Rate)**

##### stepSize / Learning Rate
- **Rôle** : Pondération de chaque nouvel arbre (α dans la formule ci-dessus)
- **Valeurs typiques** : 0.01, 0.05, 0.1, 0.2
- **Impact** :
  - ↓ stepSize → Plus d'arbres nécessaires, mais meilleure généralisation
  - ↑ stepSize → Convergence plus rapide, mais risque de surapprentissage
- **Trade-off** :
  ```
  Petit stepSize (0.01) + Beaucoup d'arbres (1000)
  VS
  Grand stepSize (0.2) + Peu d'arbres (50)
  ```
  Premier généralement meilleur mais plus lent

- **Choix projet** : Testé [0.01, 0.05, 0.1, 0.2]

### 3.3 XGBoost (XGBoostModel.scala)

**Localisation** : `src/main/scala/com/flightdelay/ml/models/XGBoostModel.scala` (292 lignes)

#### 3.3.1 Principe

XGBoost = **Extreme Gradient Boosting**, une version optimisée de GBT avec :
- Régularisation L1/L2 intégrée
- Meilleure gestion des valeurs manquantes
- Parallélisation au niveau des arbres individuels
- Nombreuses optimisations algorithmiques

```scala
// Lignes 14-32 de XGBoostModel.scala
/**
 * Advantages over GBT Spark:
 * - Better performance (speed and accuracy)
 * - Advanced regularization (L1, L2, gamma)
 * - Better handling of missing values
 * - Built-in cross-validation
 * - GPU support (if available)
 * - More hyperparameter control
 */
```

#### 3.3.2 Hyperparamètres Implémentés

```scala
// Lignes 43-92 de XGBoostModel.scala
val numRound = hp.maxIter.getOrElse(Seq(100)).head    // Nombre d'arbres
val maxDepth = hp.maxDepth.getOrElse(Seq(6)).head     // Profondeur
val eta = hp.stepSize.getOrElse(Seq(0.1)).head        // Learning rate
val subsample = hp.subsamplingRate.getOrElse(Seq(1.0)).head
val colsampleBytree = hp.colsampleBytree.getOrElse(Seq(1.0)).head
val minChildWeight = hp.minInstancesPerNode.getOrElse(Seq(1)).head
val alpha = hp.alpha.getOrElse(Seq(0.0)).head         // L1 regularization
val lambda = hp.lambda.getOrElse(Seq(1.0)).head       // L2 regularization
val gamma = hp.gamma.getOrElse(Seq(0.0)).head         // Min loss reduction

val xgbParams = Map(
  "eta" -> eta,
  "max_depth" -> maxDepth,
  "subsample" -> subsample,
  "colsample_bytree" -> colsampleBytree,
  "min_child_weight" -> minChildWeight,
  "alpha" -> alpha,
  "lambda" -> lambda,
  "gamma" -> gamma,
  "objective" -> "binary:logistic",
  "eval_metric" -> "logloss",
  "seed" -> experiment.name.hashCode.toLong,
  "nthread" -> 4
)
```

**Hyperparamètres spécifiques à XGBoost** :

##### alpha (L1 Regularization)
- **Rôle** : Régularisation L1 sur les poids des feuilles
- **Formule** : `Loss = L(y, ŷ) + α * Σ|w_i|`
- **Impact** : Pousse certains poids vers 0 (sparse solutions)
- **Valeurs typiques** : 0.0, 0.1, 0.5, 1.0
- **Choix projet** : Testé [0.0, 0.1, 0.5]

##### lambda (L2 Regularization)
- **Rôle** : Régularisation L2 sur les poids des feuilles
- **Formule** : `Loss = L(y, ŷ) + λ * Σ(w_i²)`
- **Impact** : Pénalise les poids élevés, lisse le modèle
- **Valeurs typiques** : 0.5, 1.0, 2.0, 5.0
- **Choix projet** : Testé [0.5, 1.0, 2.0]
- **Défaut XGBoost** : 1.0 (toujours actif !)

##### gamma (Minimum Loss Reduction)
- **Rôle** : Réduction de loss minimale requise pour créer un split
- **Formule** : Split créé si `Gain > gamma`
- **Impact** :
  - gamma = 0 → Aucune contrainte (défaut)
  - gamma > 0 → Élagage plus agressif
- **Valeurs typiques** : 0.0, 0.1, 0.5, 1.0
- **Choix projet** : Testé [0.0, 0.1, 0.5]

##### colsample_bytree (Column Sampling)
- **Rôle** : Fraction de features utilisées pour construire chaque arbre
- **Impact** : Équivalent à featureSubsetStrategy de RF
- **Valeurs typiques** : 0.6, 0.8, 1.0
- **Choix projet** : Testé [0.8, 1.0]

#### 3.3.3 Gestion des Valeurs NaN/Infinity

```scala
// Lignes 104-114 de XGBoostModel.scala
val cleanVectorUdf = udf((v: Vector) => {
  if (v == null) {
    Vectors.dense(Array.fill(41)(0.0))  // Remplace null par vecteur de zéros
  } else {
    val arr = v.toArray.map { x =>
      if (x.isNaN || x.isInfinity) 0.0 else x  // NaN/Inf → 0.0
    }
    Vectors.dense(arr)
  }
})

val dfClean = data
  .withColumn("features", cleanVectorUdf(col("features")))
  .na.fill(0.0, Seq("label"))
```

**Pourquoi ce nettoyage ?**
- XGBoost Spark ne gère pas bien les NaN/Infinity dans les vecteurs de features
- Les transformations précédentes (PCA, Scaler) peuvent produire NaN si données manquantes
- Solution pragmatique : remplacer par 0.0 (valeur neutre après scaling)

### 3.4 Logistic Regression (LogisticRegressionModel.scala)

**Localisation** : `src/main/scala/com/flightdelay/ml/models/LogisticRegressionModel.scala`

#### 3.4.1 Principe

Régression logistique = modèle linéaire pour classification binaire :

```
P(y=1|x) = σ(w·x + b) = 1 / (1 + e^(-(w·x + b)))
```

Où :
- w = vecteur de poids (coefficients)
- x = vecteur de features
- b = biais (intercept)
- σ = fonction sigmoïde

**Avantages** :
- Modèle simple, interprétable
- Entraînement rapide
- Coefficients = importance des features (avec signe)
- Baseline solide pour comparer aux modèles complexes

**Inconvénients** :
- Suppose relations linéaires
- Moins performant sur données complexes/non-linéaires

#### 3.4.2 Hyperparamètres Implémentés

```scala
val maxIter = hp.maxIter.getOrElse(Seq(100)).head
val regParam = hp.regParam.getOrElse(Seq(0.0)).head
val elasticNetParam = hp.elasticNetParam.getOrElse(Seq(0.0)).head

val lr = new LogisticRegression()
  .setMaxIter(maxIter)
  .setRegParam(regParam)
  .setElasticNetParam(elasticNetParam)
```

##### regParam (Regularization Parameter)
- **Rôle** : Force de la régularisation
- **Formule** : `Loss = LogLoss + λ * Penalty`
- **Valeurs typiques** : 0.0, 0.01, 0.1, 1.0
- **Impact** :
  - 0.0 = Pas de régularisation
  - ↑ regParam → Coefficients plus petits, moins de surapprentissage

##### elasticNetParam (ElasticNet Mixing)
- **Rôle** : Mix entre L1 et L2
- **Formule** : `Penalty = α * L1 + (1-α) * L2`
- **Valeurs** :
  - 0.0 = Pure L2 (Ridge)
  - 0.5 = Mix équilibré (ElasticNet)
  - 1.0 = Pure L1 (Lasso)
- **Choix** :
  - L2 (Ridge) : Tous les coefficients shrunk, aucun à 0
  - L1 (Lasso) : Sélection de features (certains coefficients = 0)
  - ElasticNet : Compromis

---

## 4. Configuration et Hyperparamètres

### 4.1 Structure de Configuration YAML

**Localisation** : `src/main/resources/local-d3_7_7-config.yml`

#### 4.1.1 Configuration Globale

```yaml
# Lignes 1-32 de local-d3_7_7-config.yml
environment: "local"

common:
  seed: 42                        # Reproductibilité
  log: true
  logLevel: "info"

  data:
    basePath: "/data"
    flight:
      path: "/data/FLIGHT-3Y/Flights/*.csv"
    weather:
      path: "/data/FLIGHT-3Y/Weather/*.txt"

  output:
    basePath: "/output"

  mlflow:
    enabled: true
    trackingUri: "http://mlflow-server:5000"
```

#### 4.1.2 Configuration d'une Expérience

```yaml
# Lignes 42-148 de local-d3_7_7-config.yml
experiments:
  - name: "Experience-local-D3-7-7"
    description: "Baseline Random Forest with hyperparameters tuning"
    enabled: true
    target: "is_delayed"
```

**Champs obligatoires** :
- `name` : Identifiant unique de l'expérience
- `description` : Description human-readable
- `enabled` : Permet de désactiver une expérience sans la supprimer
- `target` : Nom de la colonne cible ("is_delayed")

#### 4.1.3 Configuration Feature Extraction

```yaml
# Lignes 47-127 de local-d3_7_7-config.yml
featureExtraction:
  type: "feature_selection"              # ou "pca" ou "none"
  dxCol: "D3"                             # Définition du retard : D1, D2_60, D3, D4
  storeJoinData: true
  storeExplodeJoinData: true
  weatherOriginDepthHours: 7              # Fenêtre météo origine : 7h avant départ
  weatherDestinationDepthHours: 7         # Fenêtre météo destination : 7h avant arrivée
  maxCategoricalCardinality: 280          # Seuil catégorielle vs numérique
  handleInvalid: "keep"                   # Gestion valeurs inconnues
```

**Explication dxCol** :
- **D1** : Retard au départ (DEP_DELAY > 0)
- **D2_60** : Retard au départ > 60 minutes
- **D3** : Retard à l'arrivée (ARR_DELAY > 0)
- **D4** : Retard à l'arrivée > 15 minutes

**Fenêtres météo** :
```scala
weatherOriginDepthHours: 7    // Récupère données météo de 7h avant le départ
weatherDestinationDepthHours: 7  // Récupère données météo de 7h avant l'arrivée prévue
```
Justification :
- 7h permet de capturer l'évolution météo (fronts, orages)
- Pas trop large pour éviter explosion des données (join temporel)

**Features sélectionnées** :
```yaml
flightSelectedFeatures:
  CRS_DEP_TIME:                           # Heure de départ prévue
    transformation: "None"
  OP_CARRIER_AIRLINE_ID:                  # ID compagnie
    transformation: "StringIndexer"
  ORIGIN_AIRPORT_ID:                      # Aéroport origine
    transformation: "StringIndexer"
  DEST_AIRPORT_ID:                        # Aéroport destination
    transformation: "StringIndexer"
  CRS_ELAPSED_TIME:                       # Durée prévue du vol
    transformation: "None"
  feature_departure_hour_rounded_sin:     # Encoding cyclique heure départ
    transformation: "None"
  feature_departure_hour_rounded_cos:
    transformation: "None"
  feature_flight_week_of_year_sin:        # Encoding cyclique semaine année
    transformation: "None"
  feature_flight_week_of_year_cos:
    transformation: "None"

weatherSelectedFeatures:
  feature_weather_type:                   # Type de météo
    transformation: "StringIndexer"
  feature_flight_category_ordinal:        # VFR/MVFR/IFR/LIFR (0/1/2/3)
    transformation: "None"
  HourlyPrecip:                           # Précipitations
    transformation: "None"
  RelativeHumidity:                       # Humidité
    transformation: "None"
  # ... (voir fichier complet)

aggregatedSelectedFeatures:               # Agrégations sur fenêtre météo
  HourlyPrecip:
    aggregation: "sum"                    # Cumul précipitations sur 7h
    transformation: "None"
  RelativeHumidity:
    aggregation: "avg"                    # Moyenne humidité sur 7h
    transformation: "None"
```

#### 4.1.4 Configuration Modèle et Hyperparamètres

```yaml
# Lignes 128-148 de local-d3_7_7-config.yml
model:
  modelType: "randomforest"               # "randomforest", "gbt", "xgboost", "lr"
  hyperparameters:
    numTrees: [15]                        # Array pour grid search
    maxDepth: [7]
    maxBins: [280]                        # >= maxCategoricalCardinality
    minInstancesPerNode: [5]
    subsamplingRate: [1.0]
    featureSubsetStrategy: ["all"]        # Ou ["auto","all","sqrt"]
    impurity: "gini"                      # Scalar (pas array)

train:
  trainRatio: 0.75                        # 75% train, 25% test

  crossValidation:
    numFolds: 4                           # K-Fold CV avec 4 folds

  gridSearch:
    enabled: true                         # Activer grid search
    evaluationMetric: "f1"                # Métrique d'optimisation
```

**Choix trainRatio 0.75** :
- 75% dev set → ~180k échantillons pour training
- 25% test set → ~60k échantillons pour évaluation finale
- Compromis équilibré : assez de données pour training, test set représentatif

**Choix numFolds: 4** :
- 4-fold CV = split en 4 parties
- Chaque fold utilise 3/4 pour train, 1/4 pour validation
- 4 folds = bon compromis temps/qualité estimation
  - 5 ou 10 folds → Meilleure estimation mais + lent
  - 3 folds → Plus rapide mais estimation moins stable

**evaluationMetric: "f1"** :
- F1-Score = moyenne harmonique de précision et recall
- Adapté aux classes déséquilibrées (même après balancement)
- Alternatives possibles :
  - `"accuracy"` : Précision globale
  - `"auc_roc"` : Aire sous courbe ROC
  - `"recall"` : Maximiser la détection des retards

### 4.2 Case Class de Configuration

**Localisation** : `src/main/scala/com/flightdelay/config/HyperparametersConfig.scala` (56 lignes)

```scala
// Lignes 35-55 de HyperparametersConfig.scala
case class HyperparametersConfig(
  // Tree-based model params (optional)
  numTrees: Option[Seq[Int]] = None,
  maxDepth: Option[Seq[Int]] = None,
  maxBins: Option[Seq[Int]] = None,
  minInstancesPerNode: Option[Seq[Int]] = None,
  subsamplingRate: Option[Seq[Double]] = None,
  featureSubsetStrategy: Option[Seq[String]] = None,
  impurity: Option[String] = None,

  // GBT/XGBoost shared params
  stepSize: Option[Seq[Double]] = None,    // Learning rate
  maxIter: Option[Seq[Int]] = None,        // Max iterations

  // Logistic Regression specific
  regParam: Option[Seq[Double]] = None,
  elasticNetParam: Option[Seq[Double]] = None,

  // XGBoost specific
  alpha: Option[Seq[Double]] = None,       // L1
  lambda: Option[Seq[Double]] = None,      // L2
  gamma: Option[Seq[Double]] = None,       // Min loss reduction
  colsampleBytree: Option[Seq[Double]] = None
)
```

**Design pattern** :
- Tous les paramètres sont `Option[Seq[...]]`
- `Option` : Permet de ne pas spécifier un paramètre (utilise défaut)
- `Seq` : Array pour grid search (teste toutes les combinaisons)
- Exemple :
  ```yaml
  numTrees: [100, 200, 300]  # Grid search testera 3 valeurs
  impurity: "gini"            # Scalar, pas de grid search
  ```

---

## 5. Stratégie de Validation

### 5.1 Hold-out Test Set + K-Fold CV (Architecture adoptée)

**Localisation** : `src/main/scala/com/flightdelay/ml/MLPipeline.scala` lignes 19-33

```scala
/**
 * Architecture Option B : Hold-out test set + K-fold CV
 * 1. Split initial 80/20 (dev/test)
 * 2. K-fold CV + Grid Search sur 80% dev
 * 3. Entraîner modèle final sur 80% dev avec best params
 * 4. Évaluation finale sur 20% test hold-out
 */
```

#### 5.1.1 Pourquoi cette Architecture ?

**Avantages** :
1. **Séparation stricte** : Le test set ne participe JAMAIS à l'entraînement ou à l'optimisation
2. **Estimation robuste** : K-fold CV sur dev set donne estimation fiable de la performance
3. **Évaluation non biaisée** : Hold-out test set = performance réelle en production
4. **Best practice** : Architecture recommandée pour production ML

**Alternatives écartées** :

##### Option A : Simple Train/Test Split
```
Données complètes
    ↓
80% train | 20% test
    ↓           ↓
  Train       Eval
```
**Problème** : Estimation instable (1 seul split aléatoire)

##### Option C : K-Fold CV uniquement
```
Données complètes
    ↓
K-Fold CV (5 folds)
    ↓
Moyenne des métriques
```
**Problème** : Modèle final évalué sur données ayant servi à optimiser hyperparamètres (bias optimiste)

### 5.2 Implémentation de la K-Fold CV

**Localisation** : `src/main/scala/com/flightdelay/ml/training/CrossValidator.scala` (591 lignes)

#### 5.2.1 Trois Modes de Validation

```scala
// Lignes 24-52 de CrossValidator.scala
def validate(devData: DataFrame, experiment: ExperimentConfig): CVResult = {
  val numFolds = experiment.train.crossValidation.numFolds

  if (numFolds <= 1) {
    // Mode 1 : Simple train/test split (debugging)
    if (experiment.train.gridSearch.enabled) {
      return validateSimpleTrainTestWithGridSearch(devData, experiment)
    } else {
      return validateSimpleTrainTest(devData, experiment)
    }
  }

  // Mode 2 et 3 : K-Fold CV
  if (experiment.train.gridSearch.enabled) {
    validateWithGridSearch(devData, experiment, numFolds)
  } else {
    validateSimple(devData, experiment, numFolds)
  }
}
```

##### Mode 1 : Simple Train/Test (numFolds ≤ 1, gridSearch=false)
- Usage : Debugging rapide
- Split 80/20 unique
- 1 seul entraînement
- **Ligne 59-136** : `validateSimpleTrainTest()`

##### Mode 2 : Simple Train/Test + Grid Search (numFolds ≤ 1, gridSearch=true)
- Usage : Grid search rapide sans K-fold
- Split 80/20 unique
- Teste chaque combo d'hyperparamètres 1 fois
- **Beaucoup plus rapide** que K-fold grid search
- **Ligne 143-243** : `validateSimpleTrainTestWithGridSearch()`

##### Mode 3 : K-Fold CV + Grid Search (numFolds > 1, gridSearch=true)
- Usage : Production, évaluation robuste
- Chaque combo testé sur K folds
- **Très lent** mais estimation fiable
- **Ligne 412-449** : `validateWithGridSearch()`

#### 5.2.2 Algorithme K-Fold CV

```scala
// Lignes 248-328 de CrossValidator.scala (simplifié)
private def validateSimple(devData: DataFrame, experiment: ExperimentConfig, numFolds: Int): CVResult = {

  // 1. Ajouter colonne "fold" (0 à numFolds-1)
  val dataWithFold = devData.withColumn("fold", (rand(seed) * numFolds).cast("int"))

  // 2. Boucle sur chaque fold
  val foldMetrics = (0 until numFolds).map { foldIdx =>

    // 3. Split : train = tous sauf foldIdx, val = foldIdx
    val trainFold = dataWithFold.filter(col("fold") =!= foldIdx).drop("fold")
    val valFold = dataWithFold.filter(col("fold") === foldIdx).drop("fold")

    // 4. Entraîner modèle
    val model = ModelFactory.create(experiment)
    val trainedModel = model.train(trainFold)

    // 5. Évaluer sur validation fold
    val tempModelPath = s"$basePath/temp_cv_fold_${foldIdx}"
    trainedModel.write.save(tempModelPath)  // Save/reload pour éviter OOM
    val reloadedModel = PipelineModel.load(tempModelPath)
    val predictions = reloadedModel.transform(valFold)
    val metrics = ModelEvaluator.evaluate(predictions)

    // 6. Cleanup
    fs.delete(tempModelPath, true)

    metrics
  }

  // 7. Calculer moyennes et écart-types
  val (avgMetrics, stdMetrics) = calculateStatistics(foldMetrics)

  CVResult(avgMetrics, stdMetrics, foldMetrics, Map.empty, numFolds)
}
```

**Détails importants** :

##### Assignment aléatoire des folds
```scala
val dataWithFold = devData.withColumn("fold", (rand(seed) * numFolds).cast("int"))
```
- `rand(seed)` : Nombre aléatoire [0, 1) avec seed fixe (reproductibilité)
- `* numFolds` : Ramène à [0, numFolds)
- `.cast("int")` : Arrondi vers le bas → {0, 1, 2, ..., numFolds-1}
- **Résultat** : Distribution uniforme sur les folds

##### Sauvegarde/rechargement du modèle
```scala
// Lignes 278-313 de CrossValidator.scala
val tempModelPath = s"${config.common.output.basePath}/${experiment.name}/model/temp_cv_fold_${foldIdx}"

trainedModel match {
  case pm: PipelineModel =>
    pm.write.overwrite().save(tempModelPath)
    val reloadedModel = PipelineModel.load(tempModelPath)
    val predictions = reloadedModel.transform(valFold)
  case _ =>
    // Fallback : évaluer directement (risque OOM)
}
```
**Pourquoi** : Voir section 8.1 (Optimisations mémoire)

##### Calcul statistiques
```scala
// Lignes 529-576 de CrossValidator.scala
private def calculateStatistics(foldMetrics: Seq[EvaluationMetrics]): (EvaluationMetrics, EvaluationMetrics) = {
  val n = foldMetrics.length.toDouble

  // Moyennes
  val avgAccuracy = foldMetrics.map(_.accuracy).sum / n
  val avgF1 = foldMetrics.map(_.f1Score).sum / n
  // ...

  // Écart-types
  val stdAccuracy = math.sqrt(foldMetrics.map(m => math.pow(m.accuracy - avgAccuracy, 2)).sum / n)
  val stdF1 = math.sqrt(foldMetrics.map(m => math.pow(m.f1Score - avgF1, 2)).sum / n)
  // ...

  (avgMetrics, stdMetrics)
}
```
**Formules** :
- Moyenne : `μ = Σ(x_i) / n`
- Écart-type : `σ = sqrt(Σ(x_i - μ)² / n)`

---

## 6. Grid Search et Optimisation

### 6.1 Principe du Grid Search

**Grid Search** = recherche exhaustive sur une grille d'hyperparamètres.

**Exemple** :
```yaml
hyperparameters:
  numTrees: [100, 200, 300]      # 3 valeurs
  maxDepth: [5, 10, 15]          # 3 valeurs
  maxBins: [32, 64]              # 2 valeurs
```

**Grille générée** : 3 × 3 × 2 = **18 combinaisons**

```
{numTrees: 100, maxDepth: 5, maxBins: 32}
{numTrees: 100, maxDepth: 5, maxBins: 64}
{numTrees: 100, maxDepth: 10, maxBins: 32}
...
{numTrees: 300, maxDepth: 15, maxBins: 64}
```

**Avec K-Fold CV** : Chaque combinaison testée K fois → 18 × 4 = **72 entraînements** !

### 6.2 Construction de la Grille

**Localisation** : `src/main/scala/com/flightdelay/ml/training/CrossValidator.scala` lignes 451-527

```scala
private def buildParameterGrid(experiment: ExperimentConfig): Seq[Map[String, Any]] = {
  val hp = experiment.model.hyperparameters
  val modelType = experiment.model.modelType.toLowerCase

  modelType match {
    case "randomforest" | "rf" =>
      val numTreesValues = hp.numTrees.getOrElse(Seq(100))
      val maxDepthValues = hp.maxDepth.getOrElse(Seq(5))
      val maxBinsValues = hp.maxBins.getOrElse(Seq(32))
      val minInstancesPerNodeValues = hp.minInstancesPerNode.getOrElse(Seq(1))
      val subsamplingRateValues = hp.subsamplingRate.getOrElse(Seq(1.0))
      val featureSubsetStrategyValues = hp.featureSubsetStrategy.getOrElse(Seq("auto"))
      val impurityValue = hp.impurity.getOrElse("gini")

      // Produit cartésien
      val combinations = for {
        numTrees <- numTreesValues
        maxDepth <- maxDepthValues
        maxBins <- maxBinsValues
        minInstancesPerNode <- minInstancesPerNodeValues
        subsamplingRate <- subsamplingRateValues
        featureSubsetStrategy <- featureSubsetStrategyValues
      } yield Map[String, Any](
        "numTrees" -> numTrees,
        "maxDepth" -> maxDepth,
        "maxBins" -> maxBins,
        "minInstancesPerNode" -> minInstancesPerNode,
        "subsamplingRate" -> subsamplingRate,
        "featureSubsetStrategy" -> featureSubsetStrategy,
        "impurity" -> impurityValue
      )
      combinations

    case "gbt" | "gradientboostedtrees" =>
      // Similar logic for GBT...

    case "logisticregression" | "lr" =>
      // Similar logic for LR...
  }
}
```

**Produit cartésien** :
- Compréhension Scala avec plusieurs `<-`
- Génère toutes les combinaisons possibles
- Retourne `Seq[Map[String, Any]]`

### 6.3 Exécution du Grid Search

**Localisation** : `src/main/scala/com/flightdelay/ml/training/CrossValidator.scala` lignes 412-449

```scala
private def validateWithGridSearch(
  devData: DataFrame,
  experiment: ExperimentConfig,
  numFolds: Int
): CVResult = {

  info(s"[Grid Search] Building parameter grid...")
  val paramGrid = buildParameterGrid(experiment)

  info(s"  - Total combinations: ${paramGrid.size}")
  info(s"  - Evaluation metric: ${experiment.train.gridSearch.evaluationMetric}")

  // Tester chaque combinaison
  val gridResults = paramGrid.zipWithIndex.map { case (params, idx) =>
    info(s"[Grid Search] Testing combination ${idx + 1}/${paramGrid.size}")
    params.foreach { case (k, v) => info(s"    $k: $v") }

    // K-Fold CV avec ces hyperparamètres
    val cvResult = validateWithParams(devData, experiment, params, numFolds)
    val metricValue = getMetricValue(cvResult.avgMetrics, experiment.train.gridSearch.evaluationMetric)

    info(f"    → Avg ${experiment.train.gridSearch.evaluationMetric}: $metricValue%.4f")

    (params, cvResult, metricValue)
  }

  // Sélectionner la meilleure combinaison
  val (bestParams, bestCVResult, bestMetricValue) = gridResults.maxBy(_._3)

  info(s"=" * 80)
  info("[Grid Search] BEST COMBINATION FOUND")
  info("=" * 80)
  bestParams.toSeq.sortBy(_._1).foreach { case (k, v) =>
    info(f"  $k%-25s : $v")
  }
  info(f"  Best ${experiment.train.gridSearch.evaluationMetric}%-25s : $bestMetricValue%.6f")

  bestCVResult.copy(bestHyperparameters = bestParams)
}
```

**Flux d'exécution** :
```
Pour chaque combinaison d'hyperparamètres:
  1. Log de la combinaison
  2. K-Fold CV avec ces hyperparamètres (validateWithParams)
  3. Calcul de la métrique d'évaluation (F1, accuracy, etc.)
  4. Stocker (params, cvResult, metricValue)

Sélection du meilleur:
  5. maxBy(metricValue) → meilleure combinaison
  6. Log de la meilleure combinaison
  7. Return CVResult avec bestHyperparameters
```

### 6.4 Extraction de la Métrique d'Évaluation

```scala
// Lignes 578-590 de CrossValidator.scala
private def getMetricValue(metrics: EvaluationMetrics, metricName: String): Double = {
  metricName.toLowerCase match {
    case "accuracy" => metrics.accuracy
    case "precision" => metrics.precision
    case "recall" => metrics.recall
    case "f1" | "f1_score" => metrics.f1Score
    case "auc" | "auc_roc" | "auroc" => metrics.areaUnderROC
    case "auc_pr" | "aupr" => metrics.areaUnderPR
    case unknown =>
      println(s"  ⚠ Unknown metric: $unknown, defaulting to F1")
      metrics.f1Score
  }
}
```

**Métriques supportées** :
- `"accuracy"` : % de prédictions correctes
- `"precision"` : TP / (TP + FP)
- `"recall"` : TP / (TP + FN)
- `"f1"` : 2 * (precision * recall) / (precision + recall)
- `"auc_roc"` : Aire sous courbe ROC
- `"auc_pr"` : Aire sous courbe Precision-Recall

**Choix de la métrique** :
- **F1** (défaut) : Bon compromis precision/recall, robuste au déséquilibre
- **Recall** : Si on veut maximiser la détection des retards (accepter faux positifs)
- **Precision** : Si on veut minimiser les fausses alertes
- **AUC-ROC** : Performance globale à tous les seuils

### 6.5 Optimisation : Grid Search Simple (sans K-Fold)

**Localisation** : `src/main/scala/com/flightdelay/ml/training/CrossValidator.scala` lignes 143-243

```scala
private def validateSimpleTrainTestWithGridSearch(
  devData: DataFrame,
  experiment: ExperimentConfig
): CVResult = {

  val paramGrid = buildParameterGrid(experiment)
  val trainRatio = experiment.train.trainRatio

  info(s"  - Total combinations: ${paramGrid.size}")
  info(s"  - Mode: Simple train/validation split (NO K-fold)")
  info(f"  - Split ratio: ${trainRatio * 100}%.0f%% train / ${(1-trainRatio) * 100}%.0f%% validation")

  // Split 80/20 une seule fois
  val Array(trainDataTemp, valDataTemp) = devData.randomSplit(Array(trainRatio, 1-trainRatio), seed)

  // CRITICAL: Cache pour éviter recomputation
  val trainData = trainDataTemp.cache()
  val valData = valDataTemp.cache()
  trainData.count()  // Materialize cache
  valData.count()

  // Tester toutes les combinaisons sur ce split unique
  val gridResults = paramGrid.zipWithIndex.map { case (params, idx) =>
    val trainedModel = Trainer.trainWithParams(trainData, experiment, params)

    // Save/reload to avoid OOM
    val tempModelPath = s"$basePath/temp_gridsearch_${idx}"
    trainedModel.write.save(tempModelPath)
    val reloadedModel = PipelineModel.load(tempModelPath)

    val predictions = reloadedModel.transform(valData)
    val metrics = ModelEvaluator.evaluate(predictions)
    val metricValue = getMetricValue(metrics, experiment.train.gridSearch.evaluationMetric)

    fs.delete(tempModelPath, true)  // Cleanup

    (params, metrics, metricValue)
  }

  // Best combination
  val (bestParams, bestMetrics, bestMetricValue) = gridResults.maxBy(_._3)

  // Unpersist cache
  trainData.unpersist()
  valData.unpersist()

  CVResult(bestMetrics, zeroMetrics, Seq(bestMetrics), bestParams, numFolds=1)
}
```

**Avantages de cette approche** :
- **Beaucoup plus rapide** : Chaque combo testé 1 fois au lieu de K fois
  - K=4 folds, 18 combos : 72 entraînements → 18 entraînements (4x plus rapide !)
- **Bon pour exploration initiale** : Trouver rapidement les hyperparamètres prometteurs
- **Cache des données** : train/val split une fois, réutilisé pour toutes les combos

**Inconvénients** :
- **Estimation moins stable** : 1 seul split aléatoire
- **Risque de chance** : Split favorable/défavorable peut fausser les résultats

**Usage recommandé** :
1. Grid search simple pour exploration rapide
2. Grid search K-fold sur les meilleurs hyperparamètres pour validation finale

---

## 7. Évaluation des Modèles

### 7.1 Métriques Calculées

**Localisation** : `src/main/scala/com/flightdelay/ml/evaluation/ModelEvaluator.scala` (366 lignes)

#### 7.1.1 Case Class EvaluationMetrics

```scala
// Lignes 18-49 de ModelEvaluator.scala
case class EvaluationMetrics(
  accuracy: Double,         // Précision globale
  precision: Double,        // Précision classe positive
  recall: Double,           // Rappel classe positive
  f1Score: Double,          // F1-Score
  areaUnderROC: Double,     // AUC-ROC
  areaUnderPR: Double,      // AUC-PR
  truePositives: Long,      // TP
  trueNegatives: Long,      // TN
  falsePositives: Long,     // FP
  falseNegatives: Long      // FN
) {
  // Métriques dérivées
  def specificity: Double = {
    if (trueNegatives + falsePositives == 0) 0.0
    else trueNegatives.toDouble / (trueNegatives + falsePositives)
  }

  def falsePositiveRate: Double = 1.0 - specificity

  // RECo: Recall for On-time flights (class 0)
  def recallOnTime: Double = specificity

  // RECd: Recall for Delayed flights (class 1)
  def recallDelayed: Double = {
    if (truePositives + falseNegatives == 0) 0.0
    else truePositives.toDouble / (truePositives + falseNegatives)
  }
}
```

#### 7.1.2 Calcul des Métriques

```scala
// Lignes 57-136 de ModelEvaluator.scala
def evaluate(predictions: DataFrame, datasetType: String): EvaluationMetrics = {

  val cachedPredictions = predictions.cache()

  // 1. Confusion Matrix
  val confusionMatrix = cachedPredictions
    .groupBy("label", "prediction")
    .count()
    .collect()
    .map(row => ((row.getDouble(0), row.getDouble(1)), row.getLong(2)))
    .toMap

  val tp = confusionMatrix.getOrElse((1.0, 1.0), 0L)  // Vrai retard prédit retard
  val tn = confusionMatrix.getOrElse((0.0, 0.0), 0L)  // Vrai à l'heure prédit à l'heure
  val fp = confusionMatrix.getOrElse((0.0, 1.0), 0L)  // Vrai à l'heure prédit retard
  val fn = confusionMatrix.getOrElse((1.0, 0.0), 0L)  // Vrai retard prédit à l'heure

  // 2. Multiclass metrics
  val multiclassEval = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")

  val accuracy = multiclassEval.setMetricName("accuracy").evaluate(cachedPredictions)

  // 3. Calculate precision, recall, F1 manually (plus fiable)
  val precision = if (tp + fp > 0) tp.toDouble / (tp + fp) else 0.0
  val recall = if (tp + fn > 0) tp.toDouble / (tp + fn) else 0.0
  val f1 = if (precision + recall > 0) 2.0 * (precision * recall) / (precision + recall) else 0.0

  // 4. Binary classification metrics (AUC)
  val binaryEval = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setRawPredictionCol("rawPrediction")

  val auc = binaryEval.setMetricName("areaUnderROC").evaluate(cachedPredictions)
  val aupr = binaryEval.setMetricName("areaUnderPR").evaluate(cachedPredictions)

  EvaluationMetrics(accuracy, precision, recall, f1, auc, aupr, tp, tn, fp, fn)
}
```

**Pourquoi calcul manuel precision/recall/F1 ?**
```scala
// Lignes 88-99 de ModelEvaluator.scala
// Calculate precision, recall, and F1 manually from confusion matrix for class 1 (delayed)
// This avoids issues with weighted metrics producing identical values
val precision = if (tp + fp > 0) tp.toDouble / (tp + fp) else 0.0
val recall = if (tp + fn > 0) tp.toDouble / (tp + fn) else 0.0
val f1 = if (precision + recall > 0) 2.0 * (precision * recall) / (precision + recall) else 0.0
```

**Raison** :
- Spark MLlib `weightedPrecision` et `weightedRecall` peuvent produire des valeurs identiques
- Calcul manuel via confusion matrix plus fiable et explicite
- Focus sur classe 1 (delayed) car c'est notre classe d'intérêt

#### 7.1.3 Formules des Métriques

##### Accuracy (Précision Globale)
```
Accuracy = (TP + TN) / (TP + TN + FP + FN)
```
- % de prédictions correctes
- **Problème** : Trompeuse si classes déséquilibrées
  - Exemple : 90% vols à l'heure → prédire toujours "à l'heure" = 90% accuracy !

##### Precision (Précision)
```
Precision = TP / (TP + FP)
```
- Parmi les vols prédits "en retard", % vraiment en retard
- **Interprétation** : Fiabilité des alertes de retard
  - Precision élevée → Peu de fausses alertes

##### Recall (Rappel / Sensibilité)
```
Recall = TP / (TP + FN)
```
- Parmi les vols vraiment en retard, % correctement détectés
- **Interprétation** : Capacité à détecter les retards
  - Recall élevé → On rate peu de retards

##### F1-Score
```
F1 = 2 * (Precision * Recall) / (Precision + Recall)
```
- Moyenne harmonique de Precision et Recall
- **Avantage** : Pénalise les déséquilibres (si l'un est faible, F1 est faible)
- **Usage** : Métrique principale pour classes déséquilibrées

##### AUC-ROC (Area Under ROC Curve)
```
ROC = Courbe (FPR, TPR) pour différents seuils
AUC-ROC = Aire sous cette courbe
```
- **TPR** (True Positive Rate) = Recall = TP / (TP + FN)
- **FPR** (False Positive Rate) = FP / (FP + TN)
- **Valeur** : 0.5 (random) à 1.0 (parfait)
- **Interprétation** : Performance globale indépendante du seuil
  - AUC = 0.85 → Bonne discrimination retard/on-time

##### AUC-PR (Area Under Precision-Recall Curve)
```
PR = Courbe (Recall, Precision) pour différents seuils
AUC-PR = Aire sous cette courbe
```
- **Avantage vs ROC** : Plus sensible aux classes déséquilibrées
- **Usage** : Préférable si classe positive minoritaire

##### Specificity (Spécificité)
```
Specificity = TN / (TN + FP)
```
- Recall pour classe négative (on-time)
- % de vols à l'heure correctement identifiés

##### RECo et RECd
```
RECo (Recall On-time) = Specificity = TN / (TN + FP)
RECd (Recall Delayed) = Recall = TP / (TP + FN)
```
- **RECo** : Performance sur classe 0 (on-time)
- **RECd** : Performance sur classe 1 (delayed)
- **Idéal** : RECo et RECd tous deux élevés (≥ 80%)

#### 7.1.4 Affichage des Métriques

```scala
// Lignes 141-164 de ModelEvaluator.scala
private def displayMetrics(metrics: EvaluationMetrics): Unit = {
  info("--- Classification Metrics ---")
  info(f"Accuracy:            ${metrics.accuracy * 100}%6.2f%%")
  info(f"Precision:           ${metrics.precision * 100}%6.2f%%")
  info(f"Recall (Weighted):   ${metrics.recall * 100}%6.2f%%")
  info(f"F1-Score:            ${metrics.f1Score * 100}%6.2f%%")
  info(f"AUC-ROC:             ${metrics.areaUnderROC}%6.4f")
  info(f"AUC-PR:              ${metrics.areaUnderPR}%6.4f")

  info("--- Per-Class Recall ---")
  info(f"RECd (Delayed):      ${metrics.recallDelayed * 100}%6.2f%%  [TP/(TP+FN)]")
  info(f"RECo (On-time):      ${metrics.recallOnTime * 100}%6.2f%%  [TN/(TN+FP)]")

  info("--- Confusion Matrix ---")
  info(f"True Positives:      ${metrics.truePositives}%,10d")
  info(f"True Negatives:      ${metrics.trueNegatives}%,10d")
  info(f"False Positives:     ${metrics.falsePositives}%,10d")
  info(f"False Negatives:     ${metrics.falseNegatives}%,10d")
}
```

**Exemple de sortie** :
```
==================================================================================
[STEP 4] [Hold-out Test]  -- Model Evaluation
==================================================================================
--- Classification Metrics ---
Accuracy:             82.45%
Precision:            79.23%
Recall (Weighted):    85.67%
F1-Score:             82.31%
AUC-ROC:              0.8956
AUC-PR:               0.8723

--- Per-Class Recall ---
RECd (Delayed):       85.67%  [TP/(TP+FN)]
RECo (On-time):       79.23%  [TN/(TN+FP)]

--- Confusion Matrix ---
True Positives:          42,567
True Negatives:          39,234
False Positives:         10,256
False Negatives:          7,123
Total Predictions:       99,180
==================================================================================
```

### 7.2 Sauvegarde des Métriques

#### 7.2.1 Fichiers CSV Générés

**Localisation** : `src/main/scala/com/flightdelay/ml/MLPipeline.scala` lignes 725-820

##### cv_fold_metrics.csv
```csv
fold,accuracy,precision,recall,f1_score,auc_roc,auc_pr
1,0.823456,0.789012,0.856789,0.821234,0.895678,0.872345
2,0.831234,0.793456,0.862345,0.826789,0.901234,0.878901
3,0.819876,0.785678,0.851234,0.817890,0.891234,0.868765
4,0.827654,0.791234,0.859012,0.823456,0.898765,0.875432
```

##### cv_summary.csv
```csv
metric,mean,std
accuracy,0.825555,0.004123
precision,0.789845,0.002987
recall,0.857345,0.003856
f1_score,0.822342,0.003045
auc_roc,0.896728,0.003621
```

##### holdout_test_metrics.csv
```csv
metric,value
accuracy,0.824567
precision,0.792345
recall,0.856789
f1_score,0.823123
auc_roc,0.895678
auc_pr,0.872345
recall_delayed,0.856789
recall_ontime,0.792345
true_positives,42567
true_negatives,39234
false_positives,10256
false_negatives,7123
```

##### best_hyperparameters.csv
```csv
parameter,value
numTrees,200
maxDepth,10
maxBins,280
minInstancesPerNode,5
subsamplingRate,1.0
featureSubsetStrategy,auto
impurity,gini
```

##### holdout_roc_data.csv
```csv
label,prob_positive,prediction
0.0,0.123456,0.0
1.0,0.876543,1.0
0.0,0.234567,0.0
1.0,0.987654,1.0
...
```
(Limité à 10,000 échantillons pour performance)

#### 7.2.2 Résumé Textuel (training_summary.txt)

```scala
// Lignes 826-941 de MLPipeline.scala
private def saveTrainingSummary(
  experiment: ExperimentConfig,
  cvResult: CVResult,
  holdOutMetrics: EvaluationMetrics,
  totalTime: Double,
  basePath: String,
  fast: Boolean
): Unit = {
  val summary = new StringBuilder()

  summary.append("=" * 100 + "\n")
  summary.append("TRAINING SUMMARY\n")
  summary.append("=" * 100 + "\n\n")

  summary.append("EXPERIMENT INFORMATION\n")
  summary.append("-" * 100 + "\n")
  summary.append(s"Name:        ${experiment.name}\n")
  summary.append(s"Description: ${experiment.description}\n")
  summary.append(s"Model Type:  ${experiment.model.modelType.toUpperCase}\n")
  // ...

  summary.append("HYPERPARAMETERS\n")
  // ...

  summary.append("CROSS-VALIDATION RESULTS\n")
  // ...

  summary.append("HOLD-OUT TEST SET RESULTS\n")
  // ...

  summary.append("CONFUSION MATRIX (Test Set)\n")
  // ...

  summary.append("TRAINING TIME\n")
  summary.append(f"  Total Time: $totalTime%.2f seconds (${totalTime / 60}%.2f minutes)\n")

  // Write to file using Hadoop FileSystem
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val out = fs.create(new Path(s"$basePath/metrics/training_summary.txt"), true)
  // ...
}
```

**Exemple de fichier** :
```
====================================================================================================
TRAINING SUMMARY
====================================================================================================

EXPERIMENT INFORMATION
----------------------------------------------------------------------------------------------------
Name:        Experience-local-D3-7-7
Description: Baseline Random Forest with hyperparameters tuning
Model Type:  RANDOMFOREST
Target:      is_delayed

HYPERPARAMETERS
----------------------------------------------------------------------------------------------------
  numTrees                      : 200
  maxDepth                      : 10
  maxBins                       : 280
  minInstancesPerNode           : 5
  subsamplingRate               : 1.0
  featureSubsetStrategy         : auto
  impurity                      : gini

CROSS-VALIDATION RESULTS
----------------------------------------------------------------------------------------------------
Number of Folds: 4

  Metric       Mean          Std Dev
  ==================================================
  Accuracy      82.56%      ±  0.41%
  Precision     78.98%      ±  0.30%
  Recall        85.73%      ±  0.39%
  F1-Score      82.23%      ±  0.30%
  AUC-ROC       0.8967       ±  0.0036

HOLD-OUT TEST SET RESULTS
----------------------------------------------------------------------------------------------------
  Accuracy:      82.46%
  Precision:     79.23%
  Recall:        85.68%
  F1-Score:      82.31%
  AUC-ROC:       0.8957
  RECd:          85.68%  (Recall Delayed)
  RECo:          79.23%  (Recall On-time)

CONFUSION MATRIX (Test Set)
----------------------------------------------------------------------------------------------------
  True Positives:       42,567
  True Negatives:       39,234
  False Positives:      10,256
  False Negatives:       7,123

TRAINING TIME
----------------------------------------------------------------------------------------------------
  Total Time: 1234.56 seconds (20.58 minutes)

====================================================================================================
Generated: 2025-01-15T14:23:45
====================================================================================================
```

---

## 8. Gestion de la Mémoire et Optimisations

### 8.1 Problème : Broadcast OOM avec Grands Modèles

#### 8.1.1 Symptômes

Lors de l'entraînement de Random Forest avec beaucoup d'arbres (200-300), erreur :

```
org.apache.spark.SparkException: Job aborted due to stage failure:
Total size of serialized results of XX tasks (4.2 GB) is bigger than maxResultSize (4.0 GB)
```

ou

```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.broadcast.TorrentBroadcast
```

#### 8.1.2 Cause Racine

```scala
// Schéma problématique (AVANT optimisation)
val trainedModel = model.train(trainData)  // Modèle construit en mémoire
val predictions = trainedModel.transform(testData)  // Broadcast du modèle vers executors
```

**Problème** :
1. Random Forest avec 300 arbres → Modèle très large (plusieurs GB)
2. `transform()` broadcast le modèle vers tous les executors
3. Si modèle > `spark.driver.maxResultSize` → Crash

#### 8.1.3 Solution : Save/Reload Pattern

```scala
// Lignes 300-400 de MLPipeline.scala
// OPTIMIZATION: Save final model then reload to avoid broadcast OOM
val modelPath = s"$experimentOutputPath/models/${experiment.model.modelType}_final"

info(s"   Saving final model to: $modelPath")
finalModel.asInstanceOf[PipelineModel].write.overwrite().save(modelPath)

info(s"   Reloading model for evaluation to avoid broadcast...")
val reloadedModel = PipelineModel.load(modelPath)

info(s"   Evaluating on hold-out test set...")
val testPredictions = reloadedModel.transform(testData)
```

**Pourquoi ça fonctionne ?**
1. `write.save()` : Sérialise le modèle sur HDFS/disque
2. `PipelineModel.load()` : Charge le modèle de manière optimisée
   - Spark optimise la désérialisation pour les modèles sauvegardés
   - Distribution plus efficace vers les executors
3. `transform()` : Utilise le modèle rechargé sans broadcast excessif

**Pattern utilisé systématiquement** :
- Dans `MLPipeline.scala` (ligne 300-400) : Évaluation finale
- Dans `CrossValidator.scala` (ligne 278-313) : Chaque fold de CV
- Dans `CrossValidator.scala` (ligne 180-210) : Grid search

### 8.2 Cache des Données pour Grid Search

**Localisation** : `src/main/scala/com/flightdelay/ml/training/CrossValidator.scala` lignes 162-170

```scala
// CRITICAL: Cache train/val data to avoid recomputing DAG for each hyperparameter combo
val trainData = trainDataTemp.cache()
val valData = valDataTemp.cache()

val trainCount = trainData.count()  // Materialize cache
val valCount = valData.count()      // Materialize cache
info(f"  - Train: $trainCount%,d samples | Validation: $valCount%,d samples")
info(s"  - Train/Val data cached to avoid DAG recomputation")
```

**Impact** :
- Grid search avec 18 combinaisons
- Sans cache : 18 recomputations du DAG complet (lectures CSV, jointures, transformations)
- Avec cache : 1 computation + 18 réutilisations mémoire

**Gain de temps** :
- Exemple projet : ~10 minutes/combo SANS cache → 20 secondes/combo AVEC cache
- Avec 18 combos : 180 min → 6 min (30x plus rapide !)

**Nettoyage** :
```scala
// Lignes 229-231 de CrossValidator.scala
trainData.unpersist()
valData.unpersist()
info(s"  - Train/Val data unpersisted to free memory")
```
- Important pour libérer mémoire après grid search
- Évite accumulation de caches si plusieurs expériences

### 8.3 Checkpoint pour Random Forest

**Localisation** : `src/main/scala/com/flightdelay/ml/models/RandomForestModel.scala` lignes 72-75

```scala
// OPTIMISATIONS CRITIQUES
.setCacheNodeIds(true)             // Active le cache (améliore perf)
.setCheckpointInterval(5)         // CRITIQUE : checkpoint tous les 5 arbres
.setMaxMemoryInMB(2048)            // 2 GB (au lieu de 512 MB)
```

#### 8.3.1 Pourquoi le Checkpoint ?

**Problème DAG Spark** :
- Random Forest construit les arbres itérativement
- Chaque arbre ajoute une transformation au DAG Spark
- DAG = Directed Acyclic Graph (graphe de dépendances)
- Après 100 arbres → DAG gigantesque → Stack overflow ou timeout

**Solution Checkpoint** :
```
Arbre 1 → Arbre 2 → Arbre 3 → Arbre 4 → Arbre 5 [CHECKPOINT]
                                           ↓
                                    Matérialiser sur disque
                                    Couper le DAG
                                           ↓
Arbre 6 → Arbre 7 → Arbre 8 → Arbre 9 → Arbre 10 [CHECKPOINT]
```

- Tous les 5 arbres, Spark sauvegarde l'état sur disque
- Cela "coupe" le DAG en le matérialisant
- Les arbres suivants repartent d'un DAG court

**Valeur 5** : Compromis
- Trop fréquent (1-2) → Ralentit (beaucoup d'I/O disque)
- Trop rare (20+) → Risque DAG explosion
- 5 = Sweet spot empirique

#### 8.3.2 Configuration Checkpoint Directory

**Important** : Spark a besoin d'un répertoire pour les checkpoints

```scala
// À configurer dans SparkSession
spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
```

### 8.4 Gestion de la Mémoire XGBoost

**Localisation** : `src/main/scala/com/flightdelay/ml/models/XGBoostModel.scala` lignes 104-119

```scala
val cleanVectorUdf = udf((v: Vector) => {
  if (v == null) {
    Vectors.dense(Array.fill(41)(0.0))
  } else {
    val arr = v.toArray.map { x =>
      if (x.isNaN || x.isInfinity) 0.0 else x
    }
    Vectors.dense(arr)
  }
})

val dfClean = data
  .withColumn("features", cleanVectorUdf(col("features")))
  .na.fill(0.0, Seq("label"))

val model = pipeline.fit(dfClean)
```

**Raisons du nettoyage** :
1. **NaN/Infinity** : XGBoost Spark ne les gère pas bien → crash ou résultats incorrects
2. **Null vectors** : Certaines transformations (PCA sur données manquantes) peuvent produire null
3. **Solution pragmatique** : Remplacer par 0.0
   - Après StandardScaler, 0.0 = valeur moyenne
   - Impact minimal si peu de NaN

---

## 9. MLFlow et Tracking des Expériences

### 9.1 Initialisation MLFlow

**Localisation** : `src/main/scala/com/flightdelay/ml/tracking/MLFlowTracker.scala` (150+ lignes)

#### 9.1.1 Configuration

```yaml
# Lignes 29-31 de local-d3_7_7-config.yml
mlflow:
  enabled: true
  trackingUri: "http://mlflow-server:5000"
```

#### 9.1.2 Initialisation dans MLPipeline

```scala
// Lignes 119-142 de MLPipeline.scala
MLFlowTracker.initialize(configuration.common.mlflow.trackingUri, configuration.common.mlflow.enabled)
val experimentId = MLFlowTracker.getOrCreateExperiment()
val runId = experimentId.flatMap(expId =>
  MLFlowTracker.startRun(expId, experiment.name, Some(experiment.description))
)

// Log experiment configuration
runId.foreach { rid =>
  MLFlowTracker.logParams(rid, Map(
    "experiment_name" -> experiment.name,
    "target" -> experiment.target,
    "model_type" -> experiment.model.modelType,
    "train_ratio" -> experiment.train.trainRatio,
    "cv_folds" -> experiment.train.crossValidation.numFolds,
    "grid_search_enabled" -> experiment.train.gridSearch.enabled,
    "grid_search_metric" -> experiment.train.gridSearch.evaluationMetric,
    "feature_extraction_type" -> experiment.featureExtraction.featureType,
    "pca_enabled" -> experiment.featureExtraction.isPcaEnabled,
    "pca_variance_threshold" -> experiment.featureExtraction.pcaVarianceThreshold,
    "random_seed" -> configuration.common.seed
  ))
  MLFlowTracker.setTag(rid, "experiment_description", experiment.description)
  MLFlowTracker.setTag(rid, "environment", configuration.environment)
}
```

**Paramètres loggés** :
- Configuration expérience (nom, description, target)
- Configuration modèle (type, hyperparamètres)
- Configuration training (CV, grid search)
- Configuration features (type extraction, PCA)
- Métadonnées (seed, environnement)

### 9.2 Logging des Métriques

#### 9.2.1 Métriques Cross-Validation

```scala
// Lignes 248-272 de MLPipeline.scala
runId.foreach { rid =>
  // Log best hyperparameters
  MLFlowTracker.logParams(rid, cvRes.bestHyperparameters)

  // Log per-fold metrics
  cvRes.foldMetrics.zipWithIndex.foreach { case (metrics, fold) =>
    MLFlowTracker.logMetric(rid, s"cv_fold${fold}_accuracy", metrics.accuracy, step = fold)
    MLFlowTracker.logMetric(rid, s"cv_fold${fold}_precision", metrics.precision, step = fold)
    MLFlowTracker.logMetric(rid, s"cv_fold${fold}_recall", metrics.recall, step = fold)
    MLFlowTracker.logMetric(rid, s"cv_fold${fold}_f1", metrics.f1Score, step = fold)
    MLFlowTracker.logMetric(rid, s"cv_fold${fold}_auc", metrics.areaUnderROC, step = fold)
  }

  // Log aggregated CV metrics
  MLFlowTracker.logMetrics(rid, Map(
    "cv_mean_accuracy" -> cvRes.avgMetrics.accuracy,
    "cv_std_accuracy" -> cvRes.stdMetrics.accuracy,
    "cv_mean_precision" -> cvRes.avgMetrics.precision,
    "cv_std_precision" -> cvRes.stdMetrics.precision,
    "cv_mean_recall" -> cvRes.avgMetrics.recall,
    "cv_std_recall" -> cvRes.stdMetrics.recall,
    "cv_mean_f1" -> cvRes.avgMetrics.f1Score,
    "cv_std_f1" -> cvRes.stdMetrics.f1Score,
    "cv_mean_auc" -> cvRes.avgMetrics.areaUnderROC,
    "cv_std_auc" -> cvRes.stdMetrics.areaUnderROC
  ))
}
```

**Organisation MLFlow UI** :
- Onglet "Params" : Hyperparamètres
- Onglet "Metrics" :
  - `cv_fold0_*`, `cv_fold1_*`, ... : Métriques par fold (graphiques temporels)
  - `cv_mean_*`, `cv_std_*` : Agrégations
  - `test_*` : Métriques finales sur test set

#### 9.2.2 Métriques Hold-out Test

```scala
// Lignes 416-426 de MLPipeline.scala
runId.foreach { rid =>
  MLFlowTracker.logMetrics(rid, Map(
    "test_accuracy" -> holdOutMetrics.accuracy,
    "test_precision" -> holdOutMetrics.precision,
    "test_recall" -> holdOutMetrics.recall,
    "test_f1" -> holdOutMetrics.f1Score,
    "test_auc" -> holdOutMetrics.areaUnderROC,
    "test_recall_delayed" -> holdOutMetrics.recallDelayed,
    "test_recall_ontime" -> holdOutMetrics.recallOnTime
  ))
}
```

### 9.3 Logging des Artifacts

**Localisation** : `src/main/scala/com/flightdelay/ml/MLPipeline.scala` lignes 494-572

```scala
// Log artifacts organized in subdirectories
runId.foreach { rid =>
  // 1. Metrics CSVs to "metrics/" subdirectory
  MLFlowTracker.logArtifactWithPath(rid, metricsPath, "metrics")

  // 2. Model to "models/" subdirectory
  MLFlowTracker.logArtifactWithPath(rid, localModelPath, "models")

  // 3. YAML configuration to "configuration/" subdirectory
  MLFlowTracker.logArtifactWithPath(rid, localConfigDestPath, "configuration")

  // 4. Feature files to "features/" subdirectory
  MLFlowTracker.logArtifactWithPath(rid, localFeaturesPath, "features")

  // 5. Visualization plots to "plots/" subdirectory
  MLFlowTracker.logArtifactWithPath(rid, plotsPath, "plots")

  // 6. Execution time metrics to "execution_time/" subdirectory
  MLFlowTracker.logArtifactWithPath(rid, execTimeBasePath, "execution_time")
}
```

**Structure dans MLFlow** :
```
Run: Experience-local-D3-7-7
├── metrics/
│   ├── cv_fold_metrics.csv
│   ├── cv_summary.csv
│   ├── holdout_test_metrics.csv
│   ├── best_hyperparameters.csv
│   └── holdout_roc_data.csv
├── models/
│   └── randomforest_final/
│       ├── metadata/
│       └── stages/
├── configuration/
│   └── local-config.yml
├── features/
│   ├── selected_features.txt
│   └── feature_importances_report.txt
├── plots/
│   ├── cv_folds_detailed.png
│   ├── cv_vs_holdout_comparison.png
│   ├── confusion_matrix.png
│   └── roc_curve.png
└── execution_time/
    ├── execution_times.csv
    └── execution_times.txt
```

### 9.4 Feature Importances Logging

**Localisation** : `src/main/scala/com/flightdelay/ml/MLPipeline.scala` lignes 309-393

```scala
// Save and log feature importances report for tree-based models
val pipelineModel = finalModel.asInstanceOf[PipelineModel]
val baseModel = pipelineModel.stages(0)

val featureImportancesOpt = baseModel match {
  case rf: RandomForestClassificationModel =>
    Some((rf.featureImportances.toArray, "RandomForest"))
  case gbt: GBTClassificationModel =>
    Some((gbt.featureImportances.toArray, "GBT"))
  case _ => None
}

featureImportancesOpt.foreach { case (importances, modelType) =>
  // Load feature names
  val featureNames = loadFeatureNames(s"$experimentOutputPath/features/selected_features.txt")

  // Create report
  val reportPath = s"$experimentOutputPath/features/feature_importances_report.txt"
  saveFeatureImportancesReport(importances, featureNames, reportPath)

  // Log to MLFlow
  runId.foreach { rid =>
    MLFlowTracker.logFeatureImportances(rid, featureNames, importances, topN = Some(20))
  }
}
```

**Format dans MLFlow** :
- Artifact `feature_importances_report.txt` : Rapport textuel détaillé
- Metrics `feature_importance_0`, `feature_importance_1`, ... : Top 20 features avec valeurs

### 9.5 Execution Time Tracking

**Localisation** : `src/main/scala/com/flightdelay/ml/MLPipeline.scala` lignes 556-571

```scala
// Log execution time metrics to MLFlow
if (timeTracker != null) {
  val execTimeBasePath = s"$localExperimentPath/execution_time"

  // Log execution_time directory as artifacts
  MLFlowTracker.logArtifactWithPath(rid, execTimeBasePath, "execution_time")

  // Also log individual execution time metrics as MLFlow metrics
  timeTracker.getAllTimes.foreach { case (stepName, time) =>
    if (!time.isNaN) {
      val metricName = s"exec_time_${stepName.replace(".", "_")}"
      MLFlowTracker.logMetric(rid, metricName, time)
    }
  }
}
```

**Temps trackés** :
- `ml_feature_extraction.dev` : Extraction features dev set
- `ml_feature_extraction.test` : Extraction features test set
- `ml_kfold_cv` : K-Fold CV complète (inclut grid search)
- `ml_train` : Training modèle final
- `ml_evaluation` : Évaluation test set
- `ml_save_metrics` : Sauvegarde métriques
- `ml.total` : Temps total ML pipeline

---

## 10. Visualisations et Rapports

### 10.1 Script de Visualisation Python

**Localisation** : `work/scripts/visualize_ml_pipeline.py` (100+ lignes)

#### 10.1.1 Appel depuis MLPipeline

```scala
// Lignes 957-996 de MLPipeline.scala
private def generatePlots(metricsPath: String): Unit = {
  val scriptPath = s"$scriptsBasePath/visualize_ml_pipeline.py"
  val command = s"python3 $scriptPath $metricsPath"

  // Check if script exists
  val scriptFile = new java.io.File(scriptPath)
  if (!scriptFile.exists()) {
    warn(s"   Warning: Visualization script not found: $scriptPath")
    return
  }

  // Execute Python script
  Try {
    command.!  // Scala process execution
  } match {
    case Success(0) =>
      info(s"   Plots generated successfully in: $metricsPath/plots-ml-pipeline/")
    case Success(exitCode) =>
      warn(s"   Warning: Plot generation failed with exit code: $exitCode")
    case Failure(e) =>
      warn(s"   Warning: Could not execute Python script: ${e.getMessage}")
  }
}
```

#### 10.1.2 Visualisations Générées

**Output directory** : `{metricsPath}/plots-ml-pipeline/`

##### 1. CV Folds Detailed (`cv_folds_detailed.png`)
- Graphique en barres : Métriques de chaque fold
- X-axis : Fold 1, Fold 2, Fold 3, Fold 4
- Y-axis : Valeur métrique (0-1)
- Barres groupées : Accuracy, Precision, Recall, F1, AUC
- **Utilité** : Détecter variance entre folds, identifier folds atypiques

##### 2. CV vs Hold-out Comparison (`cv_vs_holdout_comparison.png`)
- Graphique en barres comparatif
- X-axis : Métriques (Accuracy, Precision, Recall, F1, AUC)
- Barres bleues : CV mean
- Barres oranges : Hold-out test
- Barres d'erreur sur CV : ± std
- **Utilité** : Vérifier cohérence CV/test, détecter overfitting

##### 3. Stability Box Plot (`stability_box_plot.png`)
- Box plots des métriques sur tous les folds
- Montre : Min, Q1, Median, Q3, Max
- **Utilité** : Visualiser stabilité du modèle

##### 4. Confusion Matrix (`confusion_matrix.png`)
- Heatmap 2x2 : Vrai vs Prédit
- Cellules : TP, TN, FP, FN avec valeurs
- **Utilité** : Identifier types d'erreurs (FP vs FN)

##### 5. ROC Curve (`roc_curve.png`)
- Courbe ROC : TPR vs FPR
- Ligne diagonale : Random classifier
- Aire sous courbe (AUC) affichée
- **Utilité** : Évaluer discrimination du modèle

##### 6. Feature Importances (`feature_importances.png`)
- Barres horizontales : Top 20 features
- X-axis : Importance (0-100%)
- **Utilité** : Identifier features les plus prédictives

### 10.2 Scripts de Comparaison d'Expériences

#### 10.2.1 Comparaison Globale

**Localisation** : `work/scripts/visualize_experiments_comparison.py` (591 lignes)

**Usage** :
```bash
python3 visualize_experiments_comparison.py /output/Experience-1 /output/Experience-2 /output/Experience-3
```

**Visualisations générées** :

##### Metrics Comparison (`metrics_comparison.png`)
- Barres groupées : Chaque expérience
- Métriques : Accuracy, Precision, Recall, F1, AUC
- CV (barres bleues) vs Hold-out (barres oranges)

##### Heatmap Comparison (`heatmap_comparison.png`)
- Heatmap : Expériences (lignes) vs Métriques (colonnes)
- Couleurs : Performance (vert = bon, rouge = mauvais)

##### Radar Comparison (`radar_comparison.png`)
- Graphique radar multi-métriques
- 1 ligne par expérience
- **Utilité** : Comparer profils de performance

##### F1 Ranking (`f1_ranking.png`)
- Barres triées par F1 décroissant
- **Utilité** : Identifier meilleure expérience rapidement

##### Hyperparameters Comparison (`hyperparameters_comparison.png`)
- Table visuelle : Expériences vs Hyperparamètres
- **Utilité** : Correler hyperparamètres avec performance

#### 10.2.2 Comparaison de Modèles

**Localisation** : `work/scripts/visualize_model_comparison.py` (591 lignes)

**Différence vs experiments comparison** :
- Extrait le type de modèle depuis le nom de l'expérience
- Groupe les expériences par modèle (RF, GBT, XGBoost, LR)
- Visualisations comparent les modèles plutôt que les expériences individuelles

**Usage** :
```bash
python3 visualize_model_comparison.py /output/RF-Exp1 /output/GBT-Exp1 /output/XGB-Exp1
```

---

## Conclusion

Ce document a détaillé l'intégralité de l'implémentation ML du projet de prédiction des retards de vols. Voici les points clés :

### Architecture
- **Pipeline robuste** : Hold-out test set (20%) + K-Fold CV (4 folds) sur dev set (80%)
- **Protection data leakage** : Feature transformers fit uniquement sur train data
- **Validation en 8 étapes** : De la réception des données à la génération des visualisations

### Algorithmes
- **4 modèles implémentés** : Random Forest, GBT, XGBoost, Logistic Regression
- **Hyperparamètres exhaustifs** : Tous les paramètres majeurs configurables
- **Justification des choix** : Chaque hyperparamètre documenté avec impact et valeurs typiques

### Optimisation
- **Grid Search automatisé** : Teste toutes les combinaisons d'hyperparamètres
- **2 modes de validation** : Simple split (rapide) et K-Fold (robuste)
- **Métriques multiples** : Accuracy, Precision, Recall, F1, AUC-ROC, AUC-PR, RECo, RECd

### Optimisations Techniques
- **Save/Reload pattern** : Évite broadcast OOM sur grands modèles
- **Cache stratégique** : 30x accélération du grid search
- **Checkpoint RF** : Prévient DAG explosion sur modèles larges

### Tracking et Reproductibilité
- **MLFlow intégré** : Tous les paramètres, métriques et artifacts loggés
- **Visualisations automatiques** : 6 plots par expérience + scripts de comparaison
- **Rapports complets** : CSV + TXT pour analyse externe

### Métriques et Évaluation
- **10 métriques calculées** : Couvrant tous les aspects de la classification binaire
- **Confusion matrix** : Analyse détaillée TP/TN/FP/FN
- **Feature importance** : Top 20 features avec visualisation

Ce pipeline ML est prêt pour la production avec :
- Configuration YAML flexible
- Logging exhaustif pour debugging
- Protection contre data leakage
- Optimisations mémoire robustes
- Tracking complet des expériences
- Visualisations professionnelles

**Prochaines étapes possibles** :
1. Ajout de nouveaux modèles (LightGBM, CatBoost)
2. Hyperparameter tuning automatique (Bayesian Optimization)
3. Feature engineering avancé (interactions, polynomial features)
4. Deployment sur Kubernetes avec Spark on K8s
5. Monitoring en production avec MLFlow Model Registry
