# 🔍 Guide de Validation des Valeurs Manquantes dans les Parquets

Ce guide explique comment valider que les fichiers parquet générés contiennent correctement la gestion des valeurs manquantes (sentinelles + flags).

---

## 📋 Table des Matières

1. [Qu'est-ce qui est validé ?](#quest-ce-qui-est-validé-)
2. [Méthode 1: Script Shell (Rapide)](#méthode-1-script-shell-rapide)
3. [Méthode 2: Code Scala](#méthode-2-code-scala)
4. [Méthode 3: Notebook Jupyter](#méthode-3-notebook-jupyter)
5. [Interprétation des Résultats](#interprétation-des-résultats)

---

## ✅ Qu'est-ce qui est validé ?

Le validateur vérifie automatiquement :

### 1️⃣ **Flags par heure** (`_missing_h1` à `_missing_h7`)
- Colonnes binaires (0/1) indiquant si une heure d'observation météo est manquante
- Exemples :
  ```
  origin_weather_missing_h1 = 0  (h1 présente)
  origin_weather_missing_h7 = 1  (h7 manquante)
  ```

### 2️⃣ **Compteurs de missing pour features agrégées** (`_missing_count`)
- Compteurs indiquant combien d'heures étaient NULL avant l'agrégation
- Exemples :
  ```
  origin_weather_HourlyPrecip_Sum_missing_count = 3  (3 heures NULL sur 7)
  origin_weather_press_change_abs_Max_missing_count = 0  (aucune NULL)
  ```

### 3️⃣ **Valeurs sentinelles**
- Numériques : `-999` ou `-999.0`
- String : `"MISSING"`
- Remplacent les NULL pour compatibilité avec VectorAssembler

### 4️⃣ **Absence de NULL**
- Vérifie qu'aucune colonne ne contient de NULL
- Toutes les valeurs manquantes doivent être remplacées par des sentinelles

---

## 🚀 Méthode 1: Script Shell (Rapide)

### Utilisation

```bash
# Depuis la racine du projet
./scripts/validate_parquets.sh Experience-local-D-60-7-7
```

### Ce que fait le script

1. ✅ Vérifie l'existence des parquets
2. ✅ Lance Spark en mode local
3. ✅ Exécute la validation complète (TRAIN + TEST)
4. ✅ Affiche un rapport détaillé

### Sortie attendue

```
╔══════════════════════════════════════════════════════════════════════════════╗
║           VALIDATION DES PARQUETS - GESTION DES VALEURS MANQUANTES          ║
╚══════════════════════════════════════════════════════════════════════════════╝

📦 Experiment: Experience-local-D-60-7-7
📁 Base path:  /output

🔍 Vérification de l'existence des fichiers...
✅ Train parquet trouvé: /output/Experience-local-D-60-7-7/data/join_exploded_train_prepared.parquet
✅ Test parquet trouvé:  /output/Experience-local-D-60-7-7/data/join_exploded_test_prepared.parquet

🚀 Lancement de la validation avec Spark...
[... rapport détaillé ...]
✅ Validation terminée avec succès !
```

---

## 💻 Méthode 2: Code Scala

### Utilisation depuis SBT

```bash
sbt "runMain com.flightdelay.examples.ValidateParquetExample"
```

### Utilisation programmatique

```scala
import com.flightdelay.features.quality.ParquetMissingValuesValidator
import org.apache.spark.sql.SparkSession

implicit val spark: SparkSession = SparkSession.builder()
  .appName("Validation")
  .master("local[*]")
  .getOrCreate()

// Option 1: Rapport complet (TRAIN + TEST)
ParquetMissingValuesValidator.generateReport(
  trainPath = "/output/Experience-local-D-60-7-7/data/join_exploded_train_prepared.parquet",
  testPath  = "/output/Experience-local-D-60-7-7/data/join_exploded_test_prepared.parquet"
)

// Option 2: Valider seulement TRAIN
ParquetMissingValuesValidator.validateParquet(
  parquetPath = "/output/Experience-local-D-60-7-7/data/join_exploded_train_prepared.parquet",
  datasetName = "TRAIN"
)
```

---

## 📓 Méthode 3: Notebook Jupyter

### Emplacement

```
work/notebooks/Flights/DataQuality/ParquetMissingValuesValidation.ipynb
```

### Utilisation

1. Ouvrir le notebook avec Jupyter
2. Modifier le nom de l'experiment si nécessaire :
   ```scala
   val experimentName = "Experience-local-D-60-7-7"
   ```
3. Exécuter toutes les cellules

### Avantages

- ✅ Visualisation interactive
- ✅ Modification facile des chemins
- ✅ Exploration ad-hoc des données

---

## 📊 Interprétation des Résultats

### ✅ Validation Réussie

Si tout est correct, vous devriez voir :

```
1️⃣  VALIDATION DES FLAGS PAR HEURE (_missing_h1 à _missing_h7)
  ✓ Trouvé  14 colonnes de flags par heure

  Répartition:
    - Origin flags:       7 colonnes
    - Destination flags:  7 colonnes

  Statistiques des données manquantes par heure:
  origin_weather_missing_h1                                   :      1,234 ( 5.67%)
  origin_weather_missing_h7                                   :      1,554 ( 7.14%)
  ...

2️⃣  VALIDATION DES FEATURES AGRÉGÉES (Sum, Avg, Max, Min)
  ✓ Trouvé   8 features agrégées

  Vérification de la présence des compteurs (_missing_count):
  origin_weather_HourlyPrecip_Sum                             : ✓ Présent
  origin_weather_press_change_abs_Max                         : ✓ Présent
  ...

3️⃣  VALIDATION DES VALEURS SENTINELLES
  Colonnes avec sentinelles numériques -999 ou -999.0:
  origin_weather_Humidity_Delta_1hr_h7                        :      1,554 ( 7.14%)
  origin_weather_press_change_abs_Max                         :      9,321 (42.80%)
  ...

5️⃣  VÉRIFICATION DE L'ABSENCE DE NULL (toutes colonnes)
  ✅ SUCCÈS: Aucune valeur NULL détectée dans aucune colonne !
  ✅ Tous les NULL ont été remplacés par des sentinelles
```

### ⚠️ Validation Partielle

Si des problèmes sont détectés :

```
5️⃣  VÉRIFICATION DE L'ABSENCE DE NULL (toutes colonnes)
  ⚠️  ATTENTION: Des valeurs NULL ont été détectées:
  origin_weather_SomeFeature                                  :      1,000 ( 4.59%)
  destination_weather_OtherFeature                            :        500 ( 2.30%)

  ⚠️  Il reste des NULL - vérifier le pipeline de traitement
```

**Actions à prendre :**
1. Vérifier que `MissingValuesHandler` est bien appelé dans `FeaturePipeline`
2. Vérifier que les colonnes problématiques sont dans la configuration
3. Vérifier que les sentinelles sont configurées correctement

---

## 🔧 Configuration des Sentinelles

Les sentinelles sont configurées dans le fichier YAML :

```yaml
# src/main/resources/local-d2_60_7_7-config.yml

weatherSelectedFeatures:
  HourlyPrecip:
    transformation: "None"
    sentinelValue: -999.0  # Valeur numérique

  feature_weather_type:
    transformation: "StringIndexer"
    sentinelValue: "MISSING"  # Valeur string

aggregatedSelectedFeatures:
  press_change_abs:
    aggregation: "max"
    transformation: "None"
    sentinelValue: -999.0  # Valeur numérique
```

**Valeurs par défaut** (si non spécifiées) :
- `DoubleType` / `FloatType` → `-999.0`
- `IntegerType` / `LongType` → `-999`
- `StringType` → `"MISSING"`

---

## 📈 Exemples de Résultats Attendus

Pour le dataset **D2_60_7_7**, vous devriez obtenir :

### Features non-agrégées

```
origin_weather_Humidity_Delta_1hr_h7:
  - Sentinelles -999.0: ~1,554 lignes (7.14%)
  - Flag missing_h7 = 1: ~1,554 lignes (7.14%)
```

### Features agrégées

```
origin_weather_press_change_abs_Max:
  - Sentinelles -999.0: ~9,321 lignes (42.80%)
  - Missing count = 7: ~9,321 lignes (toutes heures NULL)
  - Missing count = 3: ~2,500 lignes (3 heures NULL sur 7)
  - Missing count = 0: ~10,000 lignes (aucune heure NULL)
```

---

## ❓ FAQ

### Q: Pourquoi ai-je des sentinelles -999 ?
**R:** C'est normal ! Les sentinelles remplacent les valeurs NULL pour permettre au VectorAssembler de fonctionner. Les flags/compteurs permettent au modèle ML de savoir qu'il s'agit de données manquantes.

### Q: Dois-je avoir 0% de NULL après validation ?
**R:** OUI ! Si vous avez des NULL restants, c'est que le pipeline n'a pas fonctionné correctement.

### Q: Comment savoir si mes agrégations sont correctes ?
**R:** Vérifiez que :
1. Les compteurs `_missing_count` existent pour chaque feature agrégée
2. Les valeurs agrégées ne contiennent PAS -999 sauf si TOUTES les heures étaient NULL
3. Exemple : `Sum = 4.6` avec `missing_count = 3` → Correct (somme des 4 valeurs non-NULL)

### Q: Que faire si la validation échoue ?
**R:**
1. Vérifier que l'ordre dans `FeaturePipeline` est : Explosion → Post-processing → MissingValuesHandler
2. Vérifier que les sentinelles sont configurées dans le YAML
3. Vérifier les logs du pipeline pour voir où le problème se produit

---

## 🎯 Résumé

| Outil | Cas d'usage | Avantages |
|-------|-------------|-----------|
| **Script Shell** | Validation rapide après pipeline | ✅ Rapide, automatique |
| **Code Scala** | Intégration dans le pipeline | ✅ Programmatique |
| **Notebook Jupyter** | Exploration interactive | ✅ Visualisation, flexibilité |

**Recommandation :** Utilisez le script shell après chaque exécution du pipeline pour valider rapidement que tout est correct.

---

## 📞 Support

En cas de problème, vérifiez :
1. Les logs du pipeline (`FeaturePipeline`, `MissingValuesHandler`, `DataJoinerPostProcessor`)
2. La configuration YAML (`sentinelValue` présent pour chaque feature)
3. L'ordre d'exécution dans `FeaturePipeline`

---

**Dernière mise à jour :** 2025-12-13
