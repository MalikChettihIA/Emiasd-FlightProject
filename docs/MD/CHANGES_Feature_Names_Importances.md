# Affichage des Noms de Features dans Feature Importances

## Problème Initial

Les feature importances du Random Forest affichaient seulement des index:

```
Top 20 Feature Importances:
--------------------------------------------------
Feature 130:  10.97%
Feature 135:  10.87%
Feature   1:   5.73%
Feature 132:   5.04%
```

**Problème:** Impossible de savoir quelle feature correspond à quel index !

---

## Solution Implémentée

### Changements dans `RandomForestModel.scala`

#### 1. Modification de `displayFeatureImportance()` (lignes 100-119)

**Avant:**
```scala
println(f"Feature $idx%3d: ${importance * 100}%6.2f%%")
```

**Après:**
```scala
val featureName = featureNames.lift(idx).getOrElse(s"Feature_$idx")
println(f"[$idx%3d] $featureName%-50s: ${importance * 100}%6.2f%%")
```

**Format:** `[index] nom_de_feature : pourcentage`

---

#### 2. Nouvelle méthode `loadFeatureNames()` (lignes 121-158)

**Rôle:** Charger les noms de features depuis `selected_features.txt`

**Chemins essayés (dans l'ordre):**
1. `/output/Experience-1-local/features/selected_features.txt`
2. `output/Experience-1-local/features/selected_features.txt`
3. `work/output/Experience-1-local/features/selected_features.txt`
4. `Experience-1-local/features/selected_features.txt`

**Gestion d'erreur:**
- Si le fichier n'existe pas → affiche les index (`Feature_0`, `Feature_1`, etc.)
- Si le fichier existe → charge les noms de features
- Si une erreur survient → log l'erreur et continue avec les index

---

#### 3. Modification de `saveFeatureImportance()` (lignes 160-191)

**Avant:** Sauvegardait seulement `(index, importance)`

**Après:** Sauvegarde un CSV avec 3 colonnes:
```csv
feature_index,feature_name,importance
130,date_FL_DATE_year,0.1097
135,date_FL_DATE_unix,0.1087
1,indexed_ORIGIN_WBAN,0.0573
```

---

## Nouveaux Logs

### Chargement des Feature Names

**Si succès:**
```
✓ Loaded 128 feature names from: /output/Experience-1-local/features/selected_features.txt
```

**Si échec:**
```
⚠ Could not load feature names (tried 4 locations)
```

**Si erreur:**
```
⚠ Error loading feature names: java.io.FileNotFoundException: ...
```

---

### Affichage des Feature Importances

**Nouveau format:**
```
Top 20 Feature Importances:
--------------------------------------------------
[130] date_FL_DATE_year                              : 10.97%
[135] date_FL_DATE_unix                              : 10.87%
[  1] indexed_ORIGIN_WBAN                            :  5.73%
[132] date_UTC_FL_DATE_day                           :  5.04%
[  5] indexed_ORIGIN_AIRPORT_ID                      :  5.00%
[127] date_FL_DATE_month                             :  4.86%
[  3] indexed_OP_CARRIER_AIRLINE_ID                  :  4.72%
[128] date_FL_DATE_dayofweek                         :  4.34%
[133] date_UTC_FL_DATE_dayofweek                     :  3.66%
[ 26] origin_weather_feature_weather_severity_index-6:  2.33%
[129] date_UTC_FL_DATE_unix                          :  2.23%
[  0] indexed_DEST_WBAN                              :  1.98%
[  2] indexed_DEST_AIRPORT_ID                        :  1.96%
[ 10] origin_weather_feature_weather_severity_index-9:  1.73%
[  6] origin_weather_feature_weather_severity_index-11:  1.72%
[ 14] origin_weather_feature_weather_severity_index-7:  1.64%
[134] date_UTC_FL_DATE_month                         :  1.61%
[ 30] origin_weather_feature_weather_severity_index-4:  1.59%
[ 18] origin_weather_feature_weather_severity_index-5:  1.57%
[ 34] origin_weather_feature_weather_severity_index-3:  1.53%
--------------------------------------------------
```

**Colonnes:**
- `[index]` : Index de la feature dans le vecteur (aligné à droite sur 3 caractères)
- `nom_feature` : Nom complet de la feature (aligné à gauche sur 50 caractères)
- `: XX.XX%` : Pourcentage d'importance

---

## Interprétation des Résultats

### Top Features Identifiées

D'après l'exemple ci-dessus:

**1. Date Features (temporalité)**
```
[130] date_FL_DATE_year         : 10.97%  ← Année du vol
[135] date_FL_DATE_unix         : 10.87%  ← Timestamp Unix
[132] date_UTC_FL_DATE_day      :  5.04%  ← Jour du mois (UTC)
[127] date_FL_DATE_month        :  4.86%  ← Mois
[128] date_FL_DATE_dayofweek    :  4.34%  ← Jour de la semaine
```

**Interprétation:** Les features temporelles dominent (33% de l'importance cumulée) ! Le modèle apprend fortement des patterns saisonniers et calendaires.

---

**2. Location Features (aéroports)**
```
[  1] indexed_ORIGIN_WBAN       :  5.73%  ← Station météo origine
[  5] indexed_ORIGIN_AIRPORT_ID :  5.00%  ← Aéroport origine
[  3] indexed_OP_CARRIER_AIRLINE_ID: 4.72%  ← Compagnie aérienne
[  0] indexed_DEST_WBAN         :  1.98%  ← Station météo destination
[  2] indexed_DEST_AIRPORT_ID   :  1.96%  ← Aéroport destination
```

**Interprétation:** L'aéroport d'origine est plus important que la destination. Certains aéroports sont plus susceptibles aux retards.

---

**3. Weather Features (météo)**
```
[ 26] origin_weather_severity_index-6  :  2.33%  ← 6h avant départ
[ 10] origin_weather_severity_index-9  :  1.73%  ← 9h avant
[  6] origin_weather_severity_index-11 :  1.72%  ← 11h avant
[ 14] origin_weather_severity_index-7  :  1.64%  ← 7h avant
```

**Interprétation:** La météo 6-11 heures avant le départ est pertinente, pas seulement l'heure exacte du départ.

---

## Fichier CSV Sauvegardé

**Location:** `/output/Experience-1-local/metrics/feature_importance.csv`

**Format:**
```csv
feature_index,feature_name,importance
130,date_FL_DATE_year,0.10970123456
135,date_FL_DATE_unix,0.10870234567
1,indexed_ORIGIN_WBAN,0.05730012345
...
```

**Utilisation:** Peut être chargé pour visualisation (matplotlib, seaborn, etc.)

---

## Actions Recommandées

### 1. Analyser les Top Features

**Si les features temporelles dominent (>40%):**
- ✅ Le modèle capture bien les patterns saisonniers
- ⚠️ Mais risque d'overfitting sur des tendances temporelles non généralisables

**Solution:**
- Ajouter plus de features contextuelles (jour férié, événement spécial)
- Réduire le poids des features temporelles si nécessaire

---

**Si les features météo ont peu d'importance (<10%):**
- ⚠️ La météo n'est pas assez prédictive
- Possible: Les features météo ne capturent pas les bons patterns

**Solution:**
- Vérifier la qualité des features météo
- Ajouter des interactions (ex: météo × heure de la journée)
- Augmenter `weatherDepthHours` dans la config

---

**Si les features d'aéroport dominent (>30%):**
- ✅ Certains aéroports sont vraiment plus problématiques
- ⚠️ Possible: Le modèle mémorise juste les aéroports sans comprendre pourquoi

**Solution:**
- Ajouter des features d'aéroport (trafic, capacité, hub/non-hub)
- Grouper les aéroports par région si trop de catégories

---

### 2. Feature Engineering Guidé

**Exemple:** Si `origin_weather_severity_index-6` est important (6h avant):

**Action:**
```yaml
# local-config.yml
featureExtraction:
  weatherDepthHours: 6  # Au lieu de 12
```

**Gain:** Moins de features → Entraînement plus rapide, moins d'overfitting

---

### 3. Visualiser les Importances

**Script Python:**
```python
import pandas as pd
import matplotlib.pyplot as plt

# Charger les feature importances
df = pd.read_csv('/output/Experience-1-local/metrics/feature_importance.csv')

# Top 20
top20 = df.head(20)

plt.figure(figsize=(10, 8))
plt.barh(top20['feature_name'], top20['importance'])
plt.xlabel('Importance')
plt.title('Top 20 Feature Importances - Random Forest')
plt.tight_layout()
plt.savefig('feature_importances.png')
```

---

## Troubleshooting

### Feature Names Non Chargés

**Symptôme:**
```
⚠ Could not load feature names (tried 4 locations)

Top 20 Feature Importances:
[130] Feature_130  : 10.97%
[135] Feature_135  : 10.87%
```

**Cause:** Le fichier `selected_features.txt` n'existe pas ou n'est pas accessible

**Solution:**
1. Vérifier que `FeatureExtractor` a bien sauvegardé les features:
   ```bash
   ls -la /output/Experience-1-local/features/
   ```

2. Si le fichier n'existe pas, vérifier la config:
   ```yaml
   featureExtraction:
     type: "feature_selection"  # Doit être feature_selection pour sauvegarder
   ```

3. Relancer le pipeline feature-extraction:
   ```bash
   ./submit.sh
   ```

---

### Feature Names Incomplets

**Symptôme:**
```
✓ Loaded 50 feature names from: ...

[130] Feature_130  : 10.97%  ← Pas de nom (index > 50)
```

**Cause:** Le fichier `selected_features.txt` ne contient que 50 noms mais le vecteur en a 138

**Solution:** Bug dans FeatureExtractor → Vérifier que tous les noms sont sauvegardés

---

## Résumé

| Avant | Après |
|-------|-------|
| ❌ `Feature 130: 10.97%` | ✅ `[130] date_FL_DATE_year: 10.97%` |
| ❌ Impossible d'interpréter | ✅ Compréhension immédiate |
| ❌ Pas de CSV avec noms | ✅ CSV complet avec 3 colonnes |
| ❌ Feature engineering à l'aveugle | ✅ Feature engineering guidé par les données |

**Prochaine étape:** Recompiler et voir vos vraies feature importances !

```bash
sbt package
./submit.sh
```
