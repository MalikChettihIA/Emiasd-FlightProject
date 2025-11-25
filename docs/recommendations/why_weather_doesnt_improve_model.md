# Diagnostic: Pourquoi l'ajout d'heures météo n'améliore pas le modèle?

## Symptômes observés
- Besoin de 7+ heures météo pour 5-6% d'amélioration seulement
- L'amélioration n'est pas proportionnelle aux features ajoutées
- Le modèle Random Forest ne généralise pas bien sur les patterns météo

## Hypothèses diagnostiquées (par ordre de probabilité)

### 1. ⭐ Redondance des features temporelles (TRÈS PROBABLE)
**Problème**: `Temperature_h1`, `Temperature_h2`, `Temperature_h3` sont très corrélées (>0.9)
- Random Forest ne bénéficie pas de features redondantes
- Ajouter h8, h9, h10... n'apporte presque rien de nouveau

**Diagnostic**:
```python
# Vérifier la corrélation entre heures consécutives
correlation(Temperature_h1, Temperature_h2) > 0.95  # Probablement
correlation(Visibility_h1, Visibility_h2) > 0.90    # Probablement
```

**Solution**: Features de TENDANCE au lieu de valeurs brutes
- ✅ `Temperature_slope` = évolution sur N heures
- ✅ `Temperature_range` = volatilité
- ✅ Implémenté dans `WeatherTrendFeatures.scala`

### 2. ⭐ Manque de features de tendance (TRÈS PROBABLE)
**Problème**: Le modèle voit des valeurs absolues mais pas les PATTERNS
- Il voit: `Pressure=1013, 1012, 1011` (valeurs)
- Il ne voit PAS: "La pression BAISSE rapidement" (pattern dangereux)

**Solution**:
- ✅ Ajout de `slope` dans les agrégations (FAIT)
- ✅ Ajout de `range` dans les agrégations (FAIT)
- ✅ Création de `WeatherTrendFeatures` (FAIT)

**Configuration à ajouter**:
```yaml
aggregatedSelectedFeatures:
  Temperature:
    aggregation: "slope"  # Température monte/descend?
  Visibility:
    aggregation: "range"  # Conditions stables ou volatiles?
  press_change_abs:
    aggregation: "slope"  # Pression baisse rapidement?
```

### 3. ⭐ Random Forest inadapté aux séries temporelles (PROBABLE)
**Problème**: RF traite chaque feature indépendamment
- Il ne comprend pas que h1→h2→h3 est une SÉQUENCE
- GBT/XGBoost sont meilleurs pour capturer les interactions temporelles

**Solution**: Tester Gradient Boosted Trees
```yaml
experiments:
  - name: "D2_60_GBT_Weather"
    model:
      modelType: "gbt"  # Au lieu de randomforest
      hyperparameters:
        maxIter: [100, 200]
        maxDepth: [5, 7]
        stepSize: [0.1]
```

**Avantages GBT vs RF**:
| Aspect | Random Forest | Gradient Boosted Trees |
|--------|--------------|------------------------|
| Interactions | Faible | ✅ Excellent |
| Séries temporelles | ❌ Faible | ✅ Bon |
| Overfitting | ✅ Résiste bien | ⚠️ Risque |
| Vitesse | ✅ Rapide (parallèle) | ⚠️ Plus lent (séquentiel) |

### 4. ⭐ Manque de features d'interaction (PROBABLE)
**Problème**: Les features météo seules ne capturent pas tout
- `Visibility=1km` → impact différent si c'est la nuit ou le jour
- `Temperature=-5°C` → impact différent si Humidity=90% (givrage!)

**Solution**: Créer des interactions
```scala
// Météo × Temps
feature_visibility_at_night = visibility × is_night_flight

// Météo × Météo
feature_icing_risk = (temperature < 5) AND (humidity > 80)

// Origine vs Destination
feature_both_airports_bad = (origin_weather_bad AND dest_weather_bad)
```

**Fichier créé**: `docs/recommendations/weather_interaction_features.md`

### 5. Agrégations pas assez riches (POSSIBLE)
**Problème actuel**: Seulement sum, avg, max
**Ce qui manque**:
- ✅ `min` - Existe déjà
- ✅ `std` - Existe déjà
- ✅ `range` - AJOUTÉ
- ✅ `slope` - AJOUTÉ

### 6. Trop de features = overfitting (MOINS PROBABLE)
**Problème**: Avec 12h × 10 variables × 2 aéroports = 240 features météo
- Random Forest peut overfitter sur les features moins importantes
- Le signal se dilue dans le bruit

**Solution**:
1. Utiliser feature selection (importance > 0.01)
2. Réduire la profondeur temporelle si les features lointaines ont faible importance
3. Utiliser PCA sur les features météo

**Diagnostic**:
```bash
# Regarder les feature importances
cat output/D2_60/features/feature_importances_report.txt

# Si les features météo h7-h12 ont toutes importance < 0.5%, réduire à 6h
```

### 7. Problème de data quality (POSSIBLE)
**Problème potentiel**:
- Décalage temporel entre météo et vol
- Données météo manquantes/imputées
- Station météo trop éloignée de l'aéroport

**Diagnostic**:
```sql
-- Vérifier le taux de valeurs nulles
SELECT
  COUNT(*) as total,
  SUM(CASE WHEN origin_weather_Temperature_h1 IS NULL THEN 1 ELSE 0 END) / COUNT(*) as null_rate
FROM joined_data

-- Vérifier l'alignement temporel
SELECT
  HOUR(flight_departure_time) as flight_hour,
  HOUR(weather_observation_time) as weather_hour,
  COUNT(*) as cnt
FROM joined_data
GROUP BY 1, 2
ORDER BY 3 DESC
```

### 8. Les autres features dominent (POSSIBLE)
**Problème**: Les features météo sont moins importantes que les autres
- `ORIGIN_AIRPORT_ID`, `DEST_AIRPORT_ID` capturent déjà beaucoup de signal
- `DAY_OF_WEEK`, `HOUR` capturent des patterns temporels
- La météo ajoute peu d'information marginale

**Diagnostic**:
```bash
# Regarder les feature importances actuelles
# Si top 20 = seulement 1-2 features météo → météo pas importante
cat output/D2_60/features/feature_importances_report.txt
```

**Solution si c'est le cas**:
- Ne pas ajouter plus d'heures météo
- Focus sur améliorer les autres aspects (features de vol, interactions aéroport×temps)

## Plan d'action recommandé (par priorité)

### Phase 1: Features dérivées (FAIT ✅)
1. ✅ Ajouter agrégation `slope` (tendance) - FAIT
2. ✅ Ajouter agrégation `range` (volatilité) - FAIT
3. ✅ Créer `WeatherTrendFeatures.scala` - FAIT

**Temps estimé**: 1-2h
**Gain attendu**: +3-5% F1-score

### Phase 2: Configuration enrichie (TODO)
1. Mettre à jour `local-config.yml` avec les nouvelles agrégations
2. Ajouter les nouvelles features cycliques (sin/cos)
3. Lancer expérience avec les features de tendance

**Fichier à modifier**: `src/main/resources/local-config.yml`
```yaml
aggregatedSelectedFeatures:
  Temperature:
    aggregation: "slope"
  Visibility:
    aggregation: "range"
  press_change_abs:
    aggregation: "slope"
  RelativeHumidity:
    aggregation: "range"
```

**Temps estimé**: 30min
**Gain attendu**: +2-4% F1-score

### Phase 3: Tester GBT (TODO)
1. Créer expérience avec GradientBoostedTrees
2. Comparer RF vs GBT avec mêmes features
3. Ajuster hyperparamètres GBT

**Temps estimé**: 2h
**Gain attendu**: +5-10% F1-score

### Phase 4: Features d'interaction (TODO)
1. Créer `WeatherInteractionFeatures.scala`
2. Implémenter interactions Météo×Temps, Météo×Aéroport
3. Ajouter à la configuration

**Temps estimé**: 3-4h
**Gain attendu**: +3-7% F1-score

### Phase 5: Diagnostic profondeur optimale (TODO)
1. Lancer expériences avec 3h, 6h, 9h, 12h
2. Analyser feature importances par profondeur
3. Identifier le sweet spot

**Temps estimé**: 1h (parallélisable)
**Gain attendu**: Optimisation ressources + légère amélioration

## Expériences à lancer immédiatement

### Expérience A: Tendances météo
```bash
# Modifier local-config.yml avec les nouvelles agrégations
# Lancer le pipeline
sbt "run local"

# Observer l'amélioration
```

### Expérience B: RF vs GBT
```yaml
# Créer deux configs identiques sauf modelType
experiments:
  - name: "D2_60_RF_Trends"
    model:
      modelType: "randomforest"
    aggregatedSelectedFeatures:
      Temperature:
        aggregation: "slope"

  - name: "D2_60_GBT_Trends"
    model:
      modelType: "gbt"
    aggregatedSelectedFeatures:
      Temperature:
        aggregation: "slope"
```

### Expérience C: Profondeur optimale
```yaml
experiments:
  - name: "D2_60_Weather_3h"
    weatherOriginDepthHours: 3
    weatherDestinationDepthHours: 3

  - name: "D2_60_Weather_6h"
    weatherOriginDepthHours: 6
    weatherDestinationDepthHours: 6

  - name: "D2_60_Weather_9h"
    weatherOriginDepthHours: 9
    weatherDestinationDepthHours: 9

  - name: "D2_60_Weather_12h"
    weatherOriginDepthHours: 12
    weatherDestinationDepthHours: 12
```

Comparer:
- F1-score
- Feature importances des features météo lointaines (h7-h12)
- Training time

## Métriques de succès

| Métrique | Baseline (actuel) | Objectif Phase 1-2 | Objectif Phase 3-4 |
|----------|-------------------|--------------------|--------------------|
| F1-Score | ~65-70% | 70-75% | 75-80% |
| Feature météo dans top 20 | 0-2 | 4-6 | 8-10 |
| Training time | baseline | <1.5x baseline | <2x baseline |
| Profondeur optimale | 12h? | 6-9h | 6-9h |

## Conclusion

**Hypothèse principale**: Le problème n'est PAS la quantité de données météo (12h), mais la QUALITÉ des features dérivées.

**Solution recommandée**:
1. ✅ Ajouter features de TENDANCE (slope, range) - FAIT
2. Tester Gradient Boosted Trees
3. Créer features d'INTERACTION
4. Réduire la profondeur temporelle si pas de gain après 6h

**Gain attendu total**: +10-15% F1-score avec moins de features (6-9h au lieu de 12h)
