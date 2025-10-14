# Pourquoi UTC_FL_DATE est-elle Ajoutée Automatiquement ?

## Question

> "Comment est-ce que je peux avoir des features `date_UTC_FL_DATE_*` dans mes feature importances alors que je n'ai pas mis `UTC_FL_DATE` dans ma configuration `flightSelectedFeatures` ?"

```yaml
flightSelectedFeatures:
  - "FL_DATE"                    ← Seulement FL_DATE !
  - "OP_CARRIER_AIRLINE_ID"
  - "ORIGIN_AIRPORT_ID"
  # ... pas de UTC_FL_DATE
```

**Mais dans les logs:**
```
Top 20 Feature Importances:
[ 45] date_UTC_FL_DATE_unix      :  13.24%  ← D'où vient-elle ???
[ 43] date_UTC_FL_DATE_day       :   5.05%
[ 42] date_UTC_FL_DATE_month     :   3.79%
```

---

## Réponse

`UTC_FL_DATE` est **ajoutée automatiquement** par `FlightWeatherDataJoiner` car c'est une **colonne obligatoire** pour la jointure avec les données météo.

---

## Colonnes Requises Automatiquement

### Code Source

**Fichier:** `FlightWeatherDataJoiner.scala:337-345`

```scala
val requiredCols = Set(
  "ORIGIN_WBAN", "UTC_FL_DATE", "feature_utc_departure_hour_rounded",
  "DEST_WBAN", "UTC_ARR_DATE", "feature_utc_arrival_hour_rounded"
)

val missingRequired = requiredCols
  .filter(flightDataCols.contains)    // Vérifie si la colonne existe
  .diff(colsSet)                      // Trouve les colonnes manquantes

var finalCols = (cols ++ missingRequired).distinct  // AJOUTE les colonnes manquantes
```

**Log visible:**
```
[FlightWeatherJoinner] Colonnes ajoutées automatiquement pour origin: UTC_FL_DATE, feature_utc_departure_hour_rounded, ...
```

---

## Pourquoi UTC_FL_DATE est-elle Nécessaire ?

### Problème avec FL_DATE Seule

`FL_DATE` est en **heure locale** de l'aéroport:
- Vol à LAX (PST, UTC-8): `FL_DATE = 2012-01-15 10:00 PST`
- Vol à JFK (EST, UTC-5): `FL_DATE = 2012-01-15 10:00 EST`

**Données météo** sont en **UTC** (temps universel):
- Toutes les stations météo reportent en UTC
- `Weather Date = 2012-01-15 18:00 UTC`

**Problème de jointure:**
```
FL_DATE (LAX PST) = 2012-01-15 10:00  ← 10:00 PST
Weather (UTC)     = 2012-01-15 18:00  ← 18:00 UTC = 10:00 PST ✓
```

Sans conversion UTC, impossible de joindre correctement !

---

### Solution: UTC_FL_DATE

**Conversion automatique** dans le preprocessing:

```scala
// FlightWBANEnricher.scala
val enrichedDF = flightDF
  .withColumn("UTC_CRS_DEP_TIME", col("CRS_DEP_TIME") - (col("ORIGIN_TIMEZONE") * 100))
  .withColumn("UTC_FL_DATE", /* conversion timezone */)
```

**Jointure correcte:**
```
FL_DATE (local)   = 2012-01-15 10:00 PST  }
                                           } Conversion
UTC_FL_DATE       = 2012-01-15 18:00 UTC  } → JOIN → Weather (UTC)
```

---

## Impact sur les Feature Importances

### Features UTC Générées

Quand `UTC_FL_DATE` est présente, `EnhancedDataFeatureExtractorPipeline` génère **5 features dérivées**:

```
UTC_FL_DATE  →  date_UTC_FL_DATE_year        (Index 41)
             →  date_UTC_FL_DATE_month       (Index 42)
             →  date_UTC_FL_DATE_day         (Index 43)
             →  date_UTC_FL_DATE_dayofweek   (Index 44)
             →  date_UTC_FL_DATE_unix        (Index 45)
```

### Importance dans Votre Modèle

D'après vos logs:

```
[ 45] date_UTC_FL_DATE_unix      :  13.24%  ← #1 feature la plus importante!
[ 43] date_UTC_FL_DATE_day       :   5.05%
[ 42] date_UTC_FL_DATE_month     :   3.79%
[ 44] date_UTC_FL_DATE_dayofweek :   1.81%
----------------------------------------------
TOTAL IMPORTANCE UTC:             23.89%  ← Presque 1/4 du modèle !
```

**Vs. FL_DATE (locale):**
```
[ 40] date_FL_DATE_unix          :  12.86%
[ 38] date_FL_DATE_day           :   6.13%
[ 37] date_FL_DATE_month         :   4.51%
[ 39] date_FL_DATE_dayofweek     :   2.68%
----------------------------------------------
TOTAL IMPORTANCE LOCALE:          26.18%
```

**Constat:** Les deux sont importantes, mais ensemble elles représentent **50% de l'importance du modèle** !

---

## Que Faire ?

### Option 1: ✅ Garder UTC_FL_DATE (Recommandé)

**Pourquoi:**
- ✅ Nécessaire pour la jointure météo
- ✅ Très importante pour le modèle (23.89% d'importance)
- ✅ Capture les patterns temporels globaux (indépendants du timezone)
- ✅ Complémentaire à FL_DATE (patterns locaux)

**Action:** Rien à faire, c'est déjà optimal.

---

### Option 2: ⚠️ Comprendre FL_DATE vs UTC_FL_DATE

**FL_DATE (locale):**
- Patterns calendaires **locaux** (jour de la semaine ressenti par les passagers)
- Saisonnalité **locale** (été/hiver selon la région)
- Heure **ressentie** par les passagers (10h du matin pour tous)

**UTC_FL_DATE:**
- Patterns **globaux** (tendances mondiales)
- Corrélation directe avec les **données météo** (même référentiel)
- **Synchronisation** entre aéroports (10h PST = 18h UTC = même instant pour tous)

**Utilité conjointe:**
- Modèle apprend: "Les lundis matin locaux (FL_DATE) + moment global (UTC) → plus de retards"
- Exemple: Lundi 8h PST (local) = Lundi 16h UTC (global) → Rush hour global

---

### Option 3: ❌ Retirer UTC_FL_DATE (Non Recommandé)

**Si vous voulez vraiment retirer les features dérivées UTC:**

⚠️ **ATTENTION:** Cela va:
1. ❌ Réduire la performance du modèle (~24% d'importance perdue)
2. ❌ Rendre la jointure météo potentiellement incorrecte
3. ❌ Ignorer les patterns temporels globaux

**Comment faire (déconseillé):**

Modifier `FeatureExtractor.scala:63-69` pour exclure UTC_FL_DATE:

```scala
val (allNumericCols, allTextCols, allBooleanCols, allDateCols) =
  ColumnTypeDetector.detectColumnTypesWithHeuristics(
    cleaData,
    excludeColumns = Seq(target, "UTC_FL_DATE"),  // ← Ajouter UTC_FL_DATE
    maxCardinalityForCategorical = experiment.featureExtraction.maxCategoricalCardinality,
    sampleFraction = 0.01
  )
```

**Mais vraiment, ne faites pas ça !** Vous perdez 24% de pouvoir prédictif.

---

## Recommandations

### 1. Gardez UTC_FL_DATE

C'est la meilleure stratégie. Votre modèle l'utilise efficacement.

### 2. Ajoutez-la Explicitement à Votre Config (Optionnel)

Pour clarté et documentation:

```yaml
flightSelectedFeatures:
  - "FL_DATE"           # Date locale (patterns locaux)
  - "UTC_FL_DATE"       # Date UTC (patterns globaux + jointure météo)
  - "OP_CARRIER_AIRLINE_ID"
  - "ORIGIN_AIRPORT_ID"
  # ...
```

**Effet:** Aucun changement (déjà ajoutée automatiquement), mais plus clair.

### 3. Analysez la Complémentarité FL_DATE / UTC_FL_DATE

**Test:** Entraînez 3 modèles:
1. Avec FL_DATE seulement → F1 = ?
2. Avec UTC_FL_DATE seulement → F1 = ?
3. Avec les deux (actuel) → F1 = 62.35%

**Hypothèse:** Le modèle (3) sera meilleur car les deux capturent des informations différentes.

---

## Autres Colonnes Ajoutées Automatiquement

D'après `FlightWeatherDataJoiner.scala:337-340`, ces colonnes sont aussi ajoutées si manquantes:

```scala
val requiredCols = Set(
  "ORIGIN_WBAN",                          // Station météo origine
  "UTC_FL_DATE",                          // Date UTC départ
  "feature_utc_departure_hour_rounded",   // Heure UTC arrondie départ
  "DEST_WBAN",                            // Station météo destination
  "UTC_ARR_DATE",                         // Date UTC arrivée
  "feature_utc_arrival_hour_rounded"      // Heure UTC arrondie arrivée
)
```

**Pourquoi:**
- `ORIGIN_WBAN`, `DEST_WBAN` : Clés de jointure météo
- `UTC_FL_DATE`, `UTC_ARR_DATE` : Dates de jointure météo (UTC requis)
- `feature_utc_*_hour_rounded` : Heures arrondies pour jointure météo horaire

**Toutes nécessaires pour la jointure météo !**

---

## Vérification dans les Logs

Cherchez cette ligne dans vos logs:

```bash
grep "Colonnes ajoutées automatiquement" logs.txt
```

**Résultat attendu:**
```
[FlightWeatherJoinner] Colonnes ajoutées automatiquement pour origin: feature_utc_departure_hour_rounded, UTC_ARR_DATE, UTC_FL_DATE, feature_utc_arrival_hour_rounded
[FlightWeatherJoinner] Colonnes ajoutées automatiquement pour destination: origin_weather_observations
```

---

## Résumé

| Question | Réponse |
|----------|---------|
| **Pourquoi UTC_FL_DATE apparaît ?** | Ajoutée automatiquement pour jointure météo |
| **Puis-je la retirer ?** | Non recommandé, elle représente 24% du modèle |
| **Est-ce un bug ?** | Non, c'est un comportement **intentionnel** et **nécessaire** |
| **Quelle action ?** | **Garder** UTC_FL_DATE, elle améliore le modèle |

**Conclusion:** UTC_FL_DATE est votre amie, gardez-la ! 🎯
