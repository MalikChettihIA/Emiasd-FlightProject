# Recommandations: Features d'interaction météo

## Problème identifié
L'ajout de plus d'heures météo (>7h) n'améliore les métriques que de 5-6%, ce qui suggère que le modèle n'extrait pas assez de signal des données météo.

## Features d'interaction à implémenter

### 1. Interactions Météo × Aéroport
```yaml
# Certains aéroports sont plus sensibles à certaines conditions
- origin_weather_visibility × ORIGIN_AIRPORT_ID
- destination_weather_wind_speed × DEST_AIRPORT_ID
- origin_weather_precipitation × airport_has_deicing_equipment
```

**Pourquoi?** Un aéroport avec une seule piste est plus impacté par le vent de travers qu'un grand aéroport avec plusieurs pistes

### 2. Interactions Météo × Temps
```yaml
# La même météo a un impact différent selon le moment
- weather_severity × feature_departure_time_period (rush hour?)
- visibility × is_night_flight
- precipitation × season
```

**Pourquoi?** Une faible visibilité la nuit est plus critique que le jour

### 3. Interactions Météo × Compagnie
```yaml
# Certaines compagnies gèrent mieux les mauvaises conditions
- weather_severity × OP_CARRIER_AIRLINE_ID
- icing_risk × carrier_has_deicing_capability
```

**Pourquoi?** Les compagnies low-cost peuvent annuler plus vite en mauvaises conditions

### 4. Interactions Météo Origine × Destination
```yaml
# L'écart de conditions météo entre origine et destination
- |origin_weather_temp - dest_weather_temp|
- origin_weather_severity - dest_weather_severity
- both_airports_bad_weather (booléen)
```

**Pourquoi?** Si les deux aéroports ont du mauvais temps, le risque de retard augmente exponentiellement

### 5. Interactions Tendance × Valeur Actuelle
```yaml
# La tendance + valeur absolue donnent le contexte complet
- visibility_current × visibility_trend_slope
- pressure_current × pressure_is_dropping_fast
- temperature_current × temperature_trend_variance
```

**Pourquoi?** Une pression basse ET qui baisse rapidement = tempête imminente

## Implémentation recommandée

### Option 1: Via VectorAssembler + Interaction Stage (Spark ML)
```scala
// Dans le pipeline ML
import org.apache.spark.ml.feature.Interaction

val interaction = new Interaction()
  .setInputCols(Array("origin_weather_visibility", "is_night_flight"))
  .setOutputCol("visibility_night_interaction")
```

### Option 2: Via preprocessing (Recommandé)
Créer `WeatherInteractionFeatures.scala`:
```scala
object WeatherInteractionFeatures {
  def createInteractionFeatures(df: DataFrame): DataFrame = {
    df
      // Météo × Temps
      .withColumn("feature_visibility_at_night",
        col("origin_weather_visibility") * col("feature_is_night_flight"))

      // Météo × Aéroport (via agrégations)
      .withColumn("feature_weather_airport_risk",
        col("origin_weather_severity") * col("origin_airport_delay_rate"))

      // Origine vs Destination
      .withColumn("feature_weather_delta_severity",
        abs(col("origin_weather_severity") - col("destination_weather_severity")))
  }
}
```

### Option 3: Features polynomiales (automatique)
```scala
import org.apache.spark.ml.feature.PolynomialExpansion

val polyExpansion = new PolynomialExpansion()
  .setInputCol("weatherFeatures")
  .setOutputCol("weatherFeaturesExpanded")
  .setDegree(2)  // Crée des interactions d'ordre 2 (x1*x2, x1², x2²)
```

## Configuration YAML recommandée

```yaml
aggregatedSelectedFeatures:
  HourlyPrecip:
    aggregation: "sum"        # Existant
  HourlyPrecip:
    aggregation: "slope"      # NOUVEAU - tendance
  RelativeHumidity:
    aggregation: "avg"        # Existant
  RelativeHumidity:
    aggregation: "range"      # NOUVEAU - volatilité
  press_change_abs:
    aggregation: "max"        # Existant
  Temperature:
    aggregation: "slope"      # NOUVEAU - évolution
  Visibility:
    aggregation: "min"        # NOUVEAU - pire cas
  Visibility:
    aggregation: "slope"      # NOUVEAU - détérioration?
```

## Priorité d'implémentation

1. **HIGH**: Ajouter `slope` et `range` aux agrégations (déjà fait!)
2. **HIGH**: Créer `WeatherTrendFeatures.scala` (déjà créé!)
3. **MEDIUM**: Ajouter interactions Météo × Temps dans preprocessing
4. **MEDIUM**: Tester GradientBoostedTrees au lieu de RandomForest
5. **LOW**: Features polynomiales (si les autres ne suffisent pas)

## Expériences à mener

### Expérience 1: Tendances météo
```bash
# Avec agrégations slope + range
experiments:
  - name: "D2_60_Weather_Trends"
    aggregatedSelectedFeatures:
      Temperature:
        aggregation: "slope"
      Visibility:
        aggregation: "range"
      press_change_abs:
        aggregation: "slope"
```

### Expérience 2: GBT vs RF
```bash
# Comparer RandomForest vs GradientBoostedTrees
experiments:
  - name: "D2_60_RF_12h"
    model:
      modelType: "randomforest"
  - name: "D2_60_GBT_12h"
    model:
      modelType: "gbt"
```

### Expérience 3: Profondeur optimale
```bash
# Trouver le sweet spot d'heures météo
experiments:
  - name: "D2_60_Weather_3h"
    weatherOriginDepthHours: 3
  - name: "D2_60_Weather_6h"
    weatherOriginDepthHours: 6
  - name: "D2_60_Weather_12h"
    weatherOriginDepthHours: 12
```

## Métriques à surveiller

- **Feature Importance**: Les nouvelles features apparaissent-elles dans le top 20?
- **F1-Score**: Amélioration > 2-3% ?
- **Recall Delayed**: Capture-t-on mieux les vols retardés?
- **Training time**: Le modèle reste-t-il rapide?
