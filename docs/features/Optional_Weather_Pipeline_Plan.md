# Plan: Rendre le pipeline Weather optionnel

## Objectif
Permettre l'entraînement avec uniquement les features Flight (sans Weather) en rendant tout le pipeline weather optionnel.

## Modifications requises

### 1. FeatureExtractionConfig.scala ✅ FAIT
- [x] Ajouter méthode `isWeatherEnabled: Boolean`
- [x] weatherSelectedFeatures est déjà `Option[Map[String, FeatureTransformationConfig]]`

### 2. DataPipeline.scala
**Changements:**
- Retour: `(DataFrame, DataFrame)` → `(DataFrame, Option[DataFrame])`
- Steps 2,3,5 (weather loading, WBAN, weather preprocessing) deviennent conditionnels
- Si `experiment.featureExtraction.isWeatherEnabled == false`, skip tout weather

**Code:**
```scala
def execute(experiment: ExperimentConfig)
  (implicit spark: SparkSession, configuration: AppConfiguration): (DataFrame, Option[DataFrame]) = {

  val processedFlightData = FlightPreprocessingPipeline.execute()

  val processedWeatherData = if (experiment.featureExtraction.isWeatherEnabled) {
    println("\n[Step 2/3] Loading and preprocessing weather data...")
    WeatherDataLoader.loadFromConfiguration()
    WBANAirportTimezoneLoader.loadFromConfiguration()
    val weather = WeatherPreprocessingPipeline.execute()
    Some(weather)
  } else {
    println("\n⚠️  Weather features disabled - skipping weather pipeline")
    None
  }

  (processedFlightData, processedWeatherData)
}
```

### 3. FeaturePipeline.scala
**Changements:**
- Paramètre: `weatherData: DataFrame` → `weatherData: Option[DataFrame]`
- Méthode `join()` devient conditionnelle

**Code:**
```scala
def execute(
  flightData: DataFrame,
  weatherData: Option[DataFrame],  // ← Changed
  experiment: ExperimentConfig
)(implicit spark: SparkSession, configuration: AppConfiguration): String = {

  val dataForExtraction = weatherData match {
    case Some(weather) =>
      println("\n[Step 1/2] Joining flight & weather data...")
      join(flightData, weather, experiment)

    case None =>
      println("\n[Step 1/2] No weather data - using flight data only")
      flightData
  }

  val featuresPath = extractFeatures(dataForExtraction, experiment)
  featuresPath
}
```

### 4. FlightWeatherDataJoiner.scala
**Changements:**
- Créer méthode alternative: `joinFlightsWithWeatherOptional()`

**Code:**
```scala
def joinFlightsWithWeatherOptional(
  flightData: DataFrame,
  weatherData: Option[DataFrame],
  weatherDepthHours: Int,
  removeLeakageColumns: Boolean,
  storeJoinData: Boolean,
  storeExplodeJoinData: Boolean,
  experimentName: String
)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

  weatherData match {
    case Some(weather) =>
      joinFlightsWithWeather(flightData, weather, weatherDepthHours,
        removeLeakageColumns, storeJoinData, storeExplodeJoinData, experimentName)

    case None =>
      println("\n[FlightWeatherDataJoiner] Weather data not available - returning flight data only")
      flightData
  }
}
```

### 5. ConfigurationBasedFeatureExtractorPipeline.scala
**Changements:**
- La méthode `groupFeaturesByTransformation()` gère déjà les features weather manquantes (retourne Seq.empty)
- Aucune modification nécessaire! ✅

### 6. Configuration YAML
**Exemple sans weather:**
```yaml
experiments:
  - name: "Flight-Only-Experiment"
    description: "Entraînement sans features météo"
    enabled: true
    target: "label_is_delayed_15min"

    featureExtraction:
      type: "feature_selection"
      storeJoinData: false
      storeExplodeJoinData: false
      weatherDepthHours: 0  # Ignoré car pas de weather features
      maxCategoricalCardinality: 50

      flightSelectedFeatures:
        CRS_DEP_TIME:
          transformation: "None"
        OP_CARRIER_AIRLINE_ID:
          transformation: "StringIndexer"
        ORIGIN_AIRPORT_ID:
          transformation: "StringIndexer"
        # ... autres features flight

      # weatherSelectedFeatures: ABSENT ou vide
      # weatherSelectedFeatures: {}
```

## Ordre d'implémentation

1. ✅ FeatureExtractionConfig.scala - Ajouter isWeatherEnabled()
2. 🔧 DataPipeline.scala - Modifier pour retourner Option[DataFrame]
3. 🔧 FeaturePipeline.scala - Accepter Option[DataFrame] pour weather
4. 🔧 FlightWeatherDataJoiner.scala - Ajouter méthode conditionnelle
5. ✅ ConfigurationBasedFeatureExtractorPipeline.scala - Déjà OK
6. 🧪 Test avec configuration sans weather

## Tests de régression

Après modifications, vérifier:
- ✅ Expérience avec weather: fonctionne comme avant
- ✅ Expérience sans weather: skip le pipeline weather
- ✅ Les logs indiquent clairement si weather est activé/désactivé
- ✅ Pas d'erreur NullPointerException

## Points d'attention

⚠️ **MLPipeline.scala** doit aussi être vérifié car il appelle DataPipeline et FeaturePipeline

⚠️ **FlightDelayPredictionApp.scala** doit passer experiment à DataPipeline

⚠️ Les checkpoints intermédiaires doivent être conditionnels pour weather
