// Databricks notebook source
// MAGIC %md * Author: Malik Chettih
// MAGIC * Affiliation: EMIASD - Executive Master Intelligence artificielle
// MAGIC & science des données
// MAGIC * Email: malik.chettih@dauphine.eu
// MAGIC * Formation Continue Univ. Paris Dauphine, January 2025.

// COMMAND ----------

// MAGIC %md
// MAGIC # Weather - Analyse des données

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 1 - Pre-requis
// MAGIC
// MAGIC ### 1.1 Global Variables

// COMMAND ----------

val path = "/FileStore/tables/FLIGHT-3Y/Weather/"
val fileName = "201201hourly.txt"
//val fileName = "*hourly.txt"
val dbfsDir = "dbfs:" + path

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.2 Global parameters

// COMMAND ----------

//pipeline 
val _label = "label"
val _prefix = "indexed_"
val _featuresVec = "featuresVec"
val _featuresVecIndex = "features"

//metadata extraction
val _text = "textType"
val _numeric = "numericType"
val _date = "dateType"
val _other = "otherType"

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.3 Global imports

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import  org.apache.spark.ml.Pipeline 

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import spark.implicits._

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.4 Data Transformation Pipeline

// COMMAND ----------

def AutoPipeline(textCols: Array[String], numericCols: Array[String], target: String, maxCat: Int, handleInvalid: String):Pipeline = {
  //StringIndexer
  val inAttsNames = textCols ++ Array(target)
  val outAttsNames = inAttsNames.map(_prefix+_)

  val stringIndexer = new StringIndexer()
                              .setInputCols(inAttsNames)
                              .setOutputCols(outAttsNames)
                              .setHandleInvalid(handleInvalid)
  
  val features = outAttsNames.filterNot(_.contains(target))++numericCols
  
  //vectorAssembler
  val vectorAssembler = new VectorAssembler()
                            .setInputCols(features)
                            .setOutputCol(_featuresVec)
                            .setHandleInvalid(handleInvalid)
  
  //VectorIndexer
  val vectorIndexer = new VectorIndexer()
                            .setInputCol(_featuresVec)
                            .setOutputCol(_featuresVecIndex)
                            .setMaxCategories(maxCat)
                            .setHandleInvalid(handleInvalid)
  
  val pipeline = new Pipeline()
                    .setStages(Array(stringIndexer,vectorAssembler,vectorIndexer))
  
  return pipeline
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.5 Data quality metrics collection

// COMMAND ----------

case class MetaData(name: String, origType: String, colType: String, compRatio: Float, nbDistinctValues: Long)

//considers only three types: numeric, textual and other
def whichType(origType: String) = origType match {
  case "StringType" => _text
  case "IntegerType"|"DoubleType" => _numeric
  case "DateType" => _date
  case _ => _other
}

def MDCompletenessDV(data: DataFrame): DataFrame = {
  val total_count = data.count()
  val res = data.dtypes.map{
    case(colName, colType)=>MetaData(colName, 
                                      colType, 
                                      whichType(colType),
                                      data.filter(col(colName).isNotNull).count.toFloat/total_count,
                                      data.select(colName).distinct().count)
  }.toList
  val metadata = res.toDS().toDF()
  metadata.persist()  
  metadata.count()
  return metadata
}

def SetMDColType(metaData: DataFrame, name: String, colType: String): DataFrame = {
  val metaData_updated = metaData.withColumn(
    "colType",
    when(col("name") === name, colType)
    .otherwise(col("colType"))
  )
  return metaData_updated
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 2 - Data Loading

// COMMAND ----------

val weather_original_data = spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(dbfsDir+fileName)
            .persist()
weather_original_data.count()

// COMMAND ----------

weather_original_data.printSchema

// COMMAND ----------

// En une seule ligne
display(weather_original_data.select(weather_original_data.columns.sorted.map(col): _*).limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.3 Collecting data quality metrics

// COMMAND ----------

var weather_original_metadata = MDCompletenessDV(weather_original_data)
display(weather_original_metadata.orderBy($"name".asc))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.4 Data Description
// MAGIC
// MAGIC ## 🟦 **PRESSION ALTIMÉTRIQUE**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `Altimeter` | Pression altimétrique pour aviation (inHg) | Float | 295 | 🔶 Faible impact direct |
// MAGIC | `AltimeterFlag` | Indicateur de validité | String | 2 | ❌ À ignorer |
// MAGIC
// MAGIC ## 🟨 **POINT DE ROSÉE**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `DewPointCelsius` | Point de rosée en °C | Float | 666 | 🔶 Corrélé à humidité |
// MAGIC | `DewPointCelsiusFlag` | Indicateur de validité | String | 2 | ❌ À ignorer |
// MAGIC | `DewPointFarenheit` | Température du point de rosée (°F) | Int | 154 | 🔶 Corrélé à humidité |
// MAGIC | `DewPointFarenheitFlag` | Indicateur de validité | String | 1 | ❌ À ignorer |
// MAGIC
// MAGIC ## 🟥 **TEMPÉRATURE SÈCHE**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `DryBulbCelsius` | Température sèche en °C | Float | 834 | ✅ Température principale |
// MAGIC | `DryBulbCelsiusFlag` | Indicateur de validité | String | 2 | ❌ À ignorer |
// MAGIC | `DryBulbFarenheit` | Température sèche en °F | Int | 187 | 🔶 Doublon, préférer °C |
// MAGIC | `DryBulbFarenheitFlag` | Indicateur de validité | String | 1 | ❌ À ignorer |
// MAGIC
// MAGIC ## 💧 **PRÉCIPITATIONS**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `HourlyPrecip` | Précipitation horaire (pouces) | Float | 145 | ✅ Pluie/neige - retards |
// MAGIC | `HourlyPrecipFlag` | Indicateur de validité | String | 2 | ❌ À ignorer |
// MAGIC
// MAGIC ## 📊 **PRESSION ATMOSPHÉRIQUE**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `PressureChange` | Changement de pression sur 3h (inHg) | Float | 118 | 🔶 Optionnel |
// MAGIC | `PressureChangeFlag` | Indicateur de validité | String | 1 | ❌ À ignorer |
// MAGIC | `PressureTendency` | Tendance de pression (0-8 codée) | Int | 10 | 🔶 Optionnel |
// MAGIC | `PressureTendencyFlag` | Indicateur de validité | String | 1 | ❌ À ignorer |
// MAGIC | `SeaLevelPressure` | Pression au niveau de la mer (inHg) | Float | 293 | 🔶 Moins critique |
// MAGIC | `SeaLevelPressureFlag` | Indicateur de validité | String | 2 | ❌ À ignorer |
// MAGIC | `StationPressure` | Pression sur le site (inHg) | Float | 1202 | 🔶 Moins discriminant |
// MAGIC | `StationPressureFlag` | Indicateur de validité | String | 1 | ❌ À ignorer |
// MAGIC
// MAGIC ## 🟫 **MÉTADONNÉES STATION**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `RecordType` | Type d'enregistrement (AA, etc.) | String | 3 | ❌ À ignorer |
// MAGIC | `RecordTypeFlag` | Indicateur de validité | String | 1 | ❌ À ignorer |
// MAGIC | `StationType` | Type de station | Int | 6 | 🔶 Rarement utile |
// MAGIC
// MAGIC ## 💨 **HUMIDITÉ RELATIVE**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `RelativeHumidity` | Humidité relative (%) | Int | 100 | ✅ Variable météo importante |
// MAGIC | `RelativeHumidityFlag` | Indicateur de validité | String | 1 | ❌ À ignorer |
// MAGIC
// MAGIC ## ☁️ **CONDITIONS DU CIEL**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `SkyCondition` | Couverture nuageuse (METAR) | String | 109041 | ✅ Catégorie météo importante |
// MAGIC | `SkyConditionFlag` | Indicateur de validité | String | 2 | ❌ À ignorer |
// MAGIC
// MAGIC ## ⏰ **TEMPOREL ET GÉOGRAPHIQUE**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `Date` | Date de l'observation (yyyymmdd) | Int | 31 | ✅ Clé temporelle |
// MAGIC | `Time` | Heure d'observation (hhmm) | String | 1440 | ✅ Clé temporelle |
// MAGIC | `WBAN` | Code station météo | String | 1968 | ✅ Clé de jointure |
// MAGIC
// MAGIC ## 🌪️ **VENT**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `ValueForWindCharacter` | Caractère du vent (G pour rafale) | String | 75 | 🔶 Peut indiquer rafales |
// MAGIC | `ValueForWindCharacterFlag` | Indicateur de validité | String | 2 | ❌ À ignorer |
// MAGIC | `WindDirection` | Direction du vent (0–360°) | Int | 364 | 🔶 Moins critique |
// MAGIC | `WindDirectionFlag` | Indicateur de validité | String | 1 | ❌ À ignorer |
// MAGIC | `WindSpeed` | Vitesse du vent en nœuds | Int | 68 | ✅ Corrélé aux perturbations |
// MAGIC | `WindSpeedFlag` | Indicateur de validité | String | 2 | ❌ À ignorer |
// MAGIC
// MAGIC ## 👁️ **VISIBILITÉ**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `Visibility` | Visibilité horizontale (miles) | Float | 45 | ✅ Forte corrélation retards |
// MAGIC | `VisibilityFlag` | Indicateur de qualité | String | 2 | ❌ À ignorer |
// MAGIC
// MAGIC ## ⛈️ **PHÉNOMÈNES MÉTÉO**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `WeatherType` | Phénomène météo (RA, SN, TSRA) | String | 315 | ✅ À parser pour types sévères |
// MAGIC | `WeatherTypeFlag` | Indicateur de validation | String | 2 | ❌ À ignorer |
// MAGIC
// MAGIC ## 🌡️ **TEMPÉRATURE HUMIDE**
// MAGIC | Colonne | Description | Type | Valeurs distinctes | Utilité ML |
// MAGIC |---------|-------------|------|-------------------|------------|
// MAGIC | `WetBulbCelsius` | Température humide en °C | Float | 642 | 🔶 Corrélé à humidité |
// MAGIC | `WetBulbCelsiusFlag` | Indicateur de validité | String | 1 | ❌ À ignorer |
// MAGIC | `WetBulbFarenheit` | Température humide en °F | Int | 140 | 🔶 Corrélé à humidité |
// MAGIC | `WetBulbFarenheitFlag` | Indicateur de validité | String | 1 | ❌ À ignorer |
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ## 📊 Résumé par famille
// MAGIC
// MAGIC - **🟦 Pression altimétrique** (2) : Spécifique aviation
// MAGIC - **🟨 Point de rosée** (4) : Mesure d'humidité
// MAGIC - **🟥 Température sèche** (4) : Température de l'air
// MAGIC - **💧 Précipitations** (2) : Pluie/neige
// MAGIC - **📊 Pression atmosphérique** (8) : Variations barométriques
// MAGIC - **🟫 Métadonnées station** (3) : Info technique
// MAGIC - **💨 Humidité relative** (2) : Humidité en %
// MAGIC - **☁️ Conditions du ciel** (2) : Couverture nuageuse
// MAGIC - **⏰ Temporel/Géo** (3) : Date, heure, localisation
// MAGIC - **🌪️ Vent** (6) : Direction, vitesse, rafales
// MAGIC - **👁️ Visibilité** (2) : Distance visible
// MAGIC - **⛈️ Phénomènes météo** (2) : Conditions spéciales
// MAGIC - **🌡️ Température humide** (4) : Température avec humidité
// MAGIC
// MAGIC **Total : 45 colonnes regroupées en 13 familles**

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 3 - Feature Engineering

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.1 Data Cleaning

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.1.1 Drop Duplicates

// COMMAND ----------

println("Total records count without dropping duplicates:", weather_original_data.count())
val weather_data = weather_original_data.dropDuplicates()
println("Total records count after dropping duplicates:", weather_data.count())

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.2 Categorical Data Values

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.2.1 Flags

// COMMAND ----------

weather_original_data.columns
  .filter(_.endsWith("Flag"))
  .sorted  // Tri alphabétique des noms de colonnes
  .foreach { colName =>
    println(s"=== $colName ===")
    weather_original_data.groupBy(colName).count().orderBy($"count".desc).show()
    println()
  }

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.2.2 Weather Type
// MAGIC
// MAGIC METAR/TAF LIST OF ABBREVIATIONS AND ACRONYMS
// MAGIC - https://www.weather.gov/media/wrh/mesowest/metar_decode_key.pdf

// COMMAND ----------

display(weather_original_data.groupBy("WeatherType").count().orderBy($"count".desc))

// COMMAND ----------

// Solution simple et efficace pour extraire les types météo distincts
val weather_type_dataset = weather_original_data
  .select("WeatherType")
  .filter(col("WeatherType").isNotNull && col("WeatherType") =!= "")
  .withColumn("weather_elements", split(col("WeatherType"), "\\s+"))
  .select(explode(col("weather_elements")).as("WeatherType"))
  .filter(col("WeatherType") =!= "")
  .distinct()
  .orderBy("WeatherType")

display(weather_type_dataset)



// COMMAND ----------

// Compter le nombre total de types distincts
val totalDistinctTypes = weather_type_dataset.count()
println(s"Nombre total de types météo distincts: $totalDistinctTypes")

// Optionnel: Sauvegarder le dataset pour utilisation future
weather_type_dataset.cache() // Pour optimiser les accès futurs

// Créer une vue temporaire pour requêtes SQL
weather_type_dataset.createOrReplaceTempView("weather_types")

// Optionnel: Analyse des patterns dans les types météo
println("\n=== ANALYSE DES PATTERNS ===")

// Types avec intensité (commençant par - ou +)
val typesWithIntensity = weather_type_dataset
  .filter(col("WeatherType").startsWith("-") || col("WeatherType").startsWith("+"))
  .orderBy("WeatherType")

println("Types avec indicateur d'intensité:")
typesWithIntensity.show(50, truncate = false)

// Types de base (sans intensité)
val baseTypes = weather_type_dataset
  .filter(!col("WeatherType").startsWith("-") && !col("WeatherType").startsWith("+"))
  .orderBy("WeatherType")

println("Types de base (sans intensité):")
baseTypes.show(50, truncate = false)

// Statistiques
println(s"Types avec intensité: ${typesWithIntensity.count()}")
println(s"Types de base: ${baseTypes.count()}")

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.2.3 Sky Condition
// MAGIC
// MAGIC - https://www.ncei.noaa.gov/pub/data/cdo/documentation/LCD_documentation.pdf
// MAGIC
// MAGIC **Sky Conditions** : A report of each cloud layer (up to 3) giving the following information.
// MAGIC
// MAGIC Each layer given in the following format: ccc:ll-xxx where:
// MAGIC  1) ccc is Coverage: CLR (clear sky), FEW (few clouds), SCT (scattered clouds), BKN (broken clouds), OVC
// MAGIC (overcast), VV (obscured sky), 10 (partially obscured sky).
// MAGIC 2) ll is Layer amount used in conjunction with coverage code above. Given in eighths (aka “oktas”) of sky
// MAGIC covered by cloud. Specifically 00-08 indicates the number of oktas that cloud layer takes up in the total sky. 00
// MAGIC corresponds to CLR, 01-02 corresponds to FEW, 03-04 corresponds to SCT, 05-07 corresponds to BKN and 08
// MAGIC corresponds to OVC. 09 indicates an obscuration (i.e. the sky cannot be seen due to obscuring phenomena - e.g.
// MAGIC due to smoke, fog, etc.). 10 indicates a portion of the sky is obscured (i.e. partial obscuration). For additional
// MAGIC information see Integrated Surface Data documentation. (http://www1.ncdc.noaa.gov/pub/data/ish/ish-formatdocument.pdf) in Cloud and Solar Data portion of Additional Data Section.
// MAGIC 3) xxx is the Cloud base height at lowest point of layer. In the case of an obscuration this value represents the
// MAGIC vertical visibility from the point of observation. Given in hundreds of feet (e.g. 50 = 5000 ft, 120 = 12000 feet).
// MAGIC In some cases a cloud base height will be given without the corresponding cloud amount. In these case the cloud
// MAGIC amount is missing or not reported.
// MAGIC

// COMMAND ----------

display(weather_original_data.groupBy("SkyCondition").count().orderBy($"count".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.2.4 Record Type

// COMMAND ----------

//RecordType
display(weather_original_data.groupBy("RecordType").count().orderBy($"count".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.2.5 Station Type

// COMMAND ----------

//StationType
display(weather_original_data.groupBy("StationType").count().orderBy($"count".desc))