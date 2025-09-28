// Databricks notebook source
// MAGIC %md
// MAGIC %md * Author: Malik Chettih
// MAGIC * Affiliation: EMIASD - Executive Master Intelligence artificielle
// MAGIC & science des données
// MAGIC * Email: malik.chettih@dauphine.eu
// MAGIC * Formation Continue Univ. Paris Dauphine, January 2025.

// COMMAND ----------

// MAGIC %md
// MAGIC # Flights - Analyse des données

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 1 - Pre-requis
// MAGIC
// MAGIC ### 1.1 Global Variables

// COMMAND ----------

val path = "/FileStore/tables/FLIGHT-3Y/Flights/"
//val fileName = "201201.csv"
val fileName = "2012*.csv"
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

// MAGIC %md
// MAGIC ### 2.1 Chargement des données

// COMMAND ----------

val flights_original_data = spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(dbfsDir+fileName)
            .persist()
flights_original_data.count()

// COMMAND ----------

flights_original_data.printSchema


// COMMAND ----------

display(flights_original_data.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.3 Collecting data quality metrics

// COMMAND ----------

var flights_original_metadata = MDCompletenessDV(flights_original_data)
display(flights_original_metadata.orderBy($"compRatio".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.4 Data Description
// MAGIC
// MAGIC
// MAGIC | Colonne                   | Description                                                                 | Type           | Utilité en ML                  |
// MAGIC |---------------------------|-----------------------------------------------------------------------------|----------------|--------------------------------|
// MAGIC | `FL_DATE`                | Date du vol (format `YYYY-MM-DD`)                                           | `Date`         | ✅ Pour extraire jour/semaine  |
// MAGIC | `OP_CARRIER_AIRLINE_ID` | ID numérique de la compagnie aérienne                                        | `Int`          | ✅ Variable catégorielle       |
// MAGIC | `OP_CARRIER_FL_NUM`     | Numéro de vol dans la compagnie                                              | `String`       | 🔶 Optionnel (peut être bruit) |
// MAGIC | `ORIGIN_AIRPORT_ID`     | ID de l’aéroport d’origine                                                   | `Int`          | ✅ Variable catégorielle       |
// MAGIC | `DEST_AIRPORT_ID`       | ID de l’aéroport de destination                                              | `Int`          | ✅ Variable catégorielle       |
// MAGIC | `CRS_DEP_TIME`          | Heure de départ prévue (`hhmm`, ex: "0845")                                  | `String`/`Int` | ✅ Extraire tranche horaire    |
// MAGIC | `ARR_DELAY_NEW`         | Retard à l’arrivée (≥ 0)                                                     | `Float`        | 🎯 **Label cible**             |
// MAGIC | `CANCELLED`             | Indique si le vol est annulé (1 = oui)                                       | `Float` (0/1)  | ❌ À filtrer (inutile en ML)   |
// MAGIC | `DIVERTED`              | Indique si le vol est détourné (1 = oui)                                     | `Float` (0/1)  | ❌ À filtrer (inutile en ML)   |
// MAGIC | `CRS_ELAPSED_TIME`      | Durée de vol planifiée en minutes                                            | `Float`        | ✅ Variable continue           |
// MAGIC | `WEATHER_DELAY`         | Retard dû à la météo (en minutes)                                            | `Float`        | 🔶 Optionnel pour post-analyse |
// MAGIC | `NAS_DELAY`             | Retard dû au système de navigation aérienne (ATC, météo modérée, congestion) | `Float`        | 🔶 Optionnel pour analyse cause|
// MAGIC

// COMMAND ----------

// Summary of flight data
display(flights_original_data.describe())

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

println("Total records count without dropping duplicates:", flights_data.count())
val flights_data = flights_data.dropDuplicates()
println("Total records count after dropping duplicates:", flights_data.count())

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.1.2 Drop Unecessary Columns

// COMMAND ----------

// 1. Supprimer les lignes où DIVERTED = 1 OU CANCELLED = 1
var flights_data = flights_original_data
    .filter(col("DIVERTED") === 0)    // Garder seulement les vols non détournés
    .filter(col("CANCELLED") === 0)   // Garder seulement les vols non annulés
    .drop("DIVERTED", "CANCELLED", "_c12")
        
// 2. Vérifier le résultat
flights_data.printSchema()
display(flights_data.limit(10))
println(s"Nombre de lignes final: ${flights_data.count()}")    

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.2 Data Enrichment

// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.2.1 Adding FL_YEAR, FL_MONTH and ...

// COMMAND ----------

flights_data = flights_data
  .withColumn("FL_YEAR", year(col("FL_DATE")))
  .withColumn("FL_QUARTER", quarter(col("FL_DATE")))
  .withColumn("FL_MONTH", month(col("FL_DATE")))
  .withColumn("FL_WEEK_NUMBER", weekofyear(col("FL_DATE")))
  .withColumn("FL_DAY_OF_MONTH", dayofmonth(col("FL_DATE")))         
  .withColumn("FL_DAY_OF_WEEK", dayofweek(col("FL_DATE"))) 
  .withColumn("HOUR_OF_DAY", lpad(col("CRS_DEP_TIME").cast("string"), 4, "0").substr(1, 2).cast("int")) 

// 2. Vérifier le résultat
flights_data.printSchema()
display(flights_data.limit(10))


// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.2.2 Remove NULL Values
// MAGIC

// COMMAND ----------

flights_data = flights_data.filter(col("CRS_ELAPSED_TIME").isNotNull)
flights_data = flights_data.filter(col("CRS_DEP_TIME").isNotNull)
flights_data = flights_data.filter(col("ARR_DELAY_NEW").isNotNull)

var flights_metadata = MDCompletenessDV(flights_data)
display(flights_metadata.orderBy($"compRatio".desc))


// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.2.3 Adding DELAYED

// COMMAND ----------

flights_data = flights_data.withColumn(
    "DELAYED", 
    when(col("ARR_DELAY_NEW") >= 15, 1).otherwise(0)
)

// COMMAND ----------

// Calculer les statistiques
val total = flights_data.count()
val delayed = flights_data.filter(col("DELAYED") === 1).count()
val on_time = flights_data.filter(col("DELAYED") === 0).count()

// Ratios
val delayed_pct = (delayed * 100.0) / total
val on_time_pct = (on_time * 100.0) / total
val delay_ratio = delayed.toDouble / on_time.toDouble

// Affichage formaté
println("=== ANALYSE DES RETARDS ===")
println(f"Total des vols: $total%,d")
println(f"Vols retardés (≥15min): $delayed%,d ($delayed_pct%.1f%%)")
println(f"Vols à l'heure (<15min): $on_time%,d ($on_time_pct%.1f%%)")
println(f"Ratio retardés/à l'heure: $delay_ratio%.3f")
println(f"Pour 1 vol retardé, il y a ${1/delay_ratio%.1f} vols à l'heure")

// COMMAND ----------

// Créer un DataFrame avec les résultats
val results = flights_data.agg(
    count("*").alias("total"),
    sum("DELAYED").alias("delayed"),
    sum(when(col("DELAYED") === 0, 1).otherwise(0)).alias("on_time")
).select(
    col("total"),
    col("delayed"),
    col("on_time"),
    round((col("delayed") * 100.0 / col("total")), 2).alias("delayed_pct"),
    round((col("on_time") * 100.0 / col("total")), 2).alias("on_time_pct"),
    round((col("delayed").cast("double") / col("on_time")), 3).alias("delay_ratio")
)

display(results)

// COMMAND ----------

flights_metadata = MDCompletenessDV(flights_data)
display(flights_metadata.orderBy($"compRatio".desc))

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.3 Data Categories

// COMMAND ----------

flights_metadata = SetMDColType(flights_metadata, "FL_DAY_OF_MONTH", "textType")
flights_metadata = SetMDColType(flights_metadata, "FL_YEAR", "textType")
flights_metadata = SetMDColType(flights_metadata, "FL_MONTH", "textType")
flights_metadata = SetMDColType(flights_metadata, "FL_WEEK_NUMBER", "textType")
flights_metadata = SetMDColType(flights_metadata, "OP_CARRIER_AIRLINE_ID", "textType")
flights_metadata = SetMDColType(flights_metadata, "OP_CARRIER_FL_NUM", "textType")
flights_metadata = SetMDColType(flights_metadata, "ORIGIN_AIRPORT_ID", "textType")
flights_metadata = SetMDColType(flights_metadata, "DEST_AIRPORT_ID", "textType")
flights_metadata = SetMDColType(flights_metadata, "MONTH_OF_DAY", "textType")

display(flights_metadata.orderBy($"name".asc))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 4 - Analyse Univariée

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.1 Aéroport d'Origine (ORIGIN_AIRPORT_ID) 

// COMMAND ----------

// Calculer le nombre de vols en retard par aéroport de départ
val delay_by_airport_origin = flights_data
  .groupBy("ORIGIN_AIRPORT_ID")
  .agg(
    count("*").alias("total_flights"),
    sum("DELAYED").alias("delayed_flights"),
    (sum("DELAYED") * 100.0 / count("*")).alias("delay_percentage")
  )
  .orderBy(desc("delayed_flights"))

// Afficher le graphique dans Databricks
display(delay_by_airport_origin)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.2 Aéroport d'arrivée (DEST_AIRPORT_ID) 

// COMMAND ----------

// Calculer le nombre de vols en retard par aéroport de départ
val delay_by_airport_destination = flights_data
  .groupBy("DEST_AIRPORT_ID")
  .agg(
    count("*").alias("total_flights"),
    sum("DELAYED").alias("delayed_flights"),
    (sum("DELAYED") * 100.0 / count("*")).alias("delay_percentage")
  )
  .orderBy(desc("delayed_flights"))

// Afficher le graphique dans Databricks
display(delay_by_airport_destination)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.3 Compagnie Aérienne (OP_CARRIER_AIRLINE_ID) 

// COMMAND ----------

val delay_by_carrier_airline = flights_data
  .groupBy("OP_CARRIER_AIRLINE_ID")
  .agg(
    count("*").alias("total_flights"),
    sum("DELAYED").alias("delayed_flights"),
    (sum("DELAYED") * 100.0 / count("*")).alias("delay_percentage")
  )
  .orderBy(desc("delayed_flights"))

// Afficher le graphique dans Databricks
display(delay_by_carrier_airline)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.5 Vols (OP_CARRIER_FL_NUM) 

// COMMAND ----------

val delay_by_carrier_airline = flights_data
  .groupBy("OP_CARRIER_FL_NUM")
  .agg(
    count("*").alias("total_flights"),
    sum("DELAYED").alias("delayed_flights"),
    (sum("DELAYED") * 100.0 / count("*")).alias("delay_percentage")
  )
  .orderBy(desc("delayed_flights"))

// Afficher le graphique dans Databricks
display(delay_by_carrier_airline)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.6 Heure de départ (HOUR_OF_DAY) 

// COMMAND ----------

val delay_by_hour = flights_data
  .groupBy("HOUR_OF_DAY")
  .agg(
    count("*").alias("total_flights"),
    sum("DELAYED").alias("delayed_flights"),
    (sum("DELAYED") * 100.0 / count("*")).alias("delay_percentage")
  )
  .orderBy(desc("delayed_flights"))

// Afficher le graphique dans Databricks
display(delay_by_hour)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.7 Mois (FL_MONTH) 

// COMMAND ----------

val delay_by_month = flights_data
  .groupBy("FL_MONTH")
  .agg(
    count("*").alias("total_flights"),
    sum("DELAYED").alias("delayed_flights"),
    (sum("DELAYED") * 100.0 / count("*")).alias("delay_percentage")
  )
  .orderBy(desc("delayed_flights"))

// Afficher le graphique dans Databricks
display(delay_by_month)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.8 Trimestre (FL_QUATER) 

// COMMAND ----------

val delay_by_quarter = flights_data
  .groupBy("FL_QUARTER")
  .agg(
    count("*").alias("total_flights"),
    sum("DELAYED").alias("delayed_flights"),
    (sum("DELAYED") * 100.0 / count("*")).alias("delay_percentage")
  )
  .orderBy(desc("delayed_flights"))

// Afficher le graphique dans Databricks
display(delay_by_quarter)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.9 Jour du mois (FL_DAY_OF_MONTH) 

// COMMAND ----------

val delay_by_day_of_month = flights_data
  .groupBy("FL_DAY_OF_MONTH")
  .agg(
    count("*").alias("total_flights"),
    sum("DELAYED").alias("delayed_flights"),
    (sum("DELAYED") * 100.0 / count("*")).alias("delay_percentage")
  )
  .orderBy(desc("delayed_flights"))

// Afficher le graphique dans Databricks
display(delay_by_day_of_month)

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4.10 Jour de la semaine (FL_DAY_OF_WEEK) 

// COMMAND ----------

val delay_by_day_of_week = flights_data
  .groupBy("FL_DAY_OF_WEEK")
  .agg(
    count("*").alias("total_flights"),
    sum("DELAYED").alias("delayed_flights"),
    (sum("DELAYED") * 100.0 / count("*")).alias("delay_percentage")
  )
  .orderBy(desc("delayed_flights"))

// Afficher le graphique dans Databricks
display(delay_by_day_of_week)