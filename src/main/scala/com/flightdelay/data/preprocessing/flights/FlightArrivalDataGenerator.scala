package com.flightdelay.data.preprocessing.flights

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import com.flightdelay.config.AppConfiguration

/**
 * Génère les features liées à l'arrivée du vol
 * Calcule les dates et heures d'arrivée en temps local et UTC
 * Gère les différences de timezone entre origine et destination
 */
object FlightArrivalDataGenerator {

  /**
   * Génère toutes les features d'arrivée
   * @param flightDf DataFrame contenant les données de vols enrichies avec WBAN et timezone
   * @param spark Session Spark
   * @param configuration Configuration de l'application
   * @return DataFrame enrichi avec les features d'arrivée
   */
  def preprocess(flightDf: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    println("\n" + "=" * 80)
    println("[Preprocessing] Flight Arrival Data Generation - Start")
    println("=" * 80)

    // Étape 1: Calculer l'heure et la date d'arrivée UTC
    println("\nStep 1: Computing UTC arrival time and date")
    val withUTCArrival = computeUTCArrival(flightDf)

    // Étape 2: Calculer l'heure et la date d'arrivée locale (timezone de destination)
    println("Step 2: Computing local arrival time and date (destination timezone)")
    val withLocalArrival = computeLocalArrival(withUTCArrival)

    // Étape 3: Générer les features dérivées
    println("Step 3: Generating derived features")
    val enrichedDf = generateDerivedFeatures(withLocalArrival)

    // Display statistics
    displayStatistics(enrichedDf)

    println("\n" + "=" * 80)
    println("[Preprocessing] Flight Arrival Data Generation - End")
    println("=" * 80)

    enrichedDf
  }

  /**
   * Calcule l'heure et la date d'arrivée en UTC
   * UTC_ARR_TIME = UTC_CRS_DEP_TIME + CRS_ELAPSED_TIME
   */
  private def computeUTCArrival(df: DataFrame): DataFrame = {
    df
      // Convertir UTC_CRS_DEP_TIME en minutes depuis minuit
      .withColumn("_utc_dep_minutes",
        (col("UTC_CRS_DEP_TIME").cast(IntegerType) / lit(100)) * lit(60) +
        (col("UTC_CRS_DEP_TIME").cast(IntegerType) % lit(100))
      )
      // Ajouter la durée du vol
      .withColumn("_utc_arr_total_minutes",
        col("_utc_dep_minutes") + col("CRS_ELAPSED_TIME")
      )
      // Calculer le décalage de jour (peut être négatif, 0, ou positif)
      .withColumn("_utc_arr_day_offset",
        floor(col("_utc_arr_total_minutes") / lit(1440)).cast(IntegerType)
      )
      // Calculer les minutes dans la journée (0-1439)
      .withColumn("_utc_arr_minutes_in_day",
        ((col("_utc_arr_total_minutes") % lit(1440)) + lit(1440)) % lit(1440)
      )
      // Convertir en format HHMM
      .withColumn("UTC_ARR_TIME",
        format_string("%04d",
          (col("_utc_arr_minutes_in_day") / lit(60)).cast(IntegerType) * lit(100) +
          (col("_utc_arr_minutes_in_day") % lit(60)).cast(IntegerType)
        )
      )
      // Calculer la date d'arrivée UTC
      .withColumn("UTC_ARR_DATE",
        date_add(col("UTC_FL_DATE"), col("_utc_arr_day_offset"))
      )
      // Nettoyer les colonnes temporaires
      .drop("_utc_dep_minutes", "_utc_arr_total_minutes", "_utc_arr_day_offset", "_utc_arr_minutes_in_day")
  }

  /**
   * Calcule l'heure et la date d'arrivée locale (timezone de destination)
   * Local = UTC + DEST_TIMEZONE offset
   */
  private def computeLocalArrival(df: DataFrame): DataFrame = {
    df
      // Convertir UTC_ARR_TIME en minutes
      .withColumn("_utc_arr_minutes",
        (col("UTC_ARR_TIME").cast(IntegerType) / lit(100)) * lit(60) +
        (col("UTC_ARR_TIME").cast(IntegerType) % lit(100))
      )
      // Appliquer le décalage horaire de destination (en minutes)
      // Local = UTC + timezone_offset (car DEST_TIMEZONE est le décalage par rapport à UTC)
      .withColumn("_local_arr_total_minutes",
        col("_utc_arr_minutes") + (col("DEST_TIMEZONE") * lit(60))
      )
      // Calculer le décalage de jour pour l'heure locale
      .withColumn("_local_arr_day_offset",
        floor(col("_local_arr_total_minutes") / lit(1440)).cast(IntegerType)
      )
      // Calculer les minutes dans la journée locale (0-1439)
      .withColumn("_local_arr_minutes_in_day",
        ((col("_local_arr_total_minutes") % lit(1440)) + lit(1440)) % lit(1440)
      )
      // Convertir en format HHMM
      .withColumn("CRS_ARR_TIME",
        format_string("%04d",
          (col("_local_arr_minutes_in_day") / lit(60)).cast(IntegerType) * lit(100) +
          (col("_local_arr_minutes_in_day") % lit(60)).cast(IntegerType)
        )
      )
      // Calculer la date d'arrivée locale
      .withColumn("CRS_ARR_DATE",
        date_add(col("UTC_ARR_DATE"), col("_local_arr_day_offset"))
      )
      // Nettoyer les colonnes temporaires
      .drop("_utc_arr_minutes", "_local_arr_total_minutes", "_local_arr_day_offset", "_local_arr_minutes_in_day")
  }

  /**
   * Génère des features dérivées à partir des données d'arrivée
   */
  private def generateDerivedFeatures(df: DataFrame): DataFrame = {
    df
      // Heure d'arrivée (0-23)
      .withColumn("feature_arrival_hour",
        (col("CRS_ARR_TIME").cast(IntegerType) / lit(100)).cast(IntegerType)
      )
      // Heure d'arrivée UTC (0-23)
      .withColumn("feature_utc_arrival_hour",
        (col("UTC_ARR_TIME").cast(IntegerType) / lit(100)).cast(IntegerType)
      )
      // Arrondi de l'heure d'arrivée UTC (pour la jointure météo)
      .withColumn("feature_utc_arrival_hour_rounded",
        format_string("%04d", col("feature_utc_arrival_hour") * lit(100))
      )
      // Date d'arrivée au format string YYYY-MM-DD (pour la jointure météo)
      .withColumn("feature_utc_arrival_date",
        date_format(col("UTC_ARR_DATE"), "yyyy-MM-dd")
      )
      // Période de temps de l'arrivée (8 périodes de 3h)
      .withColumn("feature_arrival_time_period",
        when((col("CRS_ARR_TIME").cast(IntegerType) / lit(100)) < lit(3), "Late_Night")
          .when((col("CRS_ARR_TIME").cast(IntegerType) / lit(100)) < lit(6), "Early_Morning")
          .when((col("CRS_ARR_TIME").cast(IntegerType) / lit(100)) < lit(9), "Morning")
          .when((col("CRS_ARR_TIME").cast(IntegerType) / lit(100)) < lit(12), "Late_Morning")
          .when((col("CRS_ARR_TIME").cast(IntegerType) / lit(100)) < lit(15), "Early_Afternoon")
          .when((col("CRS_ARR_TIME").cast(IntegerType) / lit(100)) < lit(18), "Late_Afternoon")
          .when((col("CRS_ARR_TIME").cast(IntegerType) / lit(100)) < lit(21), "Evening")
          .otherwise("Night")
      )
      // Indicateur si le vol traverse minuit (en temps local)
      .withColumn("feature_crosses_midnight_local",
        when(col("CRS_ARR_DATE") > col("FL_DATE"), 1).otherwise(0)
      )
      // Indicateur si le vol traverse minuit (en temps UTC)
      .withColumn("feature_crosses_midnight_utc",
        when(col("UTC_ARR_DATE") > col("UTC_FL_DATE"), 1).otherwise(0)
      )
      // Nombre de jours que le vol couvre (en temps local)
      .withColumn("feature_flight_days_span",
        datediff(col("CRS_ARR_DATE"), col("FL_DATE"))
      )
      // Différence de timezone entre origine et destination (en heures)
      .withColumn("feature_timezone_difference",
        col("DEST_TIMEZONE") - col("ORIGIN_TIMEZONE")
      )
      // Indicateur de vol vers l'est (timezone destination > origine)
      .withColumn("feature_flies_eastward",
        when(col("feature_timezone_difference") > 0, 1).otherwise(0)
      )
      // Indicateur de vol vers l'ouest (timezone destination < origine)
      .withColumn("feature_flies_westward",
        when(col("feature_timezone_difference") < 0, 1).otherwise(0)
      )
  }

  /**
   * Affiche des statistiques sur les données d'arrivée générées
   */
  private def displayStatistics(df: DataFrame): Unit = {
    val totalFlights = df.count()
    val crossesMidnightLocal = df.filter(col("feature_crosses_midnight_local") === 1).count()
    val crossesMidnightUTC = df.filter(col("feature_crosses_midnight_utc") === 1).count()
    val fliesEastward = df.filter(col("feature_flies_eastward") === 1).count()
    val fliesWestward = df.filter(col("feature_flies_westward") === 1).count()

    println(s"\nArrival Data Statistics:")
    println(s"  - Total flights: ${totalFlights}")
    println(s"  - Crosses midnight (local): ${crossesMidnightLocal} (${(crossesMidnightLocal * 100.0 / totalFlights).round}%)")
    println(s"  - Crosses midnight (UTC): ${crossesMidnightUTC} (${(crossesMidnightUTC * 100.0 / totalFlights).round}%)")
    println(s"  - Flies eastward: ${fliesEastward} (${(fliesEastward * 100.0 / totalFlights).round}%)")
    println(s"  - Flies westward: ${fliesWestward} (${(fliesWestward * 100.0 / totalFlights).round}%)")

  }
}
