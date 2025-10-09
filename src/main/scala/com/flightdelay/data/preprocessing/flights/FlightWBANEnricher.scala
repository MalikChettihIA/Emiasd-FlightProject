package com.flightdelay.data.preprocessing.flights

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Enrichit les données de vols avec les informations WBAN et TimeZone
 * Associe chaque aéroport (origine et destination) avec son WBAN ID et fuseau horaire
 */
object FlightWBANEnricher {

  /**
   * Enrichit les données de vols avec les WBAN IDs et fuseaux horaires
   * @param flightDf DataFrame contenant les données de vols
   * @param spark Session Spark
   * @param configuration Configuration de l'application
   * @return DataFrame enrichi avec WBAN IDs pour origine et destination
   */
  def preprocess(flightDf: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    println("\n" + "=" * 80)
    println("[Preprocessing] Flight WBAN Enrichment - Start")
    println("=" * 80)

    // Load WBAN-Airport-Timezone mapping
    val wbanParquetPath = s"${configuration.common.output.basePath}/common/data/raw_wban_airport_timezone.parquet"
    println(s"\nLoading WBAN-Airport-Timezone mapping:")
    println(s"  - Path: $wbanParquetPath")
    val wbanMappingDf = spark.read.parquet(wbanParquetPath)
    println(s"  - Loaded ${wbanMappingDf.count()} airport-WBAN mappings")

    // Join with origin airport to get origin WBAN and timezone
    val withOriginWBAN = flightDf
      .join(
        wbanMappingDf
          .select(
            col("AirportID").as("ORIGIN_AIRPORT_ID_JOIN"),
            col("WBAN").as("ORIGIN_WBAN"),
            col("TimeZone").as("ORIGIN_TIMEZONE")
          ),
        flightDf("ORIGIN_AIRPORT_ID") === col("ORIGIN_AIRPORT_ID_JOIN"),
        "inner"
      )
      .drop("ORIGIN_AIRPORT_ID_JOIN")

    // Join with destination airport to get destination WBAN and timezone
    val enrichedDf = withOriginWBAN
      .join(
        wbanMappingDf
          .select(
            col("AirportID").as("DEST_AIRPORT_ID_JOIN"),
            col("WBAN").as("DEST_WBAN"),
            col("TimeZone").as("DEST_TIMEZONE")
          ),
        withOriginWBAN("DEST_AIRPORT_ID") === col("DEST_AIRPORT_ID_JOIN"),
        "inner"
      )
      .drop("DEST_AIRPORT_ID_JOIN")

    // Display statistics
    val totalFlights = enrichedDf.count()
    val flightsWithOriginWBAN = enrichedDf.filter(col("ORIGIN_WBAN").isNotNull).count()
    val flightsWithDestWBAN = enrichedDf.filter(col("DEST_WBAN").isNotNull).count()

    println(s"\nEnrichment statistics:")
    println(s"  - Total flights: $totalFlights")
    println(s"  - Flights with origin WBAN: $flightsWithOriginWBAN (${(flightsWithOriginWBAN * 100.0 / totalFlights).round}%)")
    println(s"  - Flights with destination WBAN: $flightsWithDestWBAN (${(flightsWithDestWBAN * 100.0 / totalFlights).round}%)")

    println("\n" + "=" * 80)
    println("[Preprocessing] Flight WBAN Enrichment - End")
    println("=" * 80)

    enrichedDf
  }

}
