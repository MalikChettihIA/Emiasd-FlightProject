package com.flightdelay.data.preprocessing.flights

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import com.flightdelay.config.AppConfiguration
import com.flightdelay.data.loaders.WBANAirportTimezoneLoader


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

    // Load WBAN-Airport-Timezone mapping with automatic fallback to CSV if parquet doesn't exist
    val wbanParquetPath = s"${configuration.common.output.basePath}/common/data/raw_wban_airport_timezone.parquet"
    println(s"\nLoading WBAN-Airport-Timezone mapping:")
    println(s"  - Parquet path: $wbanParquetPath")

    val wbanMappingDf = if (parquetFileExists(wbanParquetPath)) {
      println(s"  ✓ Loading from existing Parquet")
      spark.read.parquet(wbanParquetPath)
    } else {
      println(s"  ⚠ Parquet not found - loading from CSV and creating Parquet")
      // WBANAirportTimezoneLoader will automatically load CSV and save to Parquet
      WBANAirportTimezoneLoader.loadFromConfiguration()
    }

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
    val withDestWBAN = withOriginWBAN
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

    // Convert CRS_DEP_TIME from local time to UTC using ORIGIN_TIMEZONE
    // ORIGIN_TIMEZONE is offset from UTC (e.g., -6 for CST, -8 for PST)
    // UTC = local_time - timezone_offset

    // Step 1: Calculate total UTC minutes (can be negative or > 1440)
    val withUTCMinutes = withDestWBAN.withColumn("_utc_total_minutes",
      // Local time in minutes
      ((col("CRS_DEP_TIME") / lit(100)).cast(IntegerType) * lit(60) +
       (col("CRS_DEP_TIME") % lit(100)).cast(IntegerType)) -
      // Subtract timezone offset (convert hours to minutes)
      (col("ORIGIN_TIMEZONE") * lit(60))
    )

    // Step 2: Calculate day offset and UTC time
    val enrichedDf = withUTCMinutes
      // Calculate day offset (-1, 0, or +1)
      .withColumn("_utc_day_offset",
        floor(col("_utc_total_minutes") / lit(1440)).cast(IntegerType)
      )
      // Calculate UTC time within the day (0-1439 minutes)
      .withColumn("_utc_minutes_in_day",
        ((col("_utc_total_minutes") % lit(1440)) + lit(1440)) % lit(1440)
      )
      // Convert UTC minutes to HHMM format
      .withColumn("UTC_CRS_DEP_TIME",
        format_string("%04d",
          (col("_utc_minutes_in_day") / lit(60)).cast(IntegerType) * lit(100) +
          (col("_utc_minutes_in_day") % lit(60)).cast(IntegerType)
        )
      )
      // Calculate UTC date by adding day offset to FL_DATE
      .withColumn("UTC_FL_DATE",
        date_add(col("FL_DATE"), col("_utc_day_offset")
      )

      )
      // Drop temporary columns
      .drop("_utc_total_minutes", "_utc_day_offset", "_utc_minutes_in_day")

    println(s"\n  - Added UTC_CRS_DEP_TIME and UTC_FL_DATE conversion")
    println(s"  - Handling timezone offsets and date boundaries")

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

  /**
   * Check if Parquet file exists
   */
  private def parquetFileExists(path: String)(implicit spark: SparkSession): Boolean = {
    try {
      val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.exists(new org.apache.hadoop.fs.Path(path))
    } catch {
      case _: Exception => false
    }
  }

}
