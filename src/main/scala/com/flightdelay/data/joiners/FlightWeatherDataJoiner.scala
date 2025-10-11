package com.flightdelay.data.joiners

import com.flightdelay.config.AppConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Joint les données de vols avec les observations météo
 * Implémentation basée sur l'algorithme du papier TIST (pages A:8-A:11)
 *
 * Stratégie:
 * 1. Jointure avec météo d'origine (ORIGIN_WBAN) - 12h avant départ
 * 2. Jointure avec météo de destination (DEST_WBAN) - 12h avant arrivée
 * 3. Utilisation de partitionnement (WBAN, Date) pour optimisation
 * 4. Agrégation des 12 observations horaires pour chaque vol
 */
object FlightWeatherDataJoiner {

  /**
   * Joint les données de vols et les données météo
   * @param flightData DataFrame contenant les données de vols préprocessées
   * @param weatherData DataFrame contenant les données météo préprocessées
   * @param spark Session Spark
   * @param configuration Configuration de l'application
   * @return DataFrame joint des données de vols et météo
   */
  def join(flightData: DataFrame, weatherData: DataFrame)(implicit spark: SparkSession, configuration: AppConfiguration): DataFrame = {

    println("\n" + "=" * 80)
    println("[Joiner] Flight-Weather Data Join - Start")
    println("=" * 80)

    println(s"\nInput flight data:")
    println(s"  - Records: ${flightData.count()}")
    println(s"  - Columns: ${flightData.columns.length}")

    println(s"\nInput weather data:")
    println(s"  - Records: ${weatherData.count()}")
    println(s"  - Columns: ${weatherData.columns.length}")

    // Step 1: Prepare weather data with proper partitioning
    // Partition by (WBAN, Date) as per TIST paper optimization
    println("\nStep 1: Preparing weather data with partitioning...")
    val weatherPartitioned = weatherData
      .repartition(col("WBAN"), col("Date"))
      .cache()

    println(s"  - Weather data partitioned by (WBAN, Date)")

    // Step 2: Join with origin weather (12 hours before departure)
    println("\nStep 2: Joining with origin weather observations...")
    val withOriginWeather = joinOriginWeather(flightData, weatherPartitioned)
    println(s"  - Origin weather joined: ${withOriginWeather.count()} flights")

    // Step 3: Join with destination weather (12 hours before arrival)
    println("\nStep 3: Joining with destination weather observations...")
    val withDestWeather = joinDestinationWeather(withOriginWeather, weatherPartitioned)
    println(s"  - Destination weather joined: ${withDestWeather.count()} flights")

    // Unpersist cached weather data
    weatherPartitioned.unpersist()

    println(s"\nOutput joined data:")
    println(s"  - Records: ${withDestWeather.count()}")
    println(s"  - Columns: ${withDestWeather.columns.length}")

    println("\n" + "=" * 80)
    println("[Joiner] Flight-Weather Data Join - End")
    println("=" * 80 + "\n")

    withDestWeather
  }

  /**
   * Joint les observations météo de l'aéroport d'origine
   * Récupère 12 observations horaires avant l'heure de départ UTC
   * Retourne les observations sous forme d'arrays
   */
  private def joinOriginWeather(flightData: DataFrame, weatherData: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val startTime = System.currentTimeMillis()

    // Prepare weather data for origin join
    val weatherColumns = weatherData.columns.filter(c => c != "WBAN" && c != "Date" && c != "Time")

    println(s"\n  [Origin Weather Join] Weather columns to join: ${weatherColumns.length}")
    println(s"    - Weather features: ${weatherColumns.take(5).mkString(", ")}${if (weatherColumns.length > 5) "..." else ""}")

    // Add timestamp to weather for easier comparison
    val weatherWithTimestamp = weatherData
      .withColumn("weather_timestamp_minutes",
        unix_timestamp(col("Date")) / lit(60) +
        (col("Time").cast(IntegerType) / lit(100)) * lit(60) +
        (col("Time").cast(IntegerType) % lit(100))
      )

    // Add departure timestamp and unique ID to flights
    val flightWithTimestamp = flightData
      .withColumn("_flight_unique_id", monotonically_increasing_id())
      .withColumn("departure_timestamp_minutes",
        unix_timestamp(col("UTC_FL_DATE")) / lit(60) +
        (col("UTC_CRS_DEP_TIME").cast(IntegerType) / lit(100)) * lit(60) +
        (col("UTC_CRS_DEP_TIME").cast(IntegerType) % lit(100))
      )
      .withColumn("departure_12h_before_minutes",
        col("departure_timestamp_minutes") - lit(720) // 12 hours = 720 minutes
      )

    // Join condition: same WBAN and weather observation within 12-hour window before departure
    val joinCondition =
      flightWithTimestamp("ORIGIN_WBAN") === weatherWithTimestamp("WBAN") &&
      weatherWithTimestamp("weather_timestamp_minutes") >= flightWithTimestamp("departure_12h_before_minutes") &&
      weatherWithTimestamp("weather_timestamp_minutes") <= flightWithTimestamp("departure_timestamp_minutes")

    // Perform the join
    val weatherOriginCols = Seq(col("WBAN"), col("weather_timestamp_minutes")) ++
                            weatherColumns.map(c => col(c).as(s"origin_weather_$c"))

    val joinedWithOrigin = flightWithTimestamp
      .join(
        weatherWithTimestamp.select(weatherOriginCols: _*),
        joinCondition,
        "left"
      )

    // Rank observations by timestamp (most recent first)
    val windowSpec = Window
      .partitionBy(col("_flight_unique_id"))
      .orderBy(col("weather_timestamp_minutes").desc_nulls_last)

    val rankedObservations = joinedWithOrigin
      .withColumn("obs_rank", row_number().over(windowSpec))
      .filter(col("obs_rank") <= 12 || col("weather_timestamp_minutes").isNull)

    // Aggregate weather observations into arrays (limit to 12 most recent)
    val aggregated = rankedObservations
      .groupBy(col("_flight_unique_id"))
      .agg(
        // Aggregate all original flight columns
        flightData.columns.map(c => first(col(c)).as(c)).head,
        (flightData.columns.tail.map(c => first(col(c)).as(c)) ++
        // Aggregate weather observations into arrays
        weatherColumns.map(c =>
          collect_list(col(s"origin_weather_$c")).as(s"origin_weather_${c}_array")
        )): _*
      )

    // Add count of observations found
    val result = aggregated
      .withColumn("origin_weather_obs_count",
        size(col(s"origin_weather_${weatherColumns.head}_array"))
      )
      .drop("_flight_unique_id")

    val duration = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"  [Origin Weather Join] Completed in ${duration}s - Created ${weatherColumns.length} weather arrays")

    result
  }

  /**
   * Joint les observations météo de l'aéroport de destination
   * Récupère 12 observations horaires avant l'heure d'arrivée UTC
   * Retourne les observations sous forme d'arrays
   */
  private def joinDestinationWeather(flightData: DataFrame, weatherData: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val startTime = System.currentTimeMillis()

    // Prepare weather data for destination join
    val weatherColumns = weatherData.columns.filter(c => c != "WBAN" && c != "Date" && c != "Time")

    println(s"\n  [Destination Weather Join] Weather columns to join: ${weatherColumns.length}")
    println(s"    - Weather features: ${weatherColumns.take(5).mkString(", ")}${if (weatherColumns.length > 5) "..." else ""}")

    // Add timestamp to weather for easier comparison
    val weatherWithTimestamp = weatherData
      .withColumn("weather_timestamp_minutes",
        unix_timestamp(col("Date")) / lit(60) +
        (col("Time").cast(IntegerType) / lit(100)) * lit(60) +
        (col("Time").cast(IntegerType) % lit(100))
      )

    // Add arrival timestamp and unique ID to flights
    val flightWithTimestamp = flightData
      .withColumn("_flight_unique_id", monotonically_increasing_id())
      .withColumn("arrival_timestamp_minutes",
        unix_timestamp(to_date(col("feature_arrival_date"), "yyyy-MM-dd")) / lit(60) +
        (col("feature_arrival_hour_rounded").cast(IntegerType) / lit(100)) * lit(60) +
        (col("feature_arrival_hour_rounded").cast(IntegerType) % lit(100))
      )
      .withColumn("arrival_12h_before_minutes",
        col("arrival_timestamp_minutes") - lit(720) // 12 hours = 720 minutes
      )

    // Join condition: same WBAN and weather observation within 12-hour window before arrival
    val joinCondition =
      flightWithTimestamp("DEST_WBAN") === weatherWithTimestamp("WBAN") &&
      weatherWithTimestamp("weather_timestamp_minutes") >= flightWithTimestamp("arrival_12h_before_minutes") &&
      weatherWithTimestamp("weather_timestamp_minutes") <= flightWithTimestamp("arrival_timestamp_minutes")

    // Perform the join
    val weatherDestCols = Seq(col("WBAN"), col("weather_timestamp_minutes")) ++
                          weatherColumns.map(c => col(c).as(s"dest_weather_$c"))

    val joinedWithDest = flightWithTimestamp
      .join(
        weatherWithTimestamp.select(weatherDestCols: _*),
        joinCondition,
        "left"
      )

    // Rank observations by timestamp (most recent first)
    val windowSpec = Window
      .partitionBy(col("_flight_unique_id"))
      .orderBy(col("weather_timestamp_minutes").desc_nulls_last)

    val rankedObservations = joinedWithDest
      .withColumn("obs_rank", row_number().over(windowSpec))
      .filter(col("obs_rank") <= 12 || col("weather_timestamp_minutes").isNull)

    // Aggregate weather observations into arrays (limit to 12 most recent)
    val aggregated = rankedObservations
      .groupBy(col("_flight_unique_id"))
      .agg(
        // Aggregate all original flight columns
        flightData.columns.map(c => first(col(c)).as(c)).head,
        (flightData.columns.tail.map(c => first(col(c)).as(c)) ++
        // Aggregate weather observations into arrays
        weatherColumns.map(c =>
          collect_list(col(s"dest_weather_$c")).as(s"dest_weather_${c}_array")
        )): _*
      )

    // Add count of observations found
    val result = aggregated
      .withColumn("dest_weather_obs_count",
        size(col(s"dest_weather_${weatherColumns.head}_array"))
      )
      .drop("_flight_unique_id")

    val duration = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"  [Destination Weather Join] Completed in ${duration}s - Created ${weatherColumns.length} weather arrays")

    result
  }

}
